package kafko_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

type MockWriter_caseNoErrCloseAppendAtWriteMessages struct { //nolint:revive,stylecheck
	writtenMsgs []kafka.Message
}

func (w *MockWriter_caseNoErrCloseAppendAtWriteMessages) Close() error {
	return nil
}

func (w *MockWriter_caseNoErrCloseAppendAtWriteMessages) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	_, cancel := context.WithCancel(ctx)

	defer cancel()

	w.writtenMsgs = append(w.writtenMsgs, msgs...)

	return nil
}

func Test_Case_OK_noClose_oneMessage_WriteMessages(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	payload := []byte("payload OK")

	expectedLog := log.NewMockLogger()
	actualLog := log.NewMockLogger()

	expectedWriter := MockWriter_caseNoErrCloseAppendAtWriteMessages{
		writtenMsgs: []kafka.Message{{Value: payload}},
	}
	actualWriter := MockWriter_caseNoErrCloseAppendAtWriteMessages{
		writtenMsgs: []kafka.Message{},
	}

	publisher := kafko.NewPublisher(actualLog, kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return &actualWriter
		}),
	)

	expectedErr := error(nil)
	actualErr := publisher.Publish(ctx, payload)

	assert.Equal(t, expectedErr, actualErr)
	assert.Equal(t, expectedWriter, actualWriter)
	assert.Equal(t, expectedLog, actualLog)
}

var (
	errRandomError = errors.New("a random error")
)

type MockWriter_caseNoErrCloseAppendAtWriteMessagesFailFirstTimeSuccessSecondTime struct { //nolint:revive,stylecheck
	writtenMsgs   []kafka.Message
	attemptNumber int
}

func (w *MockWriter_caseNoErrCloseAppendAtWriteMessagesFailFirstTimeSuccessSecondTime) Close() error {
	return nil
}

func (w *MockWriter_caseNoErrCloseAppendAtWriteMessagesFailFirstTimeSuccessSecondTime) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	_, cancel := context.WithCancel(ctx)

	defer cancel()

	if w.attemptNumber == 0 {
		w.attemptNumber++

		return errRandomError
	}

	w.writtenMsgs = append(w.writtenMsgs, msgs...)

	return nil
}

func Test_Case_OK_WriteMessages_failedFirstAttempt_successSecondAttempt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	payload := []byte("payload OK")

	expectedLog := log.NewMockLogger()
	actualLog := log.NewMockLogger()

	expectedWriter := MockWriter_caseNoErrCloseAppendAtWriteMessagesFailFirstTimeSuccessSecondTime{
		writtenMsgs:   []kafka.Message{{Value: payload}},
		attemptNumber: 1,
	}
	actualWriter := MockWriter_caseNoErrCloseAppendAtWriteMessagesFailFirstTimeSuccessSecondTime{
		writtenMsgs: []kafka.Message{},
	}

	publisher := kafko.NewPublisher(actualLog, kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return &actualWriter
		}).
		WithBackoffStrategy(nil),
	)

	expectedErr := error(nil)
	actualErr := publisher.Publish(ctx, payload)

	expectedLog.Errorf(errRandomError, "cannot write message to Kafka")

	assert.Equal(t, expectedErr, actualErr)
	assert.Equal(t, expectedWriter, actualWriter)
	assert.Equal(t, expectedLog, actualLog)
}

type MockWriter_caseFailFirstAndOthersFailToo struct{} //nolint:revive,stylecheck

func (w *MockWriter_caseFailFirstAndOthersFailToo) Close() error {
	return nil
}

func (w *MockWriter_caseFailFirstAndOthersFailToo) WriteMessages(ctx context.Context, _ ...kafka.Message) error {
	_, cancel := context.WithCancel(ctx)

	defer cancel()

	return errRandomError // We've to log, just to log this error, then move on with the strategy and on and on...
}

type backoffStrategy_Case_Fail_FirstAndOthersFailToo struct{} //nolint:revive,stylecheck

func (backoffStrategy_Case_Fail_FirstAndOthersFailToo) Wait() {
	time.Sleep(time.Hour)
}

func Test_Case_Fail_FirstAndOthersFailToo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	payload := []byte("never be published")
	calledOnceTheProcessDroppedMsg := false

	expectedLog := log.NewMockLogger()
	actualLog := log.NewMockLogger()

	expectedWriter := MockWriter_caseFailFirstAndOthersFailToo{}
	actualWriter := MockWriter_caseFailFirstAndOthersFailToo{}

	publisher := kafko.NewPublisher(actualLog, kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return &actualWriter
		}).
		WithBackoffStrategy(&backoffStrategy_Case_Fail_FirstAndOthersFailToo{}).
		WithProcessDroppedMsg(func(msg *kafka.Message, lastErr error, log kafko.Logger) {
			calledOnceTheProcessDroppedMsg = true
		}),
	)

	waitForFirstFail := make(chan struct{}, 1)

	go func() {
		go func() {
			time.Sleep(time.Millisecond)
			waitForFirstFail <- struct{}{}
		}()

		actualErr := publisher.Publish(ctx, payload)

		assert.Nil(t, actualErr)
		t.Error("not waiting to run this part")
		t.Fail()
	}()

	<-waitForFirstFail

	expectedErr := kafko.ErrMessageDropped
	actualErr := publisher.Publish(ctx, payload)

	expectedLog.Errorf(errRandomError, "cannot write message to Kafka")

	assert.Equal(t, expectedErr, actualErr)
	assert.Equal(t, expectedWriter, actualWriter)
	assert.Equal(t, expectedLog, actualLog)
	assert.True(t, calledOnceTheProcessDroppedMsg)
}
