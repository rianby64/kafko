package kafko_test

import (
	"context"
	"errors"
	"testing"

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
	ErrRandomError = errors.New("a random error")
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

		return ErrRandomError
	}

	w.writtenMsgs = append(w.writtenMsgs, msgs...)

	return nil
}

// Test_Case_OK_WriteMessages_failedFirstAttempt_successSecondAttempt has a potential error:
//
//	no wait time for the next attempt
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

	expectedLog.Errorf(ErrRandomError, "cannot write message to Kafka")

	assert.Equal(t, expectedErr, actualErr)
	assert.Equal(t, expectedWriter, actualWriter)
	assert.Equal(t, expectedLog, actualLog)
}
