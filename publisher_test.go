package kafko_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"kafko"
	"kafko/log"
	"kafko/mocks"
)

func Test_Case_OK_noClose_oneMessage_WriteMessages(t *testing.T) {
	t.Parallel()

	ctl := gomock.NewController(t)
	ctx := context.Background()
	ctxMatcher := newContextMatcher(ctx)
	payload := []byte("payload OK")
	expectedMsgs := []kafka.Message{{Value: payload}}

	expectedLog := log.NewMockLogger()
	actualLog := log.NewMockLogger()

	actualWriter := mocks.NewMockWriter(ctl)

	actualWriter.EXPECT().WriteMessages(ctxMatcher, expectedMsgs)

	publisher := kafko.NewPublisher(actualLog, kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return actualWriter
		}),
	)

	expectedErr := error(nil)
	actualErr := publisher.Publish(ctx, payload)

	assert.Equal(t, expectedErr, actualErr)
	assert.Equal(t, expectedLog, actualLog)
}

var (
	errClose  = errors.New("a close error")
	errRandom = errors.New("a random error")
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

		return errRandom
	}

	w.writtenMsgs = append(w.writtenMsgs, msgs...)

	return nil
}

type MockBackoffStrategy_NoErrCloseAppendAtWriteMessagesFailFirstTimeSuccessSecondTime struct{} //nolint:revive,stylecheck

func (MockBackoffStrategy_NoErrCloseAppendAtWriteMessagesFailFirstTimeSuccessSecondTime) Wait(_ context.Context) {
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
		WithBackoffStrategy(&MockBackoffStrategy_NoErrCloseAppendAtWriteMessagesFailFirstTimeSuccessSecondTime{}),
	)

	expectedErr := error(nil)
	actualErr := publisher.Publish(ctx, payload)

	expectedLog.Errorf(errRandom, "cannot write message to Kafka")

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

	time.Sleep(time.Millisecond)

	defer cancel()

	return errRandom // We've to log, just to log this error, then move on with the strategy and on and on...
}

type backoffStrategy_Case_Fail_FirstAttempt_and_OthersFailToo struct{} //nolint:revive,stylecheck

func (b *backoffStrategy_Case_Fail_FirstAttempt_and_OthersFailToo) Wait(_ context.Context) {
	time.Sleep(time.Hour)
}

type MockProcessDroppedMsgHandler_Case_Fail_FirstAttempt_and_OthersFailToo struct { //nolint:revive,stylecheck
	calledOnceTheProcessDroppedMsg bool
}

func (m *MockProcessDroppedMsgHandler_Case_Fail_FirstAttempt_and_OthersFailToo) Handle(_ context.Context, _ *kafka.Message) error {
	m.calledOnceTheProcessDroppedMsg = true

	return errRandom
}

func Test_Case_Fail_FirstAttempt_and_OthersFailToo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	payload := []byte("never be published")

	expectedLog := log.NewMockLogger()
	actualLog := log.NewMockLogger()

	expectedWriter := MockWriter_caseFailFirstAndOthersFailToo{}
	actualWriter := MockWriter_caseFailFirstAndOthersFailToo{}

	processDroppedMsg := &MockProcessDroppedMsgHandler_Case_Fail_FirstAttempt_and_OthersFailToo{}

	publisher := kafko.NewPublisher(actualLog, kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return &actualWriter
		}).
		WithBackoffStrategy(&backoffStrategy_Case_Fail_FirstAttempt_and_OthersFailToo{}).
		WithProcessDroppedMsg(processDroppedMsg),
	)

	waitForFirstFail := make(chan struct{}, 1)

	go func() {
		go func() {
			time.Sleep(time.Millisecond * 100)
			waitForFirstFail <- struct{}{}
		}()

		actualErr := publisher.Publish(ctx, payload)

		assert.Nil(t, actualErr)
		t.Error("not waiting to run this part")
		t.Fail()
	}()

	<-waitForFirstFail

	expectedErr := errRandom
	actualErr := publisher.Publish(ctx, payload)

	expectedLog.Errorf(errRandom, "cannot write message to Kafka")

	assert.ErrorIs(t, actualErr, expectedErr)
	assert.Equal(t, expectedWriter, actualWriter)
	assert.Equal(t, expectedLog, actualLog)
	assert.True(t, processDroppedMsg.calledOnceTheProcessDroppedMsg)
}

type backoffStrategy_Case_Fail_AllAtempts_But_CalledOnlyOnce struct { //nolint:revive,stylecheck
	t           *testing.T
	calledTimes int
}

func (b *backoffStrategy_Case_Fail_AllAtempts_But_CalledOnlyOnce) Wait(_ context.Context) {
	b.calledTimes++

	if b.calledTimes > 1 {
		b.t.Error("should be called once, and only once")
	}

	time.Sleep(time.Hour)
}

type MockProcessDroppedMsgHandler_Case_Fail_AllStartedPublish_AllFailed_OnyOneDoesRetry_OtherDoFail struct { //nolint:revive,stylecheck
	calledProcessDroppedMsg int
}

func (m *MockProcessDroppedMsgHandler_Case_Fail_AllStartedPublish_AllFailed_OnyOneDoesRetry_OtherDoFail) Handle(_ context.Context, _ *kafka.Message) error {
	m.calledProcessDroppedMsg++

	return errRandom
}

func Test_Case_Fail_AllStartedPublish_AllFailed_OnyOneDoesRetry_OtherDoFail(t *testing.T) {
	t.Parallel()

	NumberOfAttempts := 1000
	waitGroup := sync.WaitGroup{}

	ctx := context.Background()

	actualLog := log.NewMockLogger()
	actualWriter := MockWriter_caseFailFirstAndOthersFailToo{}

	publisher := kafko.NewPublisher(actualLog, kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return &actualWriter
		}).
		WithBackoffStrategy(&backoffStrategy_Case_Fail_AllAtempts_But_CalledOnlyOnce{t: t}).
		WithProcessDroppedMsg(&MockProcessDroppedMsgHandler_Case_Fail_AllStartedPublish_AllFailed_OnyOneDoesRetry_OtherDoFail{}),
	)

	waitGroup.Add(NumberOfAttempts - 1) // one of the attempts will fall into a infinity loop, so do not count int.

	expectedErr := errRandom
	failedPublish := 0
	failedPublishChan := make(chan struct{}, NumberOfAttempts)

	go func() {
		for range failedPublishChan {
			failedPublish++
		}
	}()

	for index := 0; index < NumberOfAttempts; index++ {
		go func(index int) {
			defer waitGroup.Done()

			actualErr := publisher.Publish(ctx, []byte(fmt.Sprint(index)))

			assert.ErrorIs(t, actualErr, expectedErr)
			failedPublishChan <- struct{}{}
		}(index)
	}

	waitGroup.Wait()
	time.Sleep(time.Millisecond)

	assert.Equal(t, NumberOfAttempts-1, failedPublish)
}

// Perhaps I should test the case when a panic happens under some conditions...

func Test_Case_OK_CloseByShutdown_oneMessage_WriteMessages(t *testing.T) {
	t.Parallel()

	ctl := gomock.NewController(t)
	ctx := context.Background()
	ctxMatcher := newContextMatcher(ctx)
	payload := []byte("payload OK")
	expectedMsgs := []kafka.Message{{Value: payload}}

	expectedLog := log.NewMockLogger()
	actualLog := log.NewMockLogger()

	actualWriter := mocks.NewMockWriter(ctl)

	gomock.InOrder(
		actualWriter.EXPECT().WriteMessages(ctxMatcher, expectedMsgs),
		actualWriter.EXPECT().Close().Return(nil),
	)

	publisher := kafko.NewPublisher(actualLog, kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return actualWriter
		}),
	)

	expectedPublishErr := error(nil)
	actualPublishErr := publisher.Publish(ctx, payload)

	assert.Equal(t, expectedPublishErr, actualPublishErr)

	expectedShutdownErr := error(nil)
	actualShutdownErr := publisher.Shutdown(ctx)

	assert.Equal(t, expectedShutdownErr, actualShutdownErr)
	assert.Equal(t, expectedLog, actualLog)
}

type MockWriter_caseCloseDeadlock_AppendAtWriteMessages struct { //nolint:revive,stylecheck
	writtenMsgs []kafka.Message

	closeCalledTimes int
}

func (w *MockWriter_caseCloseDeadlock_AppendAtWriteMessages) Close() error {
	w.closeCalledTimes++

	time.Sleep(time.Hour) // block "forever"

	return nil
}

func (w *MockWriter_caseCloseDeadlock_AppendAtWriteMessages) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	_, cancel := context.WithCancel(ctx)

	defer cancel()

	w.writtenMsgs = append(w.writtenMsgs, msgs...)

	return nil
}

func Test_Case_OK_CloseByShutdown_CloseBlockedForever_oneMessage_WriteMessages(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	payload := []byte("payload OK")

	expectedLog := log.NewMockLogger()
	actualLog := log.NewMockLogger()

	expectedWriter := MockWriter_caseCloseDeadlock_AppendAtWriteMessages{
		writtenMsgs:      []kafka.Message{{Value: payload}},
		closeCalledTimes: 1,
	}
	actualWriter := MockWriter_caseCloseDeadlock_AppendAtWriteMessages{
		writtenMsgs: []kafka.Message{},
	}

	publisher := kafko.NewPublisher(actualLog, kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return &actualWriter
		}),
	)

	expectedPublishErr := error(nil)
	actualPublishErr := publisher.Publish(ctx, payload)

	assert.Equal(t, expectedPublishErr, actualPublishErr)

	go cancel() // let's simulate this scenario, context has been canceled somewhere else

	expectedShutdownErr := context.Canceled
	actualShutdownErr := publisher.Shutdown(ctx)

	assert.ErrorIs(t, actualShutdownErr, expectedShutdownErr)

	assert.Equal(t, expectedWriter, actualWriter)
	assert.Equal(t, expectedLog, actualLog)
}

func Test_Case_OK_CloseByShutdown_CloseBlockedForever_oneMessage_WriteMessages_FurtherPublishFails(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	payload := []byte("payload OK")

	expectedLog := log.NewMockLogger()
	actualLog := log.NewMockLogger()

	expectedWriter := MockWriter_caseCloseDeadlock_AppendAtWriteMessages{
		writtenMsgs:      []kafka.Message{{Value: payload}},
		closeCalledTimes: 1,
	}
	actualWriter := MockWriter_caseCloseDeadlock_AppendAtWriteMessages{
		writtenMsgs: []kafka.Message{},
	}

	publisher := kafko.NewPublisher(actualLog, kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return &actualWriter
		}),
	)

	expectedPublishErr := error(nil)
	actualPublishErr := publisher.Publish(ctx, payload)

	assert.Equal(t, expectedPublishErr, actualPublishErr)

	go cancel() // let's simulate this scenario, context has been canceled somewhere else

	expectedShutdownErr := context.Canceled
	actualShutdownErr := publisher.Shutdown(ctx)

	assert.ErrorIs(t, actualShutdownErr, expectedShutdownErr)

	expectedPublishAfterShutdownErr := kafko.ErrResourceUnavailable
	actualPublishAfterShutdownErr := publisher.Publish(ctx, payload)

	assert.ErrorIs(t, actualPublishAfterShutdownErr, expectedPublishAfterShutdownErr)

	assert.Equal(t, expectedWriter, actualWriter)
	assert.Equal(t, expectedLog, actualLog)
}
