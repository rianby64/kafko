package kafko_test

import (
	"context"
	"testing"
	"time"

	"kafko"
	"kafko/log"
	"kafko/mocks"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

var (
	errRandomForBackoffStrategy   = errors.New("a random error for backoff handler")
	errRandomForPublisher         = errors.New("a random error for publisher")
	errRandomForHandlerDroppedMsg = errors.New("a random error for handler dropped msg")
)

type contextMatcherForPublisher struct {
	ctx context.Context //nolint:containedctx
}

func (matcher contextMatcherForPublisher) Matches(ctxAny any) bool {
	_, isContext := ctxAny.(context.Context)

	return isContext
}

func (matcher contextMatcherForPublisher) String() string {
	return "contextMatcher"
}

func newContextMatcherForPublisher(ctx context.Context) *contextMatcherForPublisher {
	return &contextMatcherForPublisher{
		ctx: ctx,
	}
}

func Test_Publisher_single_message_OK(t *testing.T) {
	t.Parallel()

	mockPayload := []byte("mocked message")
	mockMessage := kafka.Message{
		Value: mockPayload,
	}

	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctxMatcher := newContextMatcherForPublisher(ctx)
	mockWriter := mocks.NewMockWriter(ctrl)

	gomock.InOrder(
		mockWriter.EXPECT().
			WriteMessages(ctxMatcher, mockMessage).
			Return(nil),

		mockWriter.EXPECT().
			Close().
			Return(nil),
	)

	actualLog := log.NewMockLogger()
	opts := kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return mockWriter
		})

	publisher := kafko.NewPublisher(actualLog, opts)

	publishErr := publisher.Publish(ctx, mockPayload)

	assert.Nil(t, publishErr)

	shutdownErr := publisher.Shutdown(ctx)

	assert.Nil(t, shutdownErr)
}

func Test_Publisher_single_message_WriteMessages_err_first_time_but_second_time_noerr_OK(t *testing.T) {
	t.Parallel()

	mockPayload := []byte("mocked message")
	mockMessage := kafka.Message{
		Value: mockPayload,
	}

	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctxMatcher := newContextMatcherForPublisher(ctx)
	mockWriter := mocks.NewMockWriter(ctrl)
	mockHandlerDroppedMsg := mocks.NewMockMsgHandler(ctrl)
	mockBackoff := mocks.NewMockBackoffStrategy(ctrl)

	gomock.InOrder(
		mockWriter.EXPECT().
			WriteMessages(ctxMatcher, mockMessage).
			Return(errRandomForPublisher),

		mockHandlerDroppedMsg.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(nil),

		mockBackoff.EXPECT().Wait(ctxMatcher).Return(nil),

		mockWriter.EXPECT().
			WriteMessages(ctxMatcher, mockMessage).
			Return(nil),

		mockWriter.EXPECT().
			Close().
			Return(nil),
	)

	actualLog := log.NewMockLogger()
	opts := kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return mockWriter
		}).
		WithHandlerDropped(mockHandlerDroppedMsg).
		WithBackoffStrategyFactory(func() kafko.BackoffStrategy {
			return mockBackoff
		})

	publisher := kafko.NewPublisher(actualLog, opts)

	publishErr := publisher.Publish(ctx, mockPayload)

	assert.Nil(t, publishErr)

	shutdownErr := publisher.Shutdown(ctx)

	assert.Nil(t, shutdownErr)
}

func Test_Publisher_single_message_WriteMessages_err_then_backoff_err(t *testing.T) {
	t.Parallel()

	mockPayload := []byte("mocked message")
	mockMessage := kafka.Message{
		Value: mockPayload,
	}

	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctxMatcher := newContextMatcherForPublisher(ctx)
	mockWriter := mocks.NewMockWriter(ctrl)
	mockHandlerDroppedMsg := mocks.NewMockMsgHandler(ctrl)
	mockBackoff := mocks.NewMockBackoffStrategy(ctrl)

	gomock.InOrder(
		mockWriter.EXPECT().
			WriteMessages(ctxMatcher, mockMessage).
			Return(errRandomForPublisher),

		mockHandlerDroppedMsg.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(nil),

		mockBackoff.EXPECT().Wait(ctxMatcher).Return(errRandomForBackoffStrategy),

		mockWriter.EXPECT().
			Close().
			Return(nil),
	)

	actualLog := log.NewMockLogger()
	opts := kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return mockWriter
		}).
		WithHandlerDropped(mockHandlerDroppedMsg).
		WithBackoffStrategyFactory(func() kafko.BackoffStrategy {
			return mockBackoff
		})

	publisher := kafko.NewPublisher(actualLog, opts)

	publishErr := publisher.Publish(ctx, mockPayload)

	assert.ErrorIs(t, publishErr, errRandomForBackoffStrategy)
	assert.ErrorIs(t, publishErr, errRandomForPublisher)

	shutdownErr := publisher.Shutdown(ctx)

	assert.Nil(t, shutdownErr)
}

func Test_Publisher_single_message_WriteMessages_err_handle_dropped_msg_err(t *testing.T) {
	t.Parallel()

	mockPayload := []byte("mocked message")
	mockMessage := kafka.Message{
		Value: mockPayload,
	}

	ctrl, ctx := gomock.WithContext(context.Background(), t)

	defer ctrl.Finish()

	ctxMatcher := newContextMatcherForPublisher(ctx)
	mockWriter := mocks.NewMockWriter(ctrl)
	mockHandlerDroppedMsg := mocks.NewMockMsgHandler(ctrl)

	gomock.InOrder(
		mockWriter.EXPECT().
			WriteMessages(ctxMatcher, mockMessage).
			Return(errRandomForPublisher),

		mockHandlerDroppedMsg.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(errRandomForHandlerDroppedMsg),

		mockWriter.EXPECT().
			Close().
			Return(nil),
	)

	actualLog := log.NewMockLogger()
	opts := kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return mockWriter
		}).
		WithHandlerDropped(mockHandlerDroppedMsg)

	publisher := kafko.NewPublisher(actualLog, opts)

	publishErr := publisher.Publish(ctx, mockPayload)

	assert.ErrorIs(t, publishErr, errRandomForHandlerDroppedMsg)
	assert.ErrorIs(t, publishErr, errRandomForPublisher)

	shutdownErr := publisher.Shutdown(ctx)

	assert.Nil(t, shutdownErr)
}

func Test_Publisher_call_tooSlow_WriteMessages_but_Shutdown_cancels_WriteMessage_OK(t *testing.T) {
	t.Parallel()

	mockPayload := []byte("mocked message")
	mockMessage := kafka.Message{
		Value: mockPayload,
	}

	inform := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctxMatcher := newContextMatcherForPublisher(ctx)
	mockWriter := mocks.NewMockWriter(ctrl)
	mockHandlerDroppedMsg := mocks.NewMockMsgHandler(ctrl)

	gomock.InOrder(
		mockWriter.EXPECT().
			WriteMessages(ctxMatcher, mockMessage).
			DoAndReturn(func(ctx context.Context, msg kafka.Message) error {
				go func() {
					inform <- struct{}{}
				}()

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
					t.Fatal("unexpected execution")

					return nil
				}
			}),

		mockHandlerDroppedMsg.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(errRandomForHandlerDroppedMsg),

		mockWriter.EXPECT().
			Close().
			Return(nil),
	)

	actualLog := log.NewMockLogger()
	opts := kafko.NewOptionsPublisher().
		WithWriterFactory(func() kafko.Writer {
			return mockWriter
		}).
		WithHandlerDropped(mockHandlerDroppedMsg)

	publisher := kafko.NewPublisher(actualLog, opts)

	go func() {
		<-inform

		cancel()

		time.Sleep(time.Millisecond) // temporary hack

		shutdownErr := publisher.Shutdown(ctx)

		assert.Nil(t, shutdownErr)
		close(inform)
	}()

	publishErr := publisher.Publish(ctx, mockPayload)

	assert.ErrorIs(t, publishErr, errRandomForHandlerDroppedMsg)
	assert.ErrorIs(t, publishErr, context.Canceled)

	_, ok := <-inform
	t.Log(ok)
}
