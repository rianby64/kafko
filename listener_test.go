package kafko_test

import (
	"context"
	"testing"
	"time"

	"kafko"
	"kafko/log"
	"kafko/mocks"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

type contextMatcher struct {
	ctx context.Context //nolint:containedctx
}

func (matcher contextMatcher) Matches(ctxAny any) bool {
	_, isContext := ctxAny.(context.Context)

	return isContext
}

func (matcher contextMatcher) String() string {
	return ""
}

func newContextMatcher(ctx context.Context) *contextMatcher {
	return &contextMatcher{
		ctx: ctx,
	}
}

func Test_Listener_OK(t *testing.T) { //nolint:funlen
	t.Parallel()

	waitUntilTheEnd := make(chan struct{}, 1)
	ctx := context.Background()
	ctl := gomock.NewController(t)
	ctxMatcher := newContextMatcher(ctx)

	defer ctl.Finish()

	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	inform := make(chan struct{}, 1)
	actualLog := log.NewMockLogger()
	mockHandler := mocks.NewMockMsgHandler(ctl)
	mockReader := mocks.NewMockReader(ctl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(nil),

		mockReader.EXPECT().
			CommitMessages(ctxMatcher, mockMessage).
			Return(nil),

		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Do(func(ctx context.Context) {
				inform <- struct{}{}

				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Hour):
					return
				}
			}).
			Return(mockMessage, nil),
	)

	mockReader.EXPECT().
		Close().
		Return(nil)

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)

	go func(t *testing.T) {
		t.Helper()

		<-inform

		actualErr := listener.Shutdown(ctx)
		assert.Nil(t, actualErr)

		waitUntilTheEnd <- struct{}{}
	}(t)

	actualErr := listener.Listen(ctx)
	assert.Nil(t, actualErr)

	<-waitUntilTheEnd
}
