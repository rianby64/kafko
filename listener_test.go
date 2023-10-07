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
	return "contextMatcher"
}

func newContextMatcher(ctx context.Context) *contextMatcher {
	return &contextMatcher{
		ctx: ctx,
	}
}

func listener_OK_setup(ctx context.Context, ctl *gomock.Controller) (chan struct{}, *mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	inform := make(chan struct{}, 1)
	ctxMatcher := newContextMatcher(ctx)
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
				close(inform)

				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
					panic("not expected")
				}
			}).
			Return(mockMessage, nil),

		mockReader.EXPECT().
			Close().
			Return(nil),
	)

	return inform, mockHandler, mockReader
}

func Test_Listener_OK(t *testing.T) {
	t.Parallel()

	waitUntilTheEnd := make(chan struct{}, 1)
	ctx := context.Background()
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	inform, mockHandler, mockReader := listener_OK_setup(ctx, ctl)
	actualLog := log.NewMockLogger()

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

		close(waitUntilTheEnd)
	}(t)

	actualErr := listener.Listen(ctx)
	assert.Nil(t, actualErr)

	<-waitUntilTheEnd
}

func listener_Handler_Err_case1_setup(ctx context.Context, ctl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcher(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctl)
	mockReader := mocks.NewMockReader(ctl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(errRandom),

		mockReader.EXPECT().
			Close().
			Return(nil),
	)

	return mockHandler, mockReader
}

func Test_Listener_Handler_Err_case1(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	mockHandler, mockReader := listener_Handler_Err_case1_setup(ctx, ctl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandom)
}

func listener_Handler_Err_case2_setup(ctx context.Context, ctl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcher(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctl)
	mockReader := mocks.NewMockReader(ctl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(errRandom),

		mockReader.EXPECT().
			Close().
			Return(errClose),
	)

	return mockHandler, mockReader
}

func Test_Listener_Handler_Err_case2(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	mockHandler, mockReader := listener_Handler_Err_case2_setup(ctx, ctl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandom)
	assert.ErrorIs(t, actualErr, errClose)
}

func listener_Fetch_Err_case1_setup(ctx context.Context, ctl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcher(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctl)
	mockReader := mocks.NewMockReader(ctl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, errRandom),

		mockReader.EXPECT().
			Close().
			Return(nil),
	)

	return mockHandler, mockReader
}

func Test_Listener_Fetch_Err_case1(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	mockHandler, mockReader := listener_Fetch_Err_case1_setup(ctx, ctl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandom)
}

func listener_Fetch_Err_case2_setup(ctx context.Context, ctl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcher(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctl)
	mockReader := mocks.NewMockReader(ctl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, errRandom),

		mockReader.EXPECT().
			Close().
			Return(errClose),
	)

	return mockHandler, mockReader
}

func Test_Listener_Fetch_Err_case2(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	mockHandler, mockReader := listener_Fetch_Err_case2_setup(ctx, ctl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandom)
	assert.ErrorIs(t, actualErr, errClose)
}

func listener_Commit_Err_case1_setup(ctx context.Context, ctl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcher(ctx)
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
			Return(errRandom),

		mockReader.EXPECT().
			Close().
			Return(nil),
	)

	return mockHandler, mockReader
}

func Test_Listener_Commit_Err_case1(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	mockHandler, mockReader := listener_Commit_Err_case1_setup(ctx, ctl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandom)
}

func listener_Commit_Err_case2_setup(ctx context.Context, ctl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcher(ctx)
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
			Return(errRandom),

		mockReader.EXPECT().
			Close().
			Return(errClose),
	)

	return mockHandler, mockReader
}

func Test_Listener_Commit_Err_case2(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctl := gomock.NewController(t)

	defer ctl.Finish()

	mockHandler, mockReader := listener_Commit_Err_case2_setup(ctx, ctl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandom)
	assert.ErrorIs(t, actualErr, errClose)
}
