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
	errCloseForListener  = errors.New("a close error for listener")
	errRandomForListener = errors.New("a random error for listener")
)

type contextMatcherForListener struct {
	ctx context.Context //nolint:containedctx
}

func (matcher contextMatcherForListener) Matches(ctxAny any) bool {
	_, isContext := ctxAny.(context.Context)

	return isContext
}

func (matcher contextMatcherForListener) String() string {
	return "contextMatcher"
}

func newContextMatcherForListener(ctx context.Context) *contextMatcherForListener {
	return &contextMatcherForListener{
		ctx: ctx,
	}
}

func listener_OK_setup(ctx context.Context, ctrl *gomock.Controller) (chan struct{}, *mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	inform := make(chan struct{}, 1)
	ctxMatcher := newContextMatcherForListener(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctrl)
	mockReader := mocks.NewMockReader(ctrl)

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
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	inform, mockHandler, mockReader := listener_OK_setup(ctx, ctrl)
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

func listener_Handler_Err_case1_setup(ctx context.Context, ctrl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcherForListener(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctrl)
	mockReader := mocks.NewMockReader(ctrl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(errRandomForListener),

		mockReader.EXPECT().
			Close().
			Return(nil),
	)

	return mockHandler, mockReader
}

func Test_Listener_Handler_Err_case1(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	mockHandler, mockReader := listener_Handler_Err_case1_setup(ctx, ctrl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandomForListener)
}

func listener_Handler_Err_case2_setup(ctx context.Context, ctrl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcherForListener(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctrl)
	mockReader := mocks.NewMockReader(ctrl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(errRandomForListener),

		mockReader.EXPECT().
			Close().
			Return(errCloseForListener),
	)

	return mockHandler, mockReader
}

func Test_Listener_Handler_Err_case2(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	mockHandler, mockReader := listener_Handler_Err_case2_setup(ctx, ctrl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandomForListener)
	assert.ErrorIs(t, actualErr, errCloseForListener)
}

func listener_Fetch_Err_case1_setup(ctx context.Context, ctrl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcherForListener(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctrl)
	mockReader := mocks.NewMockReader(ctrl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, errRandomForListener),

		mockReader.EXPECT().
			Close().
			Return(nil),
	)

	return mockHandler, mockReader
}

func Test_Listener_Fetch_Err_case1(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	mockHandler, mockReader := listener_Fetch_Err_case1_setup(ctx, ctrl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandomForListener)
}

func listener_Fetch_Err_case2_setup(ctx context.Context, ctrl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcherForListener(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctrl)
	mockReader := mocks.NewMockReader(ctrl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, errRandomForListener),

		mockReader.EXPECT().
			Close().
			Return(errCloseForListener),
	)

	return mockHandler, mockReader
}

func Test_Listener_Fetch_Err_case2(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	mockHandler, mockReader := listener_Fetch_Err_case2_setup(ctx, ctrl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandomForListener)
	assert.ErrorIs(t, actualErr, errCloseForListener)
}

func listener_Commit_Err_case1_setup(ctx context.Context, ctrl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcherForListener(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctrl)
	mockReader := mocks.NewMockReader(ctrl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(nil),

		mockReader.EXPECT().
			CommitMessages(ctxMatcher, mockMessage).
			Return(errRandomForListener),

		mockReader.EXPECT().
			Close().
			Return(nil),
	)

	return mockHandler, mockReader
}

func Test_Listener_Commit_Err_case1(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	mockHandler, mockReader := listener_Commit_Err_case1_setup(ctx, ctrl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandomForListener)
}

func listener_Commit_Err_case2_setup(ctx context.Context, ctrl *gomock.Controller) (*mocks.MockMsgHandler, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	ctxMatcher := newContextMatcherForListener(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctrl)
	mockReader := mocks.NewMockReader(ctrl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(nil),

		mockReader.EXPECT().
			CommitMessages(ctxMatcher, mockMessage).
			Return(errRandomForListener),

		mockReader.EXPECT().
			Close().
			Return(errCloseForListener),
	)

	return mockHandler, mockReader
}

func Test_Listener_Commit_Err_case2(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	mockHandler, mockReader := listener_Commit_Err_case2_setup(ctx, ctrl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)
	actualErr := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr, errRandomForListener)
	assert.ErrorIs(t, actualErr, errCloseForListener)
}

func listener_reListen_reason_Fetch_err_setup(ctx context.Context, ctrl *gomock.Controller) (chan struct{}, *mocks.MockMsgHandler, *mocks.MockReader, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	inform := make(chan struct{}, 1)
	ctxMatcher := newContextMatcherForListener(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctrl)
	mockReader1 := mocks.NewMockReader(ctrl)
	mockReader2 := mocks.NewMockReader(ctrl)

	gomock.InOrder(
		mockReader1.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, errRandomForListener),

		mockReader1.EXPECT().
			Close().
			Return(nil),

		mockReader2.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(nil),

		mockReader2.EXPECT().
			CommitMessages(ctxMatcher, mockMessage).
			Return(nil),

		mockReader2.EXPECT().
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

		mockReader2.EXPECT().
			Close().
			Return(nil),
	)

	return inform, mockHandler, mockReader1, mockReader2
}

func Test_reListen_reason_Fetch_err(t *testing.T) { //nolint:dupl
	t.Parallel()

	waitUntilTheEnd := make(chan struct{}, 1)
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	restartTimes := 0
	inform, mockHandler, mockReader1, mockReader2 := listener_reListen_reason_Fetch_err_setup(ctx, ctrl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			defer func() {
				restartTimes++
			}()

			if restartTimes == 0 {
				return mockReader1
			}

			if restartTimes == 1 {
				return mockReader2
			}

			panic("unexpected times of restarting")
		})

	listener := kafko.NewListener(actualLog, opts)

	go func(t *testing.T) {
		t.Helper()

		<-inform

		actualErr := listener.Shutdown(ctx)
		assert.Nil(t, actualErr)

		close(waitUntilTheEnd)
	}(t)

	actualErr1 := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr1, errRandomForListener)

	actualErr2 := listener.Listen(ctx)
	assert.Nil(t, actualErr2)

	<-waitUntilTheEnd
}

func listener_reListen_reason_Handler_err_setup(ctx context.Context, ctrl *gomock.Controller) (chan struct{}, *mocks.MockMsgHandler, *mocks.MockReader, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	inform := make(chan struct{}, 1)
	ctxMatcher := newContextMatcherForListener(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctrl)
	mockReader1 := mocks.NewMockReader(ctrl)
	mockReader2 := mocks.NewMockReader(ctrl)

	gomock.InOrder(
		mockReader1.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(errRandomForListener),

		mockReader1.EXPECT().
			Close().
			Return(nil),

		mockReader2.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(nil),

		mockReader2.EXPECT().
			CommitMessages(ctxMatcher, mockMessage).
			Return(nil),

		mockReader2.EXPECT().
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

		mockReader2.EXPECT().
			Close().
			Return(nil),
	)

	return inform, mockHandler, mockReader1, mockReader2
}

func Test_reListen_reason_Handler_err(t *testing.T) { //nolint:dupl
	t.Parallel()

	waitUntilTheEnd := make(chan struct{}, 1)
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	restartTimes := 0
	inform, mockHandler, mockReader1, mockReader2 := listener_reListen_reason_Handler_err_setup(ctx, ctrl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			defer func() {
				restartTimes++
			}()

			if restartTimes == 0 {
				return mockReader1
			}

			if restartTimes == 1 {
				return mockReader2
			}

			panic("unexpected times of restarting")
		})

	listener := kafko.NewListener(actualLog, opts)

	go func(t *testing.T) {
		t.Helper()

		<-inform

		actualErr := listener.Shutdown(ctx)
		assert.Nil(t, actualErr)

		close(waitUntilTheEnd)
	}(t)

	actualErr1 := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr1, errRandomForListener)

	actualErr2 := listener.Listen(ctx)
	assert.Nil(t, actualErr2)

	<-waitUntilTheEnd
}

func listener_reListen_reason_Commit_err_setup(ctx context.Context, ctrl *gomock.Controller) (chan struct{}, *mocks.MockMsgHandler, *mocks.MockReader, *mocks.MockReader) { //nolint:revive,stylecheck
	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	inform := make(chan struct{}, 1)
	ctxMatcher := newContextMatcherForListener(ctx)
	mockHandler := mocks.NewMockMsgHandler(ctrl)
	mockReader1 := mocks.NewMockReader(ctrl)
	mockReader2 := mocks.NewMockReader(ctrl)

	gomock.InOrder(
		mockReader1.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(nil),

		mockReader1.EXPECT().
			CommitMessages(ctxMatcher, mockMessage).
			Return(errRandomForListener),

		mockReader1.EXPECT().
			Close().
			Return(nil),

		mockReader2.EXPECT().
			FetchMessage(ctxMatcher).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctxMatcher, &mockMessage).
			Return(nil),

		mockReader2.EXPECT().
			CommitMessages(ctxMatcher, mockMessage).
			Return(nil),

		mockReader2.EXPECT().
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

		mockReader2.EXPECT().
			Close().
			Return(nil),
	)

	return inform, mockHandler, mockReader1, mockReader2
}

func Test_reListen_reason_Commit_err(t *testing.T) { //nolint:dupl
	t.Parallel()

	waitUntilTheEnd := make(chan struct{}, 1)
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	restartTimes := 0
	inform, mockHandler, mockReader1, mockReader2 := listener_reListen_reason_Commit_err_setup(ctx, ctrl)
	actualLog := log.NewMockLogger()

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			defer func() {
				restartTimes++
			}()

			if restartTimes == 0 {
				return mockReader1
			}

			if restartTimes == 1 {
				return mockReader2
			}

			panic("unexpected times of restarting")
		})

	listener := kafko.NewListener(actualLog, opts)

	go func(t *testing.T) {
		t.Helper()

		<-inform

		actualErr := listener.Shutdown(ctx)
		assert.Nil(t, actualErr)

		close(waitUntilTheEnd)
	}(t)

	actualErr1 := listener.Listen(ctx)
	assert.ErrorIs(t, actualErr1, errRandomForListener)

	actualErr2 := listener.Listen(ctx)
	assert.Nil(t, actualErr2)

	<-waitUntilTheEnd
}
