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

func Test_Listener_OK(t *testing.T) { //nolint:funlen
	t.Parallel()

	waitUntilTheEnd := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	ctl := gomock.NewController(t)

	defer cancel()
	defer ctl.Finish()

	mockMessage := kafka.Message{
		Value: []byte("mocked message"),
	}

	actualLog := log.NewMockLogger()
	mockHandler := mocks.NewMockMsgHandler(ctl)
	mockReader := mocks.NewMockReader(ctl)

	gomock.InOrder(
		mockReader.EXPECT().
			FetchMessage(ctx).
			Return(mockMessage, nil),

		mockHandler.EXPECT().
			Handle(ctx, &mockMessage).
			Return(nil),

		mockReader.EXPECT().
			CommitMessages(ctx, mockMessage).
			Return(nil),

		mockReader.EXPECT().
			FetchMessage(ctx).
			Do(func(ctx context.Context) {
				time.Sleep(time.Second)
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

		time.Sleep(time.Millisecond * 100)

		actualErr := listener.Shutdown(ctx)
		assert.Nil(t, actualErr)

		waitUntilTheEnd <- struct{}{}
	}(t)

	actualErr := listener.Listen(ctx)
	assert.Nil(t, actualErr)

	<-waitUntilTheEnd
}
