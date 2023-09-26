package kafko_test

import (
	"context"
	"testing"

	"kafko"
	"kafko/log"
	"kafko/mocks"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func Test_Listener_OK(t *testing.T) {
	t.Parallel()

	ctl := gomock.NewController(t)

	defer ctl.Finish()

	waitUntilTheEnd := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	actualLog := log.NewMockLogger()
	mockHandler := mocks.NewMockMsgHandler(ctl)
	mockReader := mocks.NewMockReader(ctl)

	mockReader.EXPECT().Close().Return(nil)

	opts := kafko.NewOptionsListener().
		WithHandler(mockHandler).
		WithReaderFactory(func() kafko.Reader {
			return mockReader
		})

	listener := kafko.NewListener(actualLog, opts)

	go func(t *testing.T) {
		t.Helper()

		actualErr := listener.Shutdown(ctx)
		assert.Nil(t, actualErr)

		waitUntilTheEnd <- struct{}{}
	}(t)

	actualErr := listener.Listen(ctx)
	assert.Nil(t, actualErr)

	<-waitUntilTheEnd
}
