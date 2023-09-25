package kafko_test

import (
	"context"
	"testing"

	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

type MockListenerMsgHangler_OK struct { //nolint:revive,stylecheck
}

func (MockListenerMsgHangler_OK) Handle(context.Context, *kafka.Message) error {
	return nil
}

func Test_Listener_OK(t *testing.T) {
	t.Parallel()

	waitUntilTheEnd := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	actualLog := log.NewMockLogger()

	listener := kafko.NewListener(actualLog, kafko.NewOptionsListener().WithHandler(new(MockListenerMsgHangler_OK)))

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
