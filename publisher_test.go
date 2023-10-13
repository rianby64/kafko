package kafko_test

import (
	"context"
	"testing"

	"kafko"
	"kafko/log"
	"kafko/mocks"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
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
