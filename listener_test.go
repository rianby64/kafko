//nolint:wrapcheck
package kafkame_test

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	listener "github.com/rianby64/kafko"
	log "github.com/rianby64/kafko/log"
)

type MockKafkaReader struct {
	mock.Mock
}

func (m *MockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	args := m.Called(ctx)
	msg, ok := args.Get(0).(kafka.Message)

	if !ok {
		panic("cannot cast")
	}

	err := args.Error(1)

	return msg, err
}

func (m *MockKafkaReader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	args := m.Called(ctx, messages)
	err := args.Error(0)

	return err
}

func (m *MockKafkaReader) Close() error {
	args := m.Called()
	err := args.Error(0)

	return err
}

func TestListen(t *testing.T) {
	t.Parallel()

	t.Run("Successful message processing", testSuccessfulMessageProcessing)
	// t.Run("Error during message fetching", testErrorDuringMessageFetching)
	// t.Run("Message processing timeout", testMessageProcessingTimeout)
	// t.Run("Context cancellation", testContextCancellation)
}

func testSuccessfulMessageProcessing(t *testing.T) {
	t.Parallel()

	msg := []byte("test message")

	mockReader := new(MockKafkaReader)
	mockReader.On("FetchMessage", mock.Anything).Return(kafka.Message{Value: msg}, nil)
	mockReader.On("CommitMessages", mock.Anything, []kafka.Message{{Value: msg}}).Return(nil)

	mockReader.On("Close").Return(nil)

	// Set up a simple logger to collect logs during testing
	log := log.NewLogger()

	// Set up test configurations and options
	config := kafka.ReaderConfig{
		Brokers: []string{"dummy:1234"},
		GroupID: "test-group",
		Topic:   "test-topic",
	}

	processingTimeout := 1 * time.Second
	opts := listener.NewOptions().WithProcessingTimeout(processingTimeout).WithReader(mockReader)

	// Create a test context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener := listener.NewListener("username", "password", "test-group", "test-topic", config.Brokers, log, opts)

	msgProcessed := false

	go func() {
		// I want to be sure the msgChan and errChan behave as expected
		msgChan, errChan := listener.MessageAndErrorChannels()
		assert.Equal(t, msg, <-msgChan)
		errChan <- nil

		// If we are here it's because we have the expected message
		msgProcessed = true

		// So, shutdown
		err := listener.Shutdown(ctx)
		assert.NoError(t, err)
	}()

	err := listener.Listen(ctx)

	assert.NoError(t, err)
	assert.True(t, msgProcessed)
}
