//nolint:wrapcheck
package kafko_test

import (
	"context"
	"errors"
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

// func TestListen(t *testing.T) {
// 	t.Parallel()

// 	t.Run("Successful message processing", TestSuccessfulMessageProcessing)
// 	t.Run("Error unrecoverable during message fetching", TestUnrecoverableErrorDuringMessageFetching)
// 	t.Run("Error recoverable during message fetching", TestRecoverableErrorDuringMessageFetching)
// 	// t.Run("Message processing timeout", testMessageProcessingTimeout)
// 	// t.Run("Context cancellation", testContextCancellation)
// }

func TestSuccessfulMessageProcessing(t *testing.T) {
	t.Parallel()

	msg := []byte("test message")

	mockReader := new(MockKafkaReader)
	mockReader.On("FetchMessage", mock.Anything).Return(kafka.Message{Value: msg}, nil)
	mockReader.On("CommitMessages", mock.Anything, []kafka.Message{{Value: msg}}).Return(nil)
	mockReader.On("Close").Return(nil)

	// Set up a simple logger to collect logs during testing
	log := log.NewLogger()

	processingTimeout := 1 * time.Second
	opts := listener.NewOptions().
		WithProcessingTimeout(processingTimeout).
		WithReaderFactory(func() listener.Reader {
			return mockReader
		})

	// Create a test context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener := listener.NewListener(log, opts)

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

func TestUnrecoverableErrorDuringMessageFetching(t *testing.T) {
	t.Parallel()

	msg := []byte("test message")
	errorForFetchMessage := errors.New("error for fetch message")

	mockReader := new(MockKafkaReader)
	mockReader.On("FetchMessage", mock.Anything).Return(kafka.Message{Value: msg}, errorForFetchMessage)
	mockReader.On("CommitMessages", mock.Anything, []kafka.Message{{Value: msg}}).Return(nil)
	mockReader.On("Close").Return(nil)

	// Set up a simple logger to collect logs during testing
	log := log.NewLogger()

	processingTimeout := 1 * time.Second
	opts := listener.NewOptions().
		WithProcessingTimeout(processingTimeout).
		WithReaderFactory(func() listener.Reader {
			return mockReader
		})

	// Create a test context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener := listener.NewListener(log, opts)

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

	assert.ErrorIs(t, err, errorForFetchMessage)
	assert.False(t, msgProcessed)
}

func TestRecoverableErrorDuringMessageFetching(t *testing.T) {
	t.Parallel()

	msg := []byte("test message")
	errorForFetchMessage := kafka.NetworkException

	mockReader := new(MockKafkaReader)
	mockReader.On("FetchMessage", mock.Anything).Return(kafka.Message{Value: msg}, &errorForFetchMessage)
	mockReader.On("CommitMessages", mock.Anything, []kafka.Message{{Value: msg}}).Return(nil)
	mockReader.On("Close").Return(nil)

	// Set up a simple logger to collect logs during testing
	log := log.NewLogger()

	reconnections := 0

	// Create a test context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	opts := listener.NewOptions().
		WithProcessingTimeout(1 * time.Second).
		WithReconnectInterval(1 * time.Second).
		WithReaderFactory(func() listener.Reader {
			reconnections++

			return mockReader
		})

	listener := listener.NewListener(log, opts)

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

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, msgProcessed)
	assert.GreaterOrEqual(t, reconnections, 2)
}
