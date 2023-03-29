package kafkame_test

/*
import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	listener "github.com/rianby64/kafkame"
	log "github.com/rianby64/kafkame/log"
)

type MockKafkaReader struct {
	mock.Mock
}

func (m *MockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	args := m.Called(ctx)

	return args.Get(0).(kafka.Message), args.Error(1)
}

func (m *MockKafkaReader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	args := m.Called(ctx, messages)

	return args.Error(1)
}

func (m *MockKafkaReader) Close() error {
	args := m.Called()

	return args.Error(0)
}

func TestListen(t *testing.T) {
	t.Run("Successful message processing", testSuccessfulMessageProcessing)
	// t.Run("Error during message fetching", testErrorDuringMessageFetching)
	// t.Run("Message processing timeout", testMessageProcessingTimeout)
	// t.Run("Context cancellation", testContextCancellation)
}

func testSuccessfulMessageProcessing(t *testing.T) {
	mockReader := new(MockKafkaReader)
	mockReader.On("FetchMessage", mock.Anything).Return(kafka.Message{Value: []byte("test message")}, nil)
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

	l := listener.NewListener("username", "password", "test-group", "test-topic", config.Brokers, log, opts)
	err := l.Listen(ctx)

	assert.NoError(t, err)
}
*/
