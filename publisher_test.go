package kafko_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/m3co/kafko"
	"github.com/m3co/kafko/log"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockWriter mocks the Writer interface.
type MockWriter struct {
	mock.Mock
}

func (m *MockWriter) Close() error {
	args := m.Called()

	return args.Error(0) //nolint:wrapcheck
}

func (m *MockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	args := m.Called(ctx, msgs)

	return args.Error(0) //nolint:wrapcheck
}

var (
	errMockBadMarshalJSON = errors.New("JSON marshaling error")
)

// CustomTypeWithMarshalError is a custom type that always returns an error when marshaling to JSON.
type CustomTypeWithMarshalError struct{}

func (ct *CustomTypeWithMarshalError) MarshalJSON() ([]byte, error) {
	return nil, errMockBadMarshalJSON
}

func TestPublisher(t *testing.T) { //nolint:funlen
	t.Parallel()

	ctx := context.Background()

	t.Run("successful message publishing", func(t *testing.T) {
		t.Parallel()

		mockWriter := new(MockWriter)
		mockLogger := log.NewLogger()

		writerFactory := func() kafko.Writer {
			return mockWriter
		}

		opts := kafko.NewOptionsPublisher().WithWriterFactory(writerFactory)
		publisher := kafko.NewPublisher(mockLogger, opts)

		payload := struct { //nolint:musttag
			Message string
		}{
			Message: "test",
		}

		messageValue, _ := json.Marshal(payload)
		expectedMessages := []kafka.Message{
			{
				Value: messageValue,
			},
		}

		mockWriter.On("WriteMessages", ctx, expectedMessages).Return(nil)

		err := publisher.Publish(ctx, payload)

		assert.NoError(t, err)
		mockWriter.AssertExpectations(t)
	})

	t.Run("JSON marshaling error", func(t *testing.T) {
		t.Parallel()

		mockWriter := new(MockWriter)
		mockLogger := log.NewLogger()

		writerFactory := func() kafko.Writer {
			return mockWriter
		}

		opts := kafko.NewOptionsPublisher().WithWriterFactory(writerFactory)
		publisher := kafko.NewPublisher(mockLogger, opts)

		payload := &CustomTypeWithMarshalError{}

		err := publisher.Publish(ctx, payload)

		assert.Error(t, err)
		assert.ErrorIs(t, err, errMockBadMarshalJSON)
		mockWriter.AssertNotCalled(t, "WriteMessages", mock.Anything, mock.Anything)
	})

	t.Run("error handling with dropped messages", func(t *testing.T) {
		t.Parallel()

		mockWriter := new(MockWriter)
		mockLogger := log.NewLogger()

		writerFactory := func() kafko.Writer {
			return mockWriter
		}

		processDroppedMsgCalled := false
		processDroppedMsg := func(message *kafka.Message, logger kafko.Logger) error {
			processDroppedMsgCalled = true

			return nil
		}

		opts := kafko.NewOptionsPublisher().
			WithWriterFactory(writerFactory).
			WithProcessDroppedMsg(processDroppedMsg)
		publisher := kafko.NewPublisher(mockLogger, opts)

		payload := struct { //nolint:musttag
			Message string
		}{
			Message: "test",
		}

		messageValue, _ := json.Marshal(payload)
		expectedMessages := []kafka.Message{
			{
				Value: messageValue,
			},
		}

		errMockWriteMessages := errors.New("mock WriteMessages error")
		mockWriter.On("WriteMessages", ctx, expectedMessages).Return(errMockWriteMessages)
		mockWriter.On("Close").Return(nil)

		err := publisher.Publish(ctx, payload)

		assert.Error(t, err)
		assert.ErrorIs(t, err, errMockWriteMessages)
		assert.True(t, processDroppedMsgCalled, "processDroppedMsg was not called")
		mockWriter.AssertExpectations(t)
	})

	t.Run("successful shutdown", func(t *testing.T) {
		t.Parallel()

		mockWriter := new(MockWriter)
		mockLogger := log.NewLogger()

		writerFactory := func() kafko.Writer {
			return mockWriter
		}

		opts := kafko.NewOptionsPublisher().WithWriterFactory(writerFactory)
		publisher := kafko.NewPublisher(mockLogger, opts)

		// Mock the Close method to simulate a successful shutdown
		mockWriter.On("Close").Return(nil)

		// Call the Shutdown method
		shutdownCtx, cancel := context.WithTimeout(ctx, 1*time.Second)

		defer cancel()

		err := publisher.Shutdown(shutdownCtx)

		// Assert that the Shutdown method returns no error and the Close method is called
		assert.NoError(t, err)
		mockWriter.AssertCalled(t, "Close")
	})

	t.Run("shutdown after closed", func(t *testing.T) {
		t.Parallel()

		mockWriter := new(MockWriter)
		mockLogger := log.NewLogger()

		writerFactory := func() kafko.Writer {
			return mockWriter
		}

		opts := kafko.NewOptionsPublisher().WithWriterFactory(writerFactory)
		publisher := kafko.NewPublisher(mockLogger, opts)

		// Mock the Close method to simulate a successful shutdown
		mockWriter.On("Close").Return(nil)

		// Call the Shutdown method twice
		shutdownCtx, cancel := context.WithTimeout(ctx, 1*time.Second)

		defer cancel()

		_ = publisher.Shutdown(shutdownCtx)
		err := publisher.Shutdown(shutdownCtx)

		// Assert that the second call to Shutdown returns an error
		assert.Error(t, err)
		assert.ErrorIs(t, err, kafko.ErrAlreadyClosed)
	})
}
