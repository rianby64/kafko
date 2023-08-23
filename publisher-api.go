package kafko

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

func (publisher *Publisher) Publish(ctx context.Context, payload []byte) error {
	now := time.Now()
	writer := publisher.writer

	if err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(uuid.New().String()),
		Value: payload,
	}); err != nil {
		return errors.Wrapf(err, "cannot write message to Kafka")
	}

	defer func() {
		fmt.Print("\033[G\033[K", time.Since(now), "\n", "\033[A")
	}()

	return nil
}

// Shutdown method to perform a graceful shutdown.
func (publisher *Publisher) Shutdown(_ context.Context) error {
	return publisher.closeWriter()
}
