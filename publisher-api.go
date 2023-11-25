package kafko

import (
	"context"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

// Publish accepts only one payload, is concurrent-safe. Call it from different places at the same time.
func (publisher *Publisher) Publish(ctx context.Context, payload []byte) error {
	backoff := publisher.opts.backoffStrategyFactory()
	msg := kafka.Message{
		Value: payload,
	}

	for {
		if err := publisher.writer.WriteMessages(ctx, msg); err != nil {
			if err := publisher.opts.droppedMsg.Handle(ctx, &msg); err != nil {
				return errors.Wrap(err, "cannot handle dropped msg")
			}

			if errors.Is(backoff.Wait(ctx), ErrRetryReached) {
				return err
			}

			continue
		}

		return nil
	}
}

// Shutdown method to perform a graceful shutdown.
func (publisher *Publisher) Shutdown(_ context.Context) error {
	if err := publisher.closeWriter(); err != nil {
		return errors.Wrap(err, "cannot close the writer")
	}

	return nil
}
