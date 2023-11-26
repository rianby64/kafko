package kafko

import (
	"context"
	errorsStd "errors"

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
			if errDroppedMsg := publisher.opts.droppedMsg.Handle(ctx, &msg); errDroppedMsg != nil {
				return errors.Wrap(errorsStd.Join(err, errDroppedMsg), "cannot handle message")
			}

			if errBackoff := backoff.Wait(ctx); errBackoff != nil {
				return errorsStd.Join(err, errBackoff)
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
