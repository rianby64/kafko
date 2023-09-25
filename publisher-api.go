package kafko

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

// Publish accepts only one payload, is concurrent-safe. Call it from different places at the same time.
func (publisher *Publisher) Publish(ctx context.Context, payload []byte) error {
	if publisher.checkShutdownFlag() {
		return errors.Wrap(ErrResourceUnavailable, "shutdown performed")
	}

	now := publisher.opts.time.Now()

	defer func() {
		publisher.opts.metricDuration.Observe(time.Since(now).Seconds())
	}()

	key := publisher.opts.keyGenerator.Generate()
	msg := kafka.Message{
		Key:   key,
		Value: payload,
	}

	if lastError := publisher.lastError(); lastError != nil {
		publisher.opts.metricErrors.Inc()

		if err := publisher.opts.processDroppedMsg.Handle(ctx, &msg); err != nil {
			return errors.Wrap(err, "cannot process dropped message")
		}

		return nil
	}

	return publisher.loopForever(ctx, &msg)
}

// Shutdown method to perform a graceful shutdown.
func (publisher *Publisher) Shutdown(ctx context.Context) error {
	// This is a solution that may be blocked by a long write. Avoid it.
	publisher.shutdownLock.Lock()

	defer publisher.shutdownLock.Unlock()

	publisher.shutdownFlag = true

	errChan := make(chan error, 1)

	go func() {
		errChan <- publisher.closeWriter()
	}()

	select {
	case err := <-errChan:
		return errors.Wrapf(err, "cannot close kafka connection")
	case <-ctx.Done():
		return errors.Wrapf(ctx.Err(), "shutdown canceled")
	}
}
