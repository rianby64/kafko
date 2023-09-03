package kafko

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

// Publish accepts only one payload, is concurrent-safe. Call it from different places at the same time.
func (publisher *Publisher) Publish(ctx context.Context, payload []byte) error {
	publisher.shutdownLock.RLock()

	defer publisher.shutdownLock.RUnlock()

	if publisher.shutdownFlag {
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

		if err := publisher.opts.processDroppedMsg(&msg, publisher.log); err != nil {
			return errors.Wrap(err, "cannot process dropped message")
		}

		return nil
	}

	return publisher.loopForever(ctx, &msg)
}

func (publisher *Publisher) loopForever(ctx context.Context, msg *kafka.Message) error {
	shouldClearStateBeforeReturning := true

	// I need this strange defer because I need to guarantee panic won't block other attempts
	defer func() {
		if !shouldClearStateBeforeReturning {
			return
		}

		publisher.clearStateError()
	}()

	for {
		if err := publisher.writer.WriteMessages(ctx, *msg); err != nil {
			publisher.opts.metricErrors.Inc()

			if shoulExitFromLoop, err := publisher.processError(err, msg); shoulExitFromLoop {
				shouldClearStateBeforeReturning = false

				return err
			}

			publisher.opts.backoffStrategy.Wait()

			continue
		}

		publisher.opts.metricMessages.Inc()

		return nil
	}
}

// Shutdown method to perform a graceful shutdown.
func (publisher *Publisher) Shutdown(ctx context.Context) error {
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
