package kafko

import (
	"context"
	errorsStd "errors"
	"time"

	"github.com/pkg/errors"
)

func (listener *Listener) shouldContinueListen(ctx context.Context) bool {
	select {
	case <-listener.shutdownChan:
		return false
	case <-ctx.Done():
		return false
	default:
		return true
	}
}

func (listener *Listener) shouldExitListen(ctx context.Context) bool {
	return !listener.shouldContinueListen(ctx)
}

// Listen starts the Listener to fetch and process messages from the Kafka topic.
// It also starts the commit loop and handles message errors.
func (listener *Listener) Listen(ctx context.Context) error {
	ctxFinal, cancel := context.WithCancel(ctx)
	listener.cancel = cancel
	listener.reader = listener.opts.readerFactory()

	for listener.shouldContinueListen(ctxFinal) {
		now := listener.opts.time.Now()

		msg, err := listener.reader.FetchMessage(ctxFinal)
		if err != nil {
			listener.opts.metricErrors.Inc()

			return errors.Wrap(errorsStd.Join(err, listener.closeReader(ctx)), "cannot fetch message")
		}

		if listener.shouldExitListen(ctxFinal) {
			break
		}

		if err := listener.opts.processMsg.Handle(ctxFinal, &msg); err != nil {
			listener.opts.metricMessagesError.Inc()

			return errors.Wrap(errorsStd.Join(err, listener.closeReader(ctx)), "cannot handle message")
		}

		if err := listener.reader.CommitMessages(ctxFinal, msg); err != nil {
			listener.opts.metricErrors.Inc()

			return errors.Wrap(errorsStd.Join(err, listener.closeReader(ctx)), "cannot commit message")
		}

		listener.opts.metricMessagesProcessed.Inc()
		listener.opts.metricDurationProcess.Observe(time.Since(now).Seconds())
	}

	if _, ok := <-listener.shutdownChan; !ok {
		return nil
	}

	return errors.Wrap(ctxFinal.Err(), "listen stopped")
}

// Shutdown gracefully shuts down the Listener, committing any uncommitted messages
// and closing the Kafka reader.
func (listener *Listener) Shutdown(ctx context.Context) error {
	close(listener.shutdownChan)

	defer listener.cancel()

	return listener.closeReader(ctx)
}

func (listener *Listener) closeReader(ctx context.Context) error {
	errChan := make(chan error, 1)
	go func() {
		err := listener.reader.Close()
		if err != nil {
			listener.opts.metricErrors.Inc()
		}

		errChan <- err
	}()

	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "context done")
	case err := <-errChan:
		return errors.Wrap(err, "cannot close reader")
	}
}
