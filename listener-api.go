package kafko

import (
	"context"

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
	for listener.shouldContinueListen(ctx) {
		msg, err := listener.reader.FetchMessage(ctx)
		if err != nil {
			return errors.Wrap(err, "cannot fetch message")
		}

		if listener.shouldExitListen(ctx) { // this looks rather a hack
			break
		}

		if err := listener.opts.processMsg.Handle(ctx, &msg); err != nil {
			return errors.Wrap(err, "cannot handle message")
		}

		if listener.shouldExitListen(ctx) { // this looks rather a hack
			break
		}

		if err := listener.reader.CommitMessages(ctx, msg); err != nil {
			return errors.Wrap(err, "cannot commit message")
		}

		if listener.shouldExitListen(ctx) { // this looks rather a hack
			break
		}
	}

	return errors.Wrap(ctx.Err(), "listen stopped")
}

// Shutdown gracefully shuts down the Listener, committing any uncommitted messages
// and closing the Kafka reader.
func (listener *Listener) Shutdown(ctx context.Context) error {
	errChan := make(chan error, 1)

	close(listener.shutdownChan)

	go func() {
		if err := listener.reader.Close(); err != nil {
			errChan <- errors.Wrap(err, "cannot close reader")

			return
		}

		errChan <- nil
	}()

	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "shutdown")
	case err := <-errChan:
		return errors.Wrap(err, "shutdown")
	}
}
