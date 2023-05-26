package kafko

import (
	"context"

	"github.com/pkg/errors"
)

// Listen starts the Listener to fetch and process messages from the Kafka topic.
// It also starts the commit loop and handles message errors.
func (listener *Listener) Listen(ctxIn context.Context) error { //nolint:cyclop
	ctx, cancel := context.WithCancel(ctxIn)

	go func() {
		<-listener.shuttingDownCh
		cancel()
	}()

	// Start the commit loop in a separate goroutine.
	go listener.runCommitLoop(ctx)

	// Continuously fetch and process messages.
	for {
		select {
		case _, isOpen := <-listener.messageChan:
			closed := !isOpen
			if closed {
				// If the listener.messageChan has been closed, exit the loop.
				return nil
			}

		case <-ctx.Done():
			// If the context is done, check for an error and return it.
			if err := ctx.Err(); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}

				return errors.Wrap(err, "err := ctx.Err() (ctx.Done()) (Listen)")
			}

		case <-listener.shuttingDownCh:
			// If the shutdown has started, exit the loop.
			return nil

		default:
		}

		err := listener.processTick(ctx)

		if errors.Is(err, errExitProcessingLoop) {
			return nil
		}

		if errors.Is(err, context.Canceled) {
			return nil
		}

		if err != nil {
			return errors.Wrap(err, "err := listener.processTick(ctx)")
		}
	}
}

// Shutdown gracefully shuts down the Listener, committing any uncommitted messages
// and closing the Kafka reader.
func (listener *Listener) Shutdown(ctx context.Context) error {
	// let's start the shutting down process
	close(listener.shuttingDownCh)

	defer func() {
		close(listener.errorChan)
		close(listener.messageChan)
	}()

	listener.processing.Lock()

	defer listener.processing.Unlock()

	// Commit any uncommitted messages. It's OK to not to process them further as
	// logs will provide the missing content while trying to commit before shutting down.
	if err := listener.commitUncommittedMessages(ctx); err != nil {
		listener.log.Errorf(err, "err := queue.commitUncommittedMessages(ctx)")
	}

	// Close the Kafka reader.
	if err := listener.reader.Close(); err != nil {
		go listener.opts.metricErrors.Inc()

		return errors.Wrap(err, "queue.reader.Close()")
	}

	return nil
}

// MessageAndErrorChannels returns the message and error channels for the Listener.
func (listener *Listener) MessageAndErrorChannels() (<-chan []byte, chan<- error) {
	return listener.messageChan, listener.errorChan
}
