package kafko

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

type ProcessDroppedMsgHandler func(msg *kafka.Message, log Logger) error

type Logger interface {
	Printf(format string, v ...any)
	Panicf(err error, format string, v ...any)
	Errorf(err error, format string, v ...any)
}

type Reader interface {
	Close() error
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

var (
	ErrMessageDropped     = errors.New("message dropped")
	ErrResourceIsNil      = errors.New("resource is nil")
	errExitProcessingLoop = errors.New("listener: exit processing loop")
)

type Listener struct {
	messageChan    chan []byte
	errorChan      chan error
	shuttingDownCh chan struct{}

	log Logger

	opts *OptionsListener

	processing sync.Locker

	reader Reader

	uncommittedMsgs      []kafka.Message
	uncommittedMsgsMutex sync.Locker
}

// processError handles errors in processing messages.
func (listener *Listener) processError(ctx context.Context, message kafka.Message) error {
	select {
	case err := <-listener.errorChan:
		// If there's an error, log it and continue processing.
		if err != nil {
			listener.log.Errorf(err, "Failed to process message =%v", message)

			return nil
		}

		// If there's no error, commit the message.
		if err := listener.doCommitMessage(ctx, message); err != nil {
			return errors.Wrap(err, "err := queue.doCommitMessage(ctx, message)")
		}

	case <-time.After(listener.opts.processingTimeout):
		// If processing times out, attempt to process the dropped message.
		if err := listener.opts.processDroppedMsg(&message, listener.log); err != nil {
			listener.log.Errorf(err, "Failed to process message")
		}
	}

	return nil
}

// processMessageAndError processes the given message and handles any errors
// that occur during processing, following a similar approach to processError.
func (listener *Listener) processMessageAndError(ctx context.Context, message kafka.Message) error {
	start := time.Now()

	select {
	case listener.messageChan <- message.Value:
		// Process the message and handle any errors.
		if err := listener.processError(ctx, message); err != nil {
			return errors.Wrap(err, "err := listener.processError(ctx, message)")
		}

		duration := time.Since(start)
		listener.opts.metricDurationProcess.Observe(float64(duration.Milliseconds()))

	case <-time.After(listener.opts.processingTimeout):
		// Attempt to empty the listener.lastMsg channel if there is a message.
		select {
		case _, closed := <-listener.messageChan:
			if closed {
				// If the listener.messageChan has been closed, exit the loop.
				return nil
			}
		default:
		}

		go listener.opts.metricMessagesDropped.Inc()

		// If processing times out, attempt to process the dropped message.
		if err := listener.opts.processDroppedMsg(&message, listener.log); err != nil {
			listener.log.Errorf(err, "Failed to process message")
		}
	}

	return nil
}

// addUncommittedMsg appends the given message to the list of uncommitted messages.
// It locks the uncommittedMsgsMutex to ensure safe concurrent access to the uncommittedMsgs slice.
func (listener *Listener) addUncommittedMsg(message kafka.Message) {
	// Lock the mutex before accessing uncommittedMsgs.
	listener.uncommittedMsgsMutex.Lock()

	// Unlock the mutex after finishing.
	defer listener.uncommittedMsgsMutex.Unlock()

	// Add the message to the uncommittedMsgs slice.
	listener.uncommittedMsgs = append(listener.uncommittedMsgs, message)
}

// doCommitMessage adds the given message to the list of uncommitted messages
// and commits all uncommitted messages.
func (listener *Listener) doCommitMessage(ctx context.Context, message kafka.Message) error {
	// Add the message to the list of uncommitted messages.
	listener.addUncommittedMsg(message)

	// Attempt to commit all uncommitted messages.
	if err := listener.commitUncommittedMessages(ctx); err != nil {
		// If there's an error, handle it and return the wrapped error.
		if err := listener.handleKafkaError(ctx, err); err != nil {
			return errors.Wrap(err, "err := queue.reader.CommitMessages(ctx, queue.uncommittedMsgs)")
		}
	}

	return nil
}

// handleKafkaError checks if the error is temporary or a timeout and
// takes appropriate action based on the error type. If the error is recoverable,
// it attempts to reconnect to Kafka. If the error is not recoverable, it wraps and returns the error.
func (listener *Listener) handleKafkaError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	var kafkaError *kafka.Error

	if errors.As(err, &kafkaError) {
		if kafkaError.Temporary() || kafkaError.Timeout() {
			listener.log.Printf("Kafka error, but this is a recoverable error so let's retry. Reason = %v", err)

			select {
			// Let's reconnect after queue.reconnectInterval.
			case <-time.After(listener.opts.reconnectInterval):
				listener.reconnectToKafka()

			// If ctx.Done and reconnect hasn't started yet, then it's secure to exit.
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					return errors.Wrap(err, "err := ctx.Err() (ctx.Done()) (handleKafkaError)")
				}

			// If the shutdown has started, exit the loop.
			case <-listener.shuttingDownCh:
				return errExitProcessingLoop
			}

			// Return no error since it's a recoverable error
			return nil
		}
	}

	// If the error is not recoverable, wrap and return it.
	return errors.Wrapf(err, "Failed to commit message, unrecoverable error")
}

// commitUncommittedMessages commits all uncommitted messages to Kafka.
// It locks the uncommittedMsgsMutex to avoid concurrent access to uncommittedMsgs.
func (listener *Listener) commitUncommittedMessages(ctx context.Context) error {
	// Lock the mutex to avoid concurrent access to uncommitted messages.
	listener.uncommittedMsgsMutex.Lock()
	defer listener.uncommittedMsgsMutex.Unlock()

	// If there are uncommitted messages, attempt to commit them.
	if len(listener.uncommittedMsgs) > 0 {
		if err := listener.reader.CommitMessages(ctx, listener.uncommittedMsgs...); err != nil {
			go listener.opts.metricErrors.Inc()

			return errors.Wrapf(err, "err := queue.reader.CommitMessages(ctx, queue.uncommittedMsgs...) (queue.uncommittedMsgs = %v)", listener.uncommittedMsgs)
		}

		go listener.opts.metricMessagesProcessed.Inc()

		// Reset the uncommitted messages slice.
		listener.uncommittedMsgs = nil
	}

	return nil
}

// runCommitLoop is a method of the Listener struct that handles periodic committing of uncommitted messages.
// It is designed to be run in a separate goroutine and will continue until the provided context is cancelled or completed.
//
// The method uses a ticker to trigger periodic commits and makes use of a defer function to ensure proper cleanup
// in case of a panic or other unexpected situations. The defer function stops the ticker and attempts to commit any
// remaining uncommitted messages.
//
// This method is part of a message processing system and is typically used in conjunction with other methods that handle
// message reception and processing.
func (listener *Listener) runCommitLoop(ctx context.Context) {
	// Add the defer function to handle stopping the ticker and committing uncommitted messages
	// in case the method returns due to a panic or other unexpected situations.
	defer func() {
		listener.opts.recommitTicker.Stop()

		if err := listener.commitUncommittedMessages(ctx); err != nil {
			listener.log.Errorf(err, "err := queue.commitUncommittedMessages(ctx)")
		}
	}()

	for {
		select {
		case <-listener.opts.recommitTicker.C:
			// When the ticker ticks, commit uncommitted messages.
			if err := listener.commitUncommittedMessages(ctx); err != nil {
				listener.log.Errorf(err, "err := queue.commitUncommittedMessages(ctx)")
			}

		case <-listener.shuttingDownCh:
			// If the shutdown has started, exit the loop.
			return

		case <-ctx.Done():
			// If the context is done, exit the loop.
			return
		}
	}
}

// reconnectToKafka attempts to reconnect the Listener to the Kafka broker.
// It returns an error if the connection fails.
func (listener *Listener) reconnectToKafka() {
	// Close the existing reader in order to avoid resource leaks
	if err := listener.reader.Close(); err != nil {
		go listener.opts.metricErrors.Inc()

		listener.log.Errorf(err, "err := listener.reader.Close()")
	}

	// Create a new Reader from the readerFactory.
	reader := listener.opts.readerFactory()
	listener.reader = reader
}

func (listener *Listener) processTick(ctx context.Context) error {
	listener.processing.Lock()

	defer listener.processing.Unlock()

	select {
	case <-listener.shuttingDownCh:
		return errExitProcessingLoop
	default:
	}

	message, err := listener.reader.FetchMessage(ctx)

	// If there's an error, handle the message error and continue to the next iteration.
	if err != nil {
		go listener.opts.metricErrors.Inc()

		if err := listener.handleKafkaError(ctx, err); err != nil {
			return errors.Wrap(err, "err := listener.handleKafkaError(ctx, err)")
		}

		return nil
	}

	// Process the message and handle any errors.
	if err := listener.processMessageAndError(ctx, message); err != nil {
		return errors.Wrap(err, "err := listener.processMessage(ctx, message)")
	}

	return nil
}

// NewListener creates a new Listener instance with the provided configuration,
// logger, and optional custom options.
func NewListener(log Logger, opts ...*OptionsListener) *Listener {
	finalOpts := obtainFinalOptsListener(log, opts)

	// messageChan should have a buffer size of 1 to accommodate for the case when
	// the consumer did not process the message within the `processingTimeout` period.
	// In the Listen method, we attempt to empty the listener.messageChan channel (only once)
	// if the processingTimeout is reached. By setting the buffer size to 1, we ensure
	// that the new message can be placed in the channel even if the previous message
	// wasn't processed within the given timeout.
	messageChan := make(chan []byte, 1)

	// errorChan has a buffer size of 1 to allow the sender to send an error without blocking
	// if the receiver is not ready to receive it yet.
	errorChan := make(chan error, 1)

	shuttingDownCh := make(chan struct{}, 1)

	// Create and return a new Listener instance with the final configuration,
	// channels, and options.
	return &Listener{
		messageChan:    messageChan,
		errorChan:      errorChan,
		shuttingDownCh: shuttingDownCh,

		processing:           &sync.Mutex{},
		uncommittedMsgsMutex: &sync.Mutex{},
		uncommittedMsgs:      make([]kafka.Message, 0),

		log:  log,
		opts: finalOpts,

		reader: finalOpts.readerFactory(),
	}
}
