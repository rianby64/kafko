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
	ErrMessageDropped = errors.New("message dropped")
	ErrNoCredentials  = errors.New("no credentials")
)

type Listener struct {
	lastMessage chan []byte
	errorChan   chan error

	log Logger

	commitTicker      *time.Ticker
	processingTimeout time.Duration
	reconnectInterval time.Duration
	processDroppedMsg ProcessDroppedMsgHandler

	readerFactory ReaderFactory
	reader        Reader

	uncommittedMsgs      []kafka.Message
	uncommittedMsgsMutex *sync.Mutex

	shuttingDown      bool
	shuttingDownMutex *sync.Mutex
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

	case <-time.After(listener.processingTimeout):
		// If processing times out, attempt to process the dropped message.
		if err := listener.processDroppedMsg(&message, listener.log); err != nil {
			listener.log.Errorf(err, "Failed to process message")
		}

	case <-ctx.Done():
		// If the context is done, check for an error and return it.
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "err := ctx.Err() (ctx.Done()) (processMessage)")
		}
	}

	return nil
}

// processMessageAndError processes the given message and handles any errors
// that occur during processing, following a similar approach to processError.
func (listener *Listener) processMessageAndError(ctx context.Context, message kafka.Message) error {
	select {
	case listener.lastMessage <- message.Value:
		// Process the message and handle any errors.
		if err := listener.processError(ctx, message); err != nil {
			return errors.Wrap(err, "err := listener.processError(ctx, message)")
		}

	case <-time.After(listener.processingTimeout):
		// Attempt to empty the listener.lastMsg channel if there is a message.
		select {
		case <-listener.lastMessage:
		default:
		}

		// If processing times out, attempt to process the dropped message.
		if err := listener.processDroppedMsg(&message, listener.log); err != nil {
			listener.log.Errorf(err, "Failed to process message")
		}

	case <-ctx.Done():
		// If the context is done, check for an error and return it.
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "err := ctx.Err() (ctx.Done()) (processMessage)")
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
		if err := listener.handleCommitMessageError(err); err != nil {
			return errors.Wrap(err, "err := queue.reader.CommitMessages(ctx, queue.uncommittedMsgs)")
		}
	}

	return nil
}

// handleCommitMessageError checks if the error is temporary or a timeout,
// and logs the appropriate message. If the error is not recoverable, it wraps
// and returns the error.
func (listener *Listener) handleCommitMessageError(err error) error {
	// Check if the error is a Kafka error.
	var kafkaError *kafka.Error

	// If the error is temporary or a timeout, log a message and return nil.
	if errors.As(err, &kafkaError) && (kafkaError.Temporary() || kafkaError.Timeout()) {
		listener.log.Printf("Failed to commit message, but this is a recoverable error so let's retry. Reason = %v", err)

		return nil
	}

	// If the error is not recoverable, wrap and return it.
	return errors.Wrapf(err, "Failed to commit message, unrecoverable error")
}

// handleMessageError checks if the error is temporary or a timeout and
// takes appropriate action based on the error type. If the error is recoverable,
// it attempts to reconnect to Kafka. If the error is not recoverable, finish Kafka listening.
func (listener *Listener) handleMessageError(ctx context.Context, err error) error {
	// Check if the error is a Kafka error.
	var kafkaError *kafka.Error

	// If the error is temporary or a timeout, log a message, try to reconnect and return nil.
	if errors.As(err, &kafkaError) && (kafkaError.Temporary() || kafkaError.Timeout()) {
		listener.log.Errorf(err, "Failed to read message, let's retry")

		select {
		// Let's reconnect after queue.reconnectInterval.
		case <-time.After(listener.reconnectInterval):
			listener.reconnectToKafka()

		// If ctx.Done and reconnect hasn't started yet, then it's secure to exit.
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				return errors.Wrap(err, "err := ctx.Err() (ctx.Done()) (handleMessageError)")
			}
		}

		return nil
	}

	// If the error is not recoverable, finish Kafka.
	return errors.Wrap(err, "Failed to read message, the error is not recoverable")
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
			return errors.Wrapf(err, "err := queue.reader.CommitMessages(ctx, queue.uncommittedMsgs...) (queue.uncommittedMsgs = %v)", listener.uncommittedMsgs)
		}

		// Reset the uncommitted messages slice.
		listener.uncommittedMsgs = nil
	}

	return nil
}

// runCommitLoop is responsible for periodically committing uncommitted messages.
// It runs in a separate goroutine and stops when the provided context is done.
func (listener *Listener) runCommitLoop(ctx context.Context) {
	// Add the defer function to handle stopping the ticker and committing uncommitted messages
	// in case the method returns due to a panic or other unexpected situations.
	defer func() {
		listener.commitTicker.Stop()

		if err := listener.commitUncommittedMessages(ctx); err != nil {
			listener.log.Errorf(err, "err := queue.commitUncommittedMessages(ctx)")
		}
	}()

	// Loop until the context is done.
	for {
		select {
		case <-listener.commitTicker.C:
			// When the ticker ticks, commit uncommitted messages.
			if err := listener.commitUncommittedMessages(ctx); err != nil {
				listener.log.Errorf(err, "err := queue.commitUncommittedMessages(ctx)")
			}

		case <-ctx.Done():
			// If the context is done, exit the loop.
			return
		}
	}
}

// reconnectToKafka attempts to reconnect the Listener to the Kafka broker.
// It returns an error if the connection fails.
func (listener *Listener) reconnectToKafka() {
	// Create a new Reader from the readerFactory.
	reader := listener.readerFactory()
	listener.reader = reader
}

// defaultProcessDroppedMsg logs a dropped message and returns a predefined error.
func defaultProcessDroppedMsg(msg *kafka.Message, log Logger) error {
	// Log the dropped message with its content.
	log.Errorf(ErrMessageDropped, "msg = %s", string(msg.Value))

	// Return a predefined error for dropped messages.
	return ErrMessageDropped
}

// MessageAndErrorChannels returns the message and error channels for the Listener.
func (listener *Listener) MessageAndErrorChannels() (<-chan []byte, chan<- error) {
	return listener.lastMessage, listener.errorChan
}

// Shutdown gracefully shuts down the Listener, committing any uncommitted messages
// and closing the Kafka reader.
func (listener *Listener) Shutdown(ctx context.Context) error {
	listener.shuttingDownMutex.Lock()
	defer listener.shuttingDownMutex.Unlock()

	listener.shuttingDown = true

	// Commit any uncommitted messages.
	if err := listener.commitUncommittedMessages(ctx); err != nil {
		listener.log.Errorf(err, "err := queue.commitUncommittedMessages(ctx)")
	}

	// Close the Kafka reader.
	if err := listener.reader.Close(); err != nil {
		return errors.Wrap(err, "queue.reader.Close()")
	}

	return nil
}

func (listener *Listener) checkShutdown() bool {
	listener.shuttingDownMutex.Lock()
	defer listener.shuttingDownMutex.Unlock()

	return listener.shuttingDown
}

// Listen starts the Listener to fetch and process messages from the Kafka topic.
// It also starts the commit loop and handles message errors.
func (listener *Listener) Listen(ctx context.Context) error {
	// Start the commit loop in a separate goroutine.
	go listener.runCommitLoop(ctx)

	// Continuously fetch and process messages.
	for {
		// Fetch a message from the Kafka topic.
		message, err := listener.reader.FetchMessage(ctx)

		if listener.checkShutdown() {
			return nil
		}

		// If there's an error, handle the message error and continue to the next iteration.
		if err != nil {
			if err := listener.handleMessageError(ctx, err); err != nil {
				return errors.Wrap(err, "err := listener.handleMessageError(ctx, err)")
			}

			continue
		}

		// Process the message and handle any errors.
		if err := listener.processMessageAndError(ctx, message); err != nil {
			return errors.Wrap(err, "err := listener.processMessage(ctx, message)")
		}
	}
}

func obtainFinalOpts(log Logger, opts []*Options) *Options {
	// Set the default options.
	finalOpts := &Options{
		commitInterval:    commitInterval,
		processDroppedMsg: defaultProcessDroppedMsg,
		processingTimeout: processingTimeout,
		reconnectInterval: reconnectInterval,
		readerFactory: func() Reader {
			log.Panicf(ErrNoCredentials, "Provide the reader via options.WithReaderFactory")

			return nil
		},
	}

	// Iterate through the provided custom options and override defaults if needed.
	for _, opt := range opts {
		if opt.processingTimeout != 0 {
			finalOpts.processingTimeout = opt.processingTimeout
		}

		if opt.reconnectInterval != 0 {
			finalOpts.reconnectInterval = opt.reconnectInterval
		}

		if opt.commitInterval != 0 {
			finalOpts.commitInterval = opt.commitInterval
		}

		if opt.processDroppedMsg != nil {
			finalOpts.processDroppedMsg = opt.processDroppedMsg
		}

		if opt.readerFactory != nil {
			finalOpts.readerFactory = opt.readerFactory
		}
	}

	return finalOpts
}

// NewListener creates a new Listener instance with the provided configuration,
// logger, and optional custom options.
func NewListener(log Logger, opts ...*Options) *Listener {
	finalOpts := obtainFinalOpts(log, opts)

	// lastMessage should have a buffer size of 1 to accommodate for the case when
	// the consumer did not process the message within the `processingTimeout` period.
	// In the Listen method, we attempt to empty the listener.lastMessage channel (only once)
	// if the processingTimeout is reached. By setting the buffer size to 1, we ensure
	// that the new message can be placed in the channel even if the previous message
	// wasn't processed within the given timeout.
	lastMessage := make(chan []byte, 1)

	// Create and return a new Listener instance with the final configuration,
	// channels, and options.
	return &Listener{
		lastMessage: lastMessage,
		errorChan:   make(chan error, 1),
		log:         log,

		readerFactory: finalOpts.readerFactory,
		reader:        finalOpts.readerFactory(),

		commitTicker:      time.NewTicker(finalOpts.commitInterval),
		reconnectInterval: finalOpts.reconnectInterval,
		processingTimeout: finalOpts.processingTimeout,
		processDroppedMsg: finalOpts.processDroppedMsg,

		uncommittedMsgsMutex: &sync.Mutex{},
		shuttingDownMutex:    &sync.Mutex{},
	}
}
