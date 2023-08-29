package kafko

import (
	"context"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

// ProcessDroppedMsgHandler is supposed to handle those messages that couldn't make their path to Kafka.
// Try to publish to another resource the mssage.
//
// `lastError` is the error that occurred in first place.
//
// `log` is the logger you should use.
type ProcessDroppedMsgHandler func(msg *kafka.Message, lastError error, log Logger)

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
	ErrResourceIsNil  = errors.New("resource is nil")
)

type Listener struct {
	messageChan chan []byte
	errorChan   chan error

	log  Logger
	opts *OptionsListener
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

	// Create and return a new Listener instance with the final configuration,
	// channels, and options.
	return &Listener{
		messageChan: messageChan,
		errorChan:   errorChan,
		log:         log,
		opts:        finalOpts,
	}
}
