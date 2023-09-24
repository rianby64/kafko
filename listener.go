package kafko

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Reader interface {
	Close() error
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Listener struct {
	log  Logger
	opts *OptionsListener
}

// NewListener creates a new Listener instance with the provided configuration,
// logger, and optional custom options.
func NewListener(log Logger, opts ...*OptionsListener) *Listener {
	finalOpts := obtainFinalOptsListener(log, opts)

	// Create and return a new Listener instance with the final configuration,
	// channels, and options.
	return &Listener{
		log:  log,
		opts: finalOpts,
	}
}
