package kafko

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

var (
	ErrAlreadyClosed = errors.New("already closed")
)

type Writer interface {
	Close() error
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Publisher struct {
	log  Logger
	opts *OptionsPublisher

	writer              Writer
	writeMessagesLocker sync.Locker

	recreateWriter       bool
	recreateWriterLocker *sync.RWMutex
}

func (publisher *Publisher) closeWriter() error {
	if publisher.writer != nil {
		if err := publisher.writer.Close(); err != nil {
			return errors.Wrapf(err, "cannot close writer")
		}
	}

	return nil
}

func NewPublisher(log Logger, opts ...*OptionsPublisher) *Publisher {
	finalOpts := obtainFinalOptionsPublisher(log, opts...)

	return &Publisher{
		log:  log,
		opts: finalOpts,

		writeMessagesLocker: &sync.Mutex{},
		writer:              finalOpts.writerFactory(),

		recreateWriterLocker: &sync.RWMutex{},
	}
}
