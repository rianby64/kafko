package kafko

import (
	"context"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

var (
	ErrAlreadyClosed = errors.New("already closed")
)

//go:generate mockgen -destination=./mocks/mock_writer.go -package=mocks kafko Writer
type Writer interface {
	Close() error
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Publisher struct {
	log  Logger
	opts *OptionsPublisher

	writer Writer
}

func (publisher *Publisher) closeWriter() error {
	if publisher.writer != nil {
		if err := publisher.writer.Close(); err != nil {
			return err
		}
	}

	return nil
}

func NewPublisher(log Logger, opts ...*OptionsPublisher) *Publisher {
	finalOpts := obtainFinalOptionsPublisher(log, opts...)

	return &Publisher{
		log:    log,
		opts:   finalOpts,
		writer: finalOpts.writerFactory(),
	}
}
