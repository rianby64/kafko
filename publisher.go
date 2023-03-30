package kafko

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

type Writer interface {
	Close() error
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Publisher struct {
	writer        Writer
	writerBuilder func() Writer
	log           Logger
}

func (queue *Publisher) Publish(ctx context.Context, payloads ...interface{}) error {
	queue.writer = queue.writerBuilder()
	messages := make([]kafka.Message, len(payloads))

	for index, payload := range payloads {
		bytes, err := json.Marshal(payload)
		if err != nil {
			return errors.Wrap(err, "bytes, err := json.Marshal(payload)")
		}

		messages[index] = kafka.Message{
			Value: bytes,
		}
	}

	defer func() {
		if err := queue.writer.Close(); err != nil {
			queue.log.Errorf(err, "err := queue.writer.Close()")
		}
	}()

	if err := queue.writer.WriteMessages(ctx, messages...); err != nil {
		return errors.Wrap(err, "queue.writer.WriteMessages(ctx, messages...)")
	}

	return nil
}

func NewPublisher(writerBuilder func() Writer, log Logger) *Publisher {
	return &Publisher{
		writerBuilder: writerBuilder,
		log:           log,
	}
}
