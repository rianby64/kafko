package kafko

import (
	"context"
	"encoding/json"
	"sync"
	"time"

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
	writer      Writer
	writerMutex sync.Locker

	alreadyClosed bool          // Add a closed flag to the Publisher struct
	closed        chan struct{} // Mutex to protect the closed flag

	log  Logger
	opts *OptionsPublisher
}

func (publisher *Publisher) writeSingleMessage(ctx context.Context, bytes []byte) error {
	publisher.writerMutex.Lock()

	defer publisher.writerMutex.Unlock()

	start := time.Now()

	defer func() {
		duration := time.Since(start)
		publisher.opts.metricDuration.Observe(float64(duration.Milliseconds()))
	}()

	message := kafka.Message{
		Value: bytes,
	}

	if err := publisher.writer.WriteMessages(ctx, message); err != nil {
		publisher.opts.metricErrors.Inc()

		if err := publisher.opts.processDroppedMsg(&message, publisher.log); err != nil {
			publisher.log.Errorf(err, "err := queue.opts.processDroppedMsg(&message, queue.log)")
		}

		publisher.writer = publisher.opts.writerFactory()

		return errors.Wrap(err, "queue.writer.WriteMessages(ctx, messages...)")
	}

	publisher.opts.metricMessages.Inc()

	return nil
}

func (publisher *Publisher) Publish(ctx context.Context, payloads ...interface{}) error {
	var lastError error

	select {
	case <-publisher.closed:
		if publisher.alreadyClosed {
			return errors.Wrap(ErrAlreadyClosed, "cannot Publish")
		}

		return nil

	default:
	}

	for _, payload := range payloads {
		bytes, err := json.Marshal(payload)
		if err != nil {
			return errors.Wrap(err, "bytes, err := json.Marshal(payload)")
		}

		if err := publisher.writeSingleMessage(ctx, bytes); err != nil {
			lastError = err
		}
	}

	return lastError
}

// Shutdown method to perform a graceful shutdown.
func (publisher *Publisher) Shutdown(ctx context.Context) error {
	if publisher.alreadyClosed {
		return errors.Wrap(ErrAlreadyClosed, "cannot Shutdown")
	}

	publisher.alreadyClosed = true

	close(publisher.closed)

	publisher.writerMutex.Lock()

	defer publisher.writerMutex.Unlock()

	// Use the provided context to allow for cancelation or timeout
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	errChan := make(chan error, 1)

	go func() {
		errChan <- publisher.writer.Close()
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "Publisher shutdown interrupted")
	}
}

func NewPublisher(log Logger, opts ...*OptionsPublisher) *Publisher {
	finalOpts := obtainFinalOptionsPublisher(log, opts...)

	return &Publisher{
		writerMutex: &sync.Mutex{},
		writer:      finalOpts.writerFactory(),
		closed:      make(chan struct{}),
		log:         log,
		opts:        finalOpts,
	}
}
