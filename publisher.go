package kafko

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
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
	writer         Writer
	alreadyRewrote int32

	errorHandlingMutex *sync.Mutex
	errorHandlingCond  *sync.Cond
	errorHandling      int32

	writeInProgress *sync.WaitGroup

	alreadyClosed bool          // Add a closed flag to the Publisher struct
	closed        chan struct{} // Mutex to protect the closed flag

	log  Logger
	opts *OptionsPublisher
}

func (publisher *Publisher) writeMessages(ctx context.Context, bytes []byte) error {
	publisher.writeInProgress.Add(1)

	start := time.Now()

	defer func() {
		publisher.writeInProgress.Done()

		duration := time.Since(start)
		publisher.opts.metricDuration.Observe(float64(duration.Milliseconds()))
	}()

	message := kafka.Message{
		Value: bytes,
	}

	if err := publisher.writer.WriteMessages(ctx, message); err != nil {
		publisher.errorHandlingMutex.Lock()
		atomic.StoreInt32(&publisher.errorHandling, 1)

		defer publisher.errorHandlingMutex.Unlock()

		publisher.opts.metricErrors.Inc()

		if err := publisher.opts.processDroppedMsg(&message, publisher.log); err != nil {
			publisher.log.Errorf(err, "err := queue.opts.processDroppedMsg(&message, queue.log)")
		}

		if atomic.CompareAndSwapInt32(&publisher.alreadyRewrote, 0, 1) {
			publisher.writer = publisher.opts.writerFactory()
		}

		// Signal that error handling is complete
		atomic.StoreInt32(&publisher.errorHandling, 0)
		publisher.errorHandlingCond.Broadcast()

		return errors.Wrap(err, "queue.writer.WriteMessages(ctx, messages...)")
	}

	atomic.StoreInt32(&publisher.alreadyRewrote, 0)
	publisher.opts.metricMessages.Inc()

	return nil
}

func (publisher *Publisher) Publish(ctx context.Context, payloads ...interface{}) error {
	var lastError error

	select {
	case <-publisher.closed:
		if publisher.alreadyClosed {
			return errors.Wrapf(ErrAlreadyClosed, "(Publish) publisher.alreadyClosed: %t", publisher.alreadyClosed)
		}

		return nil

	default:
	}

	for _, payload := range payloads {
		if atomic.LoadInt32(&publisher.errorHandling) == 1 {
			publisher.errorHandlingMutex.Lock()
			publisher.errorHandlingCond.Wait()
			publisher.errorHandlingMutex.Unlock()
		}

		bytes, err := json.Marshal(payload)
		if err != nil {
			return errors.Wrap(err, "bytes, err := json.Marshal(payload)")
		}

		if err := publisher.writeMessages(ctx, bytes); err != nil {
			lastError = err
		}
	}

	return lastError
}

// Shutdown method to perform a graceful shutdown.
func (publisher *Publisher) Shutdown(ctx context.Context) error {
	if publisher.alreadyClosed {
		return errors.Wrapf(ErrAlreadyClosed, "(Shutdown) publisher.alreadyClosed: %t", publisher.alreadyClosed)
	}

	publisher.alreadyClosed = true

	close(publisher.closed)

	publisher.writeInProgress.Wait()

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
		return errors.Wrap(ctx.Err(), "(Shutdown) <-ctx.Done()")
	}
}

func NewPublisher(log Logger, opts ...*OptionsPublisher) *Publisher {
	finalOpts := obtainFinalOptionsPublisher(log, opts...)
	errorHandlingMutex := &sync.Mutex{}

	return &Publisher{
		writeInProgress: &sync.WaitGroup{},
		writer:          finalOpts.writerFactory(),
		closed:          make(chan struct{}),
		log:             log,
		opts:            finalOpts,

		errorHandlingMutex: errorHandlingMutex,
		errorHandlingCond:  sync.NewCond(errorHandlingMutex),
	}
}
