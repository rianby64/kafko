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

	writer           Writer
	processErrorLock sync.Locker

	stateError     error
	stateErrorLock *sync.RWMutex

	shutdownFlag bool
	shutdownLock *sync.RWMutex
}

func (publisher *Publisher) closeWriter() error {
	if publisher.writer != nil {
		if err := publisher.writer.Close(); err != nil {
			return errors.Wrapf(err, "cannot close writer")
		}
	}

	return nil
}

func (publisher *Publisher) lastError() error {
	publisher.stateErrorLock.RLock()

	defer publisher.stateErrorLock.RUnlock()

	return publisher.stateError
}

func (publisher *Publisher) setStateError(err error) {
	publisher.stateErrorLock.Lock()

	defer publisher.stateErrorLock.Unlock()

	publisher.stateError = err
}

func (publisher *Publisher) clearStateError() {
	publisher.stateErrorLock.Lock()

	defer publisher.stateErrorLock.Unlock()

	publisher.stateError = nil
}

func (publisher *Publisher) processError(err error, msg *kafka.Message) (bool, error) {
	publisher.processErrorLock.Lock()

	defer publisher.processErrorLock.Unlock()

	if lastError := publisher.lastError(); lastError != nil {
		if err := publisher.opts.processDroppedMsg(msg, publisher.log); err != nil {
			return true, errors.Wrap(err, "cannot process dropped message")
		}

		return true, nil
	}

	// In fact, setStateError happens after a possible panic
	// but let's leave the strange defer just in case
	publisher.setStateError(err)
	publisher.log.Errorf(err, "cannot write message to Kafka")

	return false, nil
}

func NewPublisher(log Logger, opts ...*OptionsPublisher) *Publisher {
	finalOpts := obtainFinalOptionsPublisher(log, opts...)

	return &Publisher{
		log:  log,
		opts: finalOpts,

		writer:           finalOpts.writerFactory(),
		processErrorLock: &sync.Mutex{},

		stateError:     nil,
		stateErrorLock: &sync.RWMutex{},

		shutdownFlag: false,
		shutdownLock: &sync.RWMutex{},
	}
}
