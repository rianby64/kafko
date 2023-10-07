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

//go:generate mockgen -destination=./mocks/mock_writer.go -package=mocks kafko Writer
type Writer interface {
	Close() error
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Publisher struct {
	log  Logger
	opts *OptionsPublisher

	writer           Writer
	processErrorLock sync.Locker // this is a hack... get rid of this later

	stateError     error
	stateErrorLock *sync.RWMutex // this is a hack... get rid of this later

	shutdownFlag bool
	shutdownLock *sync.RWMutex // this is a hack... get rid of this later
}

func (publisher *Publisher) closeWriter() error {
	if publisher.writer != nil {
		if err := publisher.writer.Close(); err != nil {
			return errors.Wrapf(err, "cannot close writer")
		}
	}

	return nil
}

func (publisher *Publisher) checkShutdownFlag() bool {
	publisher.shutdownLock.RLock()

	defer publisher.shutdownLock.RUnlock()

	return publisher.shutdownFlag
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

func (publisher *Publisher) processError(ctx context.Context, err error, msg *kafka.Message) (bool, error) {
	publisher.processErrorLock.Lock()

	defer publisher.processErrorLock.Unlock()

	if lastError := publisher.lastError(); lastError != nil {
		if err := publisher.opts.processDroppedMsg.Handle(ctx, msg); err != nil {
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

func (publisher *Publisher) loopForever(ctx context.Context, msg *kafka.Message) error {
	shouldClearStateBeforeReturning := true

	// I need this strange defer because I need to guarantee panic won't block other attempts
	defer func() {
		if !shouldClearStateBeforeReturning {
			return
		}

		publisher.clearStateError()
	}()

	for {
		if err := publisher.writer.WriteMessages(ctx, *msg); err != nil {
			publisher.opts.metricErrors.Inc()

			if shoulExitFromLoop, err := publisher.processError(ctx, err, msg); shoulExitFromLoop {
				shouldClearStateBeforeReturning = false

				return err
			}

			publisher.opts.backoffStrategy.Wait(ctx)

			continue
		}

		publisher.opts.metricMessages.Inc()

		return nil
	}
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
