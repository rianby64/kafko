package kafko

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

var (
	ErrRetryReached        = errors.New("retry reached")
	ErrMessageDropped      = errors.New("message dropped")
	ErrResourceUnavailable = errors.New("resource unavailable")
)

//go:generate mockgen -destination=./mocks/mock_msg_handler.go -package=mocks kafko MsgHandler
type MsgHandler interface {
	Handle(ctx context.Context, msg *kafka.Message) error
}

type Logger interface {
	Printf(format string, v ...any)
	Panicf(err error, format string, v ...any)
	Errorf(err error, format string, v ...any)
}

type Time interface {
	Now() time.Time
}

type timeDefault struct{}

func (timeDefault) Now() time.Time {
	return time.Now()
}

type ReaderFactory func() Reader
type WriterFactory func() Writer

type Incrementer interface {
	Inc()
}

type nopIncrementer struct{}

func (n *nopIncrementer) Inc() {}

type Duration interface {
	Observe(float64)
}

type nopDuration struct{}

func (n *nopDuration) Observe(float64) {}

type defaultHandlerDroppedMsg struct{}

// defaultProcessDroppedMsg logs a dropped message and returns a predefined error.
func (defaultHandlerDroppedMsg) Handle(_ context.Context, msg *kafka.Message) error {
	return errors.Wrapf(ErrMessageDropped,
		"msg = %s, key = %s, topic = %s, partition = %d, offset = %d",
		string(msg.Value),
		string(msg.Key),
		msg.Topic,
		msg.Partition,
		msg.Offset,
	)
}

//go:generate mockgen -destination=./mocks/mock_keygenerator.go -package=mocks kafko KeyGenerator
type KeyGenerator interface {
	Generate() []byte
}

type defaultKeyGenerator struct{}

func (gen *defaultKeyGenerator) Generate() []byte {
	return nil // add an example with this piece of code: []byte(uuid.New().String())
}

type BackoffStrategyFactory func() BackoffStrategy

//go:generate mockgen -destination=./mocks/mock_backoff_strategy.go -package=mocks kafko BackoffStrategy
type BackoffStrategy interface {
	Wait(ctx context.Context) error
}

func defaultBackoffStrategyFactory() BackoffStrategy {
	return &defaultBackoffStrategy{}
}

type defaultBackoffStrategy struct {
	numberOfAttempts int
}

// Wait is intended to implement a wait using sleep.
//
// If this method returns an error then it means, the loop should break.
//
// If this method returns nil then it means, the loop should retry.
func (gen *defaultBackoffStrategy) Wait(ctx context.Context) error {
	gen.numberOfAttempts++

	if gen.numberOfAttempts > maxAttempts {
		return ErrRetryReached
	}

	select {
	case <-time.After(waitNextAttempt):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
