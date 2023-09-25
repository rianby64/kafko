package kafko

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

var (
	ErrMessageDropped      = errors.New("message dropped")
	ErrResourceUnavailable = errors.New("resource unavailable")
)

type MsgHandler interface {
	Handle(msg *kafka.Message) error
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

type defaultProcessDroppedMsg struct{}

// defaultProcessDroppedMsg logs a dropped message and returns a predefined error.
func (defaultProcessDroppedMsg) Handle(_ *kafka.Message) error {
	// Log the dropped message with its content.
	// log.Errorf(ErrMessageDropped,
	// 	"msg = %s, key = %s, topic = %s, partition = %d, offset = %d",
	// 	string(msg.Value),
	// 	string(msg.Key),
	// 	msg.Topic,
	// 	msg.Partition,
	// 	msg.Offset,
	// )

	return ErrMessageDropped
}

type keyGenerator interface {
	Generate() []byte
}

type keyGeneratorDefault struct{}

func (gen *keyGeneratorDefault) Generate() []byte {
	return nil // add an example with this piece of code: []byte(uuid.New().String())
}

type backoffStrategy interface {
	Wait(ctx context.Context)
}

type backoffStrategyDefault struct{}

func (gen *backoffStrategyDefault) Wait(ctx context.Context) {
	select {
	case <-time.After(waitNextAtempt):
		return
	case <-ctx.Done():
		return
	}
}
