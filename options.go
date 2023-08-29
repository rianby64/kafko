package kafko

import (
	"time"

	"github.com/segmentio/kafka-go"
)

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

// defaultProcessDroppedMsg logs a dropped message and returns a predefined error.
func defaultProcessDroppedMsg(msg *kafka.Message, lastError error, log Logger) {
	// Log the dropped message with its content.
	log.Errorf(lastError,
		"msg = %s, key = %s, topic = %s, partition = %d, offset = %d",
		string(msg.Value),
		string(msg.Key),
		msg.Topic,
		msg.Partition,
		msg.Offset,
	)
}

type keyGenerator interface {
	Generate() []byte
}

type keyGeneratorDefault struct{}

func (gen *keyGeneratorDefault) Generate() []byte {
	return nil // add an example with this piece of code: []byte(uuid.New().String())
}

type backoffStrategy interface {
	Wait()
}

type backoffStrategyDefault struct{}

func (gen *backoffStrategyDefault) Wait() {
	time.Sleep(waitNextAtempt)
}

// OptionsListener is a configuration struct for a Kafka consumer.
type OptionsListener struct {
	processDroppedMsg ProcessDroppedMsgHandler // Handler function to process dropped messages.
	readerFactory     ReaderFactory            // Factory function to create Reader instances.

	metricMessagesProcessed Incrementer // Incrementer for the number of processed messages.
	metricMessagesDropped   Incrementer // Incrementer for the number of dropped messages.
	metricErrors            Incrementer // Incrementer for the number of Kafka errors.
	metricDurationProcess   Duration
}

func (opts *OptionsListener) WithDurationProcess(metric Duration) *OptionsListener {
	opts.metricDurationProcess = metric

	return opts
}

// WithProcessDroppedMsg sets the dropped message processing handler for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *OptionsListener) WithProcessDroppedMsg(processDroppedMsg ProcessDroppedMsgHandler) *OptionsListener {
	pdm := defaultProcessDroppedMsg
	if processDroppedMsg != nil {
		pdm = processDroppedMsg
	}

	opts.processDroppedMsg = pdm

	return opts
}

// WithReaderFactory sets the reader factory function for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *OptionsListener) WithReaderFactory(readerFactory ReaderFactory) *OptionsListener {
	opts.readerFactory = readerFactory

	return opts
}

// WithMetricMessagesProcessed sets the messages processed incrementer for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *OptionsListener) WithMetricMessagesProcessed(metric Incrementer) *OptionsListener {
	opts.metricMessagesProcessed = metric

	return opts
}

// WithMetricMessagesDropped sets the messages dropped incrementer for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *OptionsListener) WithMetricMessagesDropped(metric Incrementer) *OptionsListener {
	opts.metricMessagesDropped = metric

	return opts
}

// WithMetricErrors sets the Kafka errors incrementer for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *OptionsListener) WithMetricErrors(metric Incrementer) *OptionsListener {
	opts.metricErrors = metric

	return opts
}

// NewOptionsListener creates a new Options instance with default values.
func NewOptionsListener() *OptionsListener {
	return &OptionsListener{}
}

func obtainFinalOptsListener(log Logger, opts []*OptionsListener) *OptionsListener {
	// Set the default options.
	finalOpts := &OptionsListener{
		processDroppedMsg: defaultProcessDroppedMsg,
		readerFactory: func() Reader {
			log.Panicf(ErrResourceIsNil, "provide the reader")

			return nil
		},

		metricMessagesProcessed: new(nopIncrementer),
		metricMessagesDropped:   new(nopIncrementer),
		metricErrors:            new(nopIncrementer),
		metricDurationProcess:   new(nopDuration),
	}

	// Iterate through the provided custom options and override defaults if needed.
	for _, opt := range opts {
		if opt.processDroppedMsg != nil {
			finalOpts.processDroppedMsg = opt.processDroppedMsg
		}

		if opt.readerFactory != nil {
			finalOpts.readerFactory = opt.readerFactory
		}

		if opt.metricMessagesProcessed != nil {
			finalOpts.metricMessagesProcessed = opt.metricMessagesProcessed
		}

		if opt.metricMessagesDropped != nil {
			finalOpts.metricMessagesDropped = opt.metricMessagesDropped
		}

		if opt.metricErrors != nil {
			finalOpts.metricErrors = opt.metricErrors
		}

		if opt.metricDurationProcess != nil {
			finalOpts.metricDurationProcess = opt.metricDurationProcess
		}
	}

	return finalOpts
}

type OptionsPublisher struct {
	writerFactory     WriterFactory
	processDroppedMsg ProcessDroppedMsgHandler
	keyGenerator      keyGenerator
	backoffStrategy   backoffStrategy

	metricMessages Incrementer
	metricErrors   Incrementer
	metricDuration Duration
}

func (opts *OptionsPublisher) WithWriterFactory(writerFactory WriterFactory) *OptionsPublisher {
	opts.writerFactory = writerFactory

	return opts
}

func (opts *OptionsPublisher) WithProcessDroppedMsg(handler ProcessDroppedMsgHandler) *OptionsPublisher {
	opts.processDroppedMsg = handler

	return opts
}

func (opts *OptionsPublisher) WithKeyGenerator(keyGenerator keyGenerator) *OptionsPublisher {
	opts.keyGenerator = keyGenerator

	return opts
}

func (opts *OptionsPublisher) WithBackoffStrategy(backoffStrategy backoffStrategy) *OptionsPublisher {
	opts.backoffStrategy = backoffStrategy

	return opts
}

func (opts *OptionsPublisher) WithMetricMessages(metric Incrementer) *OptionsPublisher {
	opts.metricMessages = metric

	return opts
}

func (opts *OptionsPublisher) WithMetricErrors(metric Incrementer) *OptionsPublisher {
	opts.metricErrors = metric

	return opts
}

func (opts *OptionsPublisher) WithMetricDurationProcess(metric Duration) *OptionsPublisher {
	opts.metricDuration = metric

	return opts
}

func obtainFinalOptionsPublisher(log Logger, opts ...*OptionsPublisher) *OptionsPublisher {
	finalOpts := &OptionsPublisher{
		writerFactory: func() Writer {
			log.Panicf(ErrResourceIsNil, "provide the writer")

			return nil
		},
		processDroppedMsg: defaultProcessDroppedMsg,
		keyGenerator:      new(keyGeneratorDefault),
		backoffStrategy:   new(backoffStrategyDefault),
		metricMessages:    new(nopIncrementer),
		metricErrors:      new(nopIncrementer),
		metricDuration:    new(nopDuration),
	}

	for _, opt := range opts {
		if opt.writerFactory != nil {
			finalOpts.writerFactory = opt.writerFactory
		}

		if opt.processDroppedMsg != nil {
			finalOpts.processDroppedMsg = opt.processDroppedMsg
		}

		if opt.keyGenerator != nil {
			finalOpts.keyGenerator = opt.keyGenerator
		}

		if opt.backoffStrategy != nil {
			finalOpts.backoffStrategy = opt.backoffStrategy
		}

		if opt.metricMessages != nil {
			finalOpts.metricMessages = opt.metricMessages
		}

		if opt.metricErrors != nil {
			finalOpts.metricErrors = opt.metricErrors
		}

		if opt.metricDuration != nil {
			finalOpts.metricDuration = opt.metricDuration
		}
	}

	return finalOpts
}

func NewOptionsPublisher() *OptionsPublisher {
	return &OptionsPublisher{}
}
