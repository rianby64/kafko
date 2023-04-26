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
func defaultProcessDroppedMsg(msg *kafka.Message, log Logger) error {
	// Log the dropped message with its content.
	log.Errorf(ErrMessageDropped, "msg = %s, key = %s, topic = %s, partition = %d, offset = %d", string(msg.Value), string(msg.Key), msg.Topic, msg.Partition, msg.Offset)

	// Return a predefined error for dropped messages.
	return ErrMessageDropped
}

// OptionsListener is a configuration struct for a Kafka consumer.
type OptionsListener struct {
	recommitTicker    *time.Ticker             // Time interval between attempts to commit uncommitted messages.
	reconnectInterval time.Duration            // Time interval between reconnect attempts.
	processingTimeout time.Duration            // Maximum allowed time for processing a message.
	processDroppedMsg ProcessDroppedMsgHandler // Handler function to process dropped messages.
	readerFactory     ReaderFactory            // Factory function to create Reader instances.

	metricMessagesProcessed Incrementer // Incrementer for the number of processed messages.
	metricMessagesDropped   Incrementer // Incrementer for the number of dropped messages.
	metricErrors            Incrementer // Incrementer for the number of Kafka errors.
	metricDurationProcess   Duration
}

// WithRecommitInterval sets the commit interval for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *OptionsListener) WithRecommitInterval(recommitInterval time.Duration) *OptionsListener {
	opts.recommitTicker = time.NewTicker(recommitInterval)

	return opts
}

// WithReconnectInterval sets the reconnect interval for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *OptionsListener) WithReconnectInterval(reconnectInterval time.Duration) *OptionsListener {
	opts.reconnectInterval = reconnectInterval

	return opts
}

// WithProcessingTimeout sets the processing timeout for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *OptionsListener) WithProcessingTimeout(processingTimeout time.Duration) *OptionsListener {
	opts.processingTimeout = processingTimeout

	return opts
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

func obtainFinalOptsListener(log Logger, opts []*OptionsListener) *OptionsListener { //nolint:cyclop
	// Set the default options.
	finalOpts := &OptionsListener{
		recommitTicker:    time.NewTicker(commitInterval),
		processDroppedMsg: defaultProcessDroppedMsg,
		processingTimeout: processingTimeout,
		reconnectInterval: reconnectInterval,
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
		if opt.processingTimeout != 0 {
			finalOpts.processingTimeout = opt.processingTimeout
		}

		if opt.reconnectInterval != 0 {
			finalOpts.reconnectInterval = opt.reconnectInterval
		}

		if opt.recommitTicker != nil {
			finalOpts.recommitTicker = opt.recommitTicker
		}

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
