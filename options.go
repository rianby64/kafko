package kafko

import (
	"time"

	"github.com/segmentio/kafka-go"
)

type ReaderFactory func() Reader

type Incrementer interface {
	Inc()
}

// Options is a configuration struct for a Kafka consumer.
type Options struct {
	recommitInterval  time.Duration            // Time interval between attempts to commit uncommitted messages.
	reconnectInterval time.Duration            // Time interval between reconnect attempts.
	processingTimeout time.Duration            // Maximum allowed time for processing a message.
	processDroppedMsg ProcessDroppedMsgHandler // Handler function to process dropped messages.
	readerFactory     ReaderFactory            // Factory function to create Reader instances.

	metricMessagesProcessed Incrementer // Incrementer for the number of processed messages.
	metricMessagesDropped   Incrementer // Incrementer for the number of dropped messages.
	metricKafkaErrors       Incrementer // Incrementer for the number of Kafka errors.
}

// WithRecommitInterval sets the commit interval for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *Options) WithRecommitInterval(recommitInterval time.Duration) *Options {
	opts.recommitInterval = recommitInterval

	return opts
}

// WithReconnectInterval sets the reconnect interval for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *Options) WithReconnectInterval(reconnectInterval time.Duration) *Options {
	opts.reconnectInterval = reconnectInterval

	return opts
}

// WithProcessingTimeout sets the processing timeout for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *Options) WithProcessingTimeout(processingTimeout time.Duration) *Options {
	opts.processingTimeout = processingTimeout

	return opts
}

// WithProcessDroppedMsg sets the dropped message processing handler for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *Options) WithProcessDroppedMsg(processDroppedMsg ProcessDroppedMsgHandler) *Options {
	pdm := defaultProcessDroppedMsg
	if processDroppedMsg != nil {
		pdm = processDroppedMsg
	}

	opts.processDroppedMsg = pdm

	return opts
}

// WithReaderFactory sets the reader factory function for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *Options) WithReaderFactory(readerFactory ReaderFactory) *Options {
	opts.readerFactory = readerFactory

	return opts
}

// WithMetricMessagesProcessed sets the messages processed incrementer for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *Options) WithMetricMessagesProcessed(metric Incrementer) *Options {
	opts.metricMessagesProcessed = metric

	return opts
}

// WithMetricMessagesDropped sets the messages dropped incrementer for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *Options) WithMetricMessagesDropped(metric Incrementer) *Options {
	opts.metricMessagesDropped = metric

	return opts
}

// WithMetricKafkaErrors sets the Kafka errors incrementer for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *Options) WithMetricKafkaErrors(metric Incrementer) *Options {
	opts.metricKafkaErrors = metric

	return opts
}

// NewOptions creates a new Options instance with default values.
func NewOptions() *Options {
	return &Options{}
}

// defaultProcessDroppedMsg logs a dropped message and returns a predefined error.
func defaultProcessDroppedMsg(msg *kafka.Message, log Logger) error {
	// Log the dropped message with its content.
	log.Errorf(ErrMessageDropped, "msg = %s, key = %s, topic = %s, partition = %d, offset = %d", string(msg.Value), string(msg.Key), msg.Topic, msg.Partition, msg.Offset)

	// Return a predefined error for dropped messages.
	return ErrMessageDropped
}

type nopIncrementer struct{}

func (n *nopIncrementer) Inc() {}

func obtainFinalOpts(log Logger, opts []*Options) *Options {
	// Set the default options.
	finalOpts := &Options{
		recommitInterval:  commitInterval,
		processDroppedMsg: defaultProcessDroppedMsg,
		processingTimeout: processingTimeout,
		reconnectInterval: reconnectInterval,
		readerFactory: func() Reader {
			log.Panicf(ErrResourceIsNil, "provide the reader")

			return nil
		},

		metricMessagesProcessed: new(nopIncrementer),
		metricMessagesDropped:   new(nopIncrementer),
		metricKafkaErrors:       new(nopIncrementer),
	}

	// Iterate through the provided custom options and override defaults if needed.
	for _, opt := range opts {
		if opt.processingTimeout != 0 {
			finalOpts.processingTimeout = opt.processingTimeout
		}

		if opt.reconnectInterval != 0 {
			finalOpts.reconnectInterval = opt.reconnectInterval
		}

		if opt.recommitInterval != 0 {
			finalOpts.recommitInterval = opt.recommitInterval
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

		if opt.metricKafkaErrors != nil {
			finalOpts.metricKafkaErrors = opt.metricKafkaErrors
		}
	}

	return finalOpts
}
