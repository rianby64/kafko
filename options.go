package kafko

import "time"

type ReaderFactory func() Reader

type Incrementer interface {
	Inc()
}

type dummyIncrementer struct{}

func (*dummyIncrementer) Inc() {}

// Options is a configuration struct for a Kafka consumer.
type Options struct {
	commitInterval    time.Duration            // Time interval between committing offsets.
	reconnectInterval time.Duration            // Time interval between reconnect attempts.
	processingTimeout time.Duration            // Maximum allowed time for processing a message.
	processDroppedMsg ProcessDroppedMsgHandler // Handler function to process dropped messages.
	readerFactory     ReaderFactory            // Factory function to create Reader instances.

	metricMessagesProcessed Incrementer // Incrementer for the number of processed messages.
	metricMessagesDropped   Incrementer // Incrementer for the number of dropped messages.
	metricKafkaErrors       Incrementer // Incrementer for the number of Kafka errors.
}

// WithCommitInterval sets the commit interval for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *Options) WithCommitInterval(commitInterval time.Duration) *Options {
	opts.commitInterval = commitInterval

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
