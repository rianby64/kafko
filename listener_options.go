package kafko

// OptionsListener is a configuration struct for a Kafka consumer.
type OptionsListener struct {
	readerFactory ReaderFactory // Factory function to create Reader instances.

	metricMessagesProcessed Incrementer // Incrementer for the number of processed messages.
	metricMessagesError     Incrementer // Incrementer for the number of dropped messages.
	metricErrors            Incrementer // Incrementer for the number of Kafka errors.
	metricDurationProcess   Duration    // Observer for the duration of processing a kafka message.
	time                    Time        // Obtain the value of time.Now() by default - testing purposes

	processMsg MsgHandler // Handler function to process the message from the queue.
}

func (opts *OptionsListener) WithDurationProcess(metric Duration) *OptionsListener {
	opts.metricDurationProcess = metric

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
	opts.metricMessagesError = metric

	return opts
}

// WithMetricErrors sets the Kafka errors incrementer for the Options instance.
// Returns the updated Options instance for method chaining.
func (opts *OptionsListener) WithMetricErrors(metric Incrementer) *OptionsListener {
	opts.metricErrors = metric

	return opts
}

func (opts *OptionsListener) WithHandler(handler MsgHandler) *OptionsListener {
	opts.processMsg = handler

	return opts
}

// NewOptionsListener creates a new Options instance with default values.
func NewOptionsListener() *OptionsListener {
	return &OptionsListener{}
}

func obtainFinalOptsListener(log Logger, opts []*OptionsListener) *OptionsListener {
	// Set the default options.
	finalOpts := &OptionsListener{
		readerFactory: func() Reader {
			log.Panicf(ErrResourceUnavailable, "provide the reader")

			return nil
		},

		metricMessagesProcessed: new(nopIncrementer),
		metricMessagesError:     new(nopIncrementer),
		metricErrors:            new(nopIncrementer),
		metricDurationProcess:   new(nopDuration),
		time:                    new(timeDefault),

		processMsg: nil,
	}

	// Iterate through the provided custom options and override defaults if needed.
	for _, opt := range opts {
		if opt.readerFactory != nil {
			finalOpts.readerFactory = opt.readerFactory
		}

		if opt.metricMessagesProcessed != nil {
			finalOpts.metricMessagesProcessed = opt.metricMessagesProcessed
		}

		if opt.metricMessagesError != nil {
			finalOpts.metricMessagesError = opt.metricMessagesError
		}

		if opt.metricErrors != nil {
			finalOpts.metricErrors = opt.metricErrors
		}

		if opt.metricDurationProcess != nil {
			finalOpts.metricDurationProcess = opt.metricDurationProcess
		}

		if opt.processMsg != nil {
			finalOpts.processMsg = opt.processMsg
		}
	}

	if finalOpts.processMsg == nil {
		log.Panicf(ErrResourceUnavailable, "no handler defined for processing message")
	}

	return finalOpts
}
