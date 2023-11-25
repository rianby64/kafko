package kafko

type OptionsPublisher struct {
	droppedMsg MsgHandler

	writerFactory          WriterFactory
	keyGenerator           KeyGenerator
	backoffStrategyFactory BackoffStrategyFactory

	metricMessages Incrementer
	metricErrors   Incrementer
	metricDuration Duration

	time Time
}

func (opts *OptionsPublisher) WithHandlerDropped(handler MsgHandler) *OptionsPublisher {
	opts.droppedMsg = handler

	return opts
}

func (opts *OptionsPublisher) WithWriterFactory(writerFactory WriterFactory) *OptionsPublisher {
	opts.writerFactory = writerFactory

	return opts
}

func (opts *OptionsPublisher) WithKeyGenerator(keyGenerator KeyGenerator) *OptionsPublisher {
	opts.keyGenerator = keyGenerator

	return opts
}

func (opts *OptionsPublisher) WithBackoffStrategyFactory(backoffStrategyFactory BackoffStrategyFactory) *OptionsPublisher {
	opts.backoffStrategyFactory = backoffStrategyFactory

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
			log.Panicf(ErrResourceUnavailable, "provide the writer")

			return nil
		},

		metricMessages: new(nopIncrementer),
		metricErrors:   new(nopIncrementer),
		metricDuration: new(nopDuration),
		time:           new(timeDefault),

		droppedMsg:             new(defaultHandlerDroppedMsg),
		keyGenerator:           new(defaultKeyGenerator),
		backoffStrategyFactory: defaultBackoffStrategyFactory,
	}

	for _, opt := range opts {
		if opt.writerFactory != nil {
			finalOpts.writerFactory = opt.writerFactory
		}

		if opt.droppedMsg != nil {
			finalOpts.droppedMsg = opt.droppedMsg
		}

		if opt.keyGenerator != nil {
			finalOpts.keyGenerator = opt.keyGenerator
		}

		if opt.backoffStrategyFactory != nil {
			finalOpts.backoffStrategyFactory = opt.backoffStrategyFactory
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
