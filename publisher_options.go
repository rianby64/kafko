package kafko

type OptionsPublisher struct {
	processDroppedMsg MsgHandler

	writerFactory   WriterFactory
	keyGenerator    keyGenerator
	backoffStrategy backoffStrategy

	metricMessages Incrementer
	metricErrors   Incrementer
	metricDuration Duration

	time Time
}

func (opts *OptionsPublisher) WithProcessDroppedMsg(handler MsgHandler) *OptionsPublisher {
	opts.processDroppedMsg = handler

	return opts
}

func (opts *OptionsPublisher) WithWriterFactory(writerFactory WriterFactory) *OptionsPublisher {
	opts.writerFactory = writerFactory

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
			log.Panicf(ErrResourceUnavailable, "provide the writer")

			return nil
		},
		processDroppedMsg: new(defaultProcessDroppedMsg),
		keyGenerator:      new(keyGeneratorDefault),
		backoffStrategy:   new(backoffStrategyDefault),
		metricMessages:    new(nopIncrementer),
		metricErrors:      new(nopIncrementer),
		metricDuration:    new(nopDuration),
		time:              new(timeDefault),
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
