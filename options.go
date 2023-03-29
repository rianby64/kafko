package kafkame

import "time"

type Options struct {
	commitInterval    time.Duration
	reconnectInterval time.Duration
	processingTimeout time.Duration
	processDroppedMsg ProcessDroppedMsgHandler
	reader            Reader
}

func (opts *Options) WithCommitInterval(commitInterval time.Duration) *Options {
	opts.commitInterval = commitInterval

	return opts
}

func (opts *Options) WithReconnectInterval(reconnectInterval time.Duration) *Options {
	opts.reconnectInterval = reconnectInterval

	return opts
}

func (opts *Options) WithProcessingTimeout(processingTimeout time.Duration) *Options {
	opts.processingTimeout = processingTimeout

	return opts
}

func (opts *Options) WithProcessDroppedMsg(processDroppedMsg ProcessDroppedMsgHandler) *Options {
	pdm := defaultProcessDroppedMsg
	if processDroppedMsg != nil {
		pdm = processDroppedMsg
	}

	opts.processDroppedMsg = pdm

	return opts
}

func (opts *Options) WithReader(reader Reader) *Options {
	opts.reader = reader

	return opts
}

func NewOptions() *Options {
	return &Options{}
}
