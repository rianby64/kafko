package log

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

type Logger interface {
	Printf(format string, v ...any)
	Panicf(err error, format string, v ...any)
	Errorf(err error, format string, v ...any)
}

type LoggerInternal struct {
	err,
	info,
	panic zerolog.Logger
}

func (log *LoggerInternal) Panicf(err error, format string, v ...any) {
	log.panic.Panic().Err(errors.WithStack(err)).Msgf(format, v...)
}

func (log *LoggerInternal) Printf(format string, v ...any) {
	log.info.Info().Msgf(format, v...)
}

func (log *LoggerInternal) Errorf(err error, format string, v ...any) {
	log.err.Error().Err(errors.WithStack(err)).Msgf(format, v...)
}

func NewLogger() *LoggerInternal {
	return newConsoleLogger()
}

func newConsoleLogger() *LoggerInternal {
	consoleWriter := zerolog.NewConsoleWriter()
	devLogger := &LoggerInternal{
		info:  zerolog.New(consoleWriter),
		err:   zerolog.New(consoleWriter),
		panic: zerolog.New(consoleWriter),
	}

	return devLogger
}
