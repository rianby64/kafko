package log

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

type Logger interface {
	Printf(format string, v ...any)
	Panicf(err error, format string, v ...any)
	Errorf(err error, format string, v ...any)
}

type MockLogger struct {
	PrintMessages []string
	PanicMessages []string
	ErrorMessages []string
}

func (m *MockLogger) Printf(format string, v ...interface{}) {
	m.PrintMessages = append(m.PrintMessages, fmt.Sprintf(format, v...))
}

func (m *MockLogger) Panicf(err error, format string, v ...interface{}) {
	m.PanicMessages = append(m.PanicMessages, fmt.Sprint(err, fmt.Sprintf(format, v...)))
}

func (m *MockLogger) Errorf(err error, format string, v ...interface{}) {
	m.ErrorMessages = append(m.ErrorMessages, fmt.Sprint(err, fmt.Sprintf(format, v...)))
}

func NewMockLogger() *MockLogger {
	return &MockLogger{}
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
