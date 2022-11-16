package log

import (
	"context"
)

type (
	Logger interface {
		Infof(string, ...interface{})
		Debugf(string, ...interface{})
		Tracef(string, ...interface{})
		Warnf(string, ...interface{})
		Errorf(string, ...interface{})
		Fatalf(string, ...interface{})
		Panicf(string, ...interface{})

		Info(...interface{})
		Debug(...interface{})
		Trace(...interface{})
		Warn(...interface{})
		Error(...interface{})
		Fatal(...interface{})
		Panic(...interface{})

		WithField(key string, val interface{}) Logger

		WithFields(fields Fields) Logger
	}
	// Fields is alias of map
	Fields = map[string]interface{}

	// context key
	contextKey string
)

const (
	loggerKey contextKey = contextKey("logger_key")
)

var (
	root Logger
)

// Root return default logger instance
func Root() Logger {
	if root == nil {
		root = newRlog()
	}
	return root
}

// NewContext return a new logger context
func NewContext(ctx context.Context, logger Logger) context.Context {
	if logger == nil {
		logger = Root()
	}
	return context.WithValue(ctx, loggerKey, logger)
}

// FromContext get logger form context
func FromContext(ctx context.Context) Logger {
	if ctx == nil {
		return Root()
	}
	if logger, ok := ctx.Value(loggerKey).(Logger); ok {
		return logger
	}
	return Root()
}

// Infof print info with format.
func Infof(format string, v ...interface{}) {
	Root().Infof(format, v...)
}

// Debugf print debug with format.
func Debugf(format string, v ...interface{}) {
	Root().Debugf(format, v...)
}

func Tracef(format string, v ...interface{}) {
	Root().Tracef(format, v...)
}

// Warnf print warning with format.
func Warnf(format string, v ...interface{}) {
	Root().Warnf(format, v...)
}

// Errorf print error with format.
func Errorf(format string, v ...interface{}) {
	Root().Errorf(format, v...)
}

// Panicf panic with format.
func Panicf(format string, v ...interface{}) {
	Root().Panicf(format, v...)
}

func Fatalf(format string, v ...interface{}) {
	Root().Fatalf(format, v...)
}

func Info(args ...interface{}) {
	Root().Info(args...)
}

func Debug(args ...interface{}) {
	Root().Debug(args...)
}
func Trace(args ...interface{}) {
	Root().Trace(args...)
}
func Warn(args ...interface{}) {
	Root().Trace(args...)
}

func Error(args ...interface{}) {
	Root().Error(args...)
}
func Panic(args ...interface{}) {
	Root().Panic(args...)
}
func Fatal(args ...interface{}) {
	Root().Fatal(args...)
}

// WithFields return a new logger entry with fields
func WithFields(fields Fields) Logger {
	return Root().WithFields(fields)
}

func WithField(key string, value interface{}) Logger {
	return Root().WithField(key, value)
}

// WithContext return a logger from the given context
func WithContext(ctx context.Context) Logger {
	return FromContext(ctx)
}
