package logger

import (
	"log"
	"os"
)

type logger struct{}

func (l *logger) Debug(v []byte) {
	if level > _debug {
		return
	}

	_, _ = os.Stdout.Write(v)
}

func (l *logger) Info(v []byte) {
	if level > _info {
		return
	}

	_, _ = os.Stdout.Write(v)
}

func (l *logger) Warning(v []byte) {
	if level > _warning {
		return
	}

	_, _ = os.Stdout.Write(v)
}

func (l *logger) Error(v []byte) {
	if level > _error {
		return
	}

	_, _ = os.Stderr.Write(v)
}

func (l *logger) Fatal(v []byte) {
	log.Fatal(v)
}

var (
	l = &logger{}
)

func Debug(v []byte) {
	l.Debug(v)
}

func Info(v []byte) {
	l.Info(v)
}

func Warning(v []byte) {
	l.Warning(v)
}

func Error(v []byte) {
	l.Error(v)
}

func Fatal(v []byte) {
	l.Fatal(v)
}
