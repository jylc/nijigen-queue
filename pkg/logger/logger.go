package logger

type Logger interface {
	Debug([]byte)
	Info([]byte)
	Warning([]byte)
	Error([]byte)
	Fatal([]byte)
}
