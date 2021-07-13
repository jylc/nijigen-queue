package logger

const (
	_trace = iota + 1
	_debug
	_info
	_warning
	_error
	_fatal
)

var (
	level = _info
)

func SetLevel(v int) {
	level = v
}
