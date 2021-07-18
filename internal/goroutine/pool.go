package goroutine

import (
	"time"

	"github.com/panjf2000/ants/v2"
)

const (
	ExpiryDuration = 10 * time.Second
	DefaultSize    = 1 << 18
)

func Default() *ants.Pool {
	options := ants.Options{ExpiryDuration: ExpiryDuration, Nonblocking: false}
	defaultAntsPool, _ := ants.NewPool(DefaultSize, ants.WithOptions(options))
	return defaultAntsPool
}
