package tools

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Warp(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
