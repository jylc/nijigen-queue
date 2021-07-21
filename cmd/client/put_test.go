package main

import (
	"testing"
)

func BenchmarkPublish(b *testing.B) {
	conn := newConn()
	sub(conn)
	b.ResetTimer()
	//b.RunParallel(func(tpb *testing.PB) {
	//	for tpb.Next() {
	//		pub(conn)
	//	}
	//})
	for i := 0; i < b.N; i++ {
		pub(conn)
	}
}
