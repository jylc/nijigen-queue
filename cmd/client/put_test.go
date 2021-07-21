package main

import (
	"net"
	"testing"

	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
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

func newConn() net.Conn {
	conn, err := net.Dial("tcp", "0.0.0.0:6789")
	if err != nil {
		panic(err)
	}

	return conn
}

func sub(conn net.Conn) {
	data, err := message.BuildSubscribeRequest(&pb.SubscribeRequest{
		Topic:   "topic-1",
		Channel: "channel-1",
	})
	if err != nil {
		panic(err)
	}

	_, err = conn.Write(data)
	if err != nil {
		panic(err)
	}
}

func pub(conn net.Conn) {
	data, err := message.BuildPublicRequest(&pb.PublicRequest{
		Topic:   "topic-1",
		Channel: "channel-1",
		Content: "test content",
	})
	if err != nil {
		panic(err)
	}

	_, err = conn.Write(data)
	if err != nil {
		panic(err)
	}
}
