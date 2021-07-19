package main

import (
	"encoding/binary"
	"net"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/jylc/nijigen-queue/internal/core"
	"github.com/jylc/nijigen-queue/internal/pb"
)

func BenchmarkPublish(b *testing.B) {
	conn, err := net.Dial("tcp", "0.0.0.0:6789")
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	b.RunParallel(func(tpb *testing.PB) {
		for tpb.Next() {
			sub(conn)
			pub(conn)
		}
	})
}

func sub(conn net.Conn) {
	data, err := proto.Marshal(&pb.Message{
		Channel:   "test key",
		Operation: core.OperationSub,
	})
	if err != nil {
		panic(err)
	}

	l := uint32(len(data))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, l)

	_, err = conn.Write(append(buf, data...))
	if err != nil {
		panic(err)
	}
}

func pub(conn net.Conn) {
	data, err := proto.Marshal(&pb.Message{
		Channel:   "test key",
		Operation: core.OperationPub,
		Content:   "test content",
	})
	if err != nil {
		panic(err)
	}

	l := uint32(len(data))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, l)

	_, err = conn.Write(append(buf, data...))
	if err != nil {
		panic(err)
	}
}
