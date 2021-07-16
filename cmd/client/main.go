package main

import (
	"net"

	"github.com/jylc/nijigen-queue/internal/pb"
	"github.com/jylc/nijigen-queue/internal/queue"
	"google.golang.org/protobuf/proto"
)

func main() {
	conn, err := net.Dial("tcp", "0.0.0.0:6789")
	if err != nil {
		panic(err)
	}

	data, err := proto.Marshal(&pb.Message{
		Channel:   "key1",
		Operation: queue.OperationSub,
		Content:   "11",
	})
	if err != nil {
		panic(err)
	}

	l := uint32(len(data))
	buf := make([]byte, 4)
	buf[3] = uint8(l)
	buf[2] = uint8(l >> 8)
	buf[1] = uint8(l >> 16)
	buf[0] = uint8(l >> 24)
	_, err = conn.Write(append(buf, data...))
	if err != nil {
		panic(err)
	}
}
