package main

import (
	"net"
	"sync"

	"github.com/jylc/nijigen-queue/internal/pb"
	"github.com/jylc/nijigen-queue/internal/queue"
	"github.com/sirupsen/logrus"
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
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(conn net.Conn) {
		readBuf := make([]byte, 256)
		length, err := conn.Read(readBuf)
		if err != nil {
			panic(err)
		}
		if length != 0 {
			logrus.Info(string(readBuf))
		} else {
			logrus.Info("cannot receive message from server\n")
		}
		wg.Done()
	}(conn)
	wg.Wait()
}
