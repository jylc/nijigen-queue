package main

import (
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	"github.com/jylc/nijigen-queue/internal/pb"
	"github.com/jylc/nijigen-queue/internal/queue"
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
	var buffer [0x10000]byte
	go func(conn net.Conn) {
		n, err := conn.Read(buffer[:])
		if err != nil {
			panic(err)
		}
		if n != 0 {
			logrus.Info(string(buffer[:n]))
		} else {
			logrus.Info("cannot receive message from server\n")
		}

		time.Sleep(2 * time.Second)
		conn.Close()
		wg.Done()
	}(conn)
	wg.Wait()
}
