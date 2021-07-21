package main

import (
	"net"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
)

func main() {
	conn, err := net.Dial("tcp", "0.0.0.0:6789")
	if err != nil {
		panic(err)
	}

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

		conn.Close()
		wg.Done()
	}(conn)
	wg.Wait()
}
