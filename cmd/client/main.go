package main

import (
	"encoding/binary"
	"net"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	"github.com/jylc/nijigen-queue/internal/core"
	"github.com/jylc/nijigen-queue/internal/pb"
)

func main() {
	conn, err := net.Dial("tcp", "0.0.0.0:6789")
	if err != nil {
		panic(err)
	}

	data, err := proto.Marshal(&pb.Message{
		Topic:     "key1",
		Operation: core.OperationPub,
		Content:   "11",
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
