package main

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
)

func main() {
	cnt := int64(0)

	conn := newConn()
	defer conn.Close()

	sub(conn)

	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			pub(conn)
			fmt.Println(atomic.AddInt64(&cnt, 1))
		}()
	}
	wg.Wait()

	time.Sleep(5 * time.Second)
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
