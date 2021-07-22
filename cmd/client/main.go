package main

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
)

func main() {
	var outerWG sync.WaitGroup
	for i := 0; i < 10; i++ {
		outerWG.Add(1)
		go func() {
			conn := newConn()
			defer conn.Close()

			sub(conn)

			var wg sync.WaitGroup
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					pub(conn)
				}()
			}
			wg.Wait()
		}()
	}
	outerWG.Done()
	time.Sleep(5 * time.Second)
}

func newConn() net.Conn {
	conn, err := net.DialTCP("tcp", nil, &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 6789,
	})
	if err != nil {
		panic(err)
	}

	//err = conn.SetDeadline(time.Time{})
	//if err != nil {
	//	panic(err)
	//}

	//if err = conn.SetKeepAlive(true); err != nil {
	//	panic(err)
	//}
	//if err = conn.SetKeepAlivePeriod(5 * time.Second); err != nil {
	//	panic(err)
	//}

	return conn
}

func sub(conn net.Conn) {
	data, err := message.BuildSubscribeRequest(&pb.SubscribeRequest{
		Topic:   fmt.Sprintf("topic-%d", rand.Intn(100)),
		Channel: fmt.Sprintf("channel-%d", rand.Intn(100)),
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
		Topic:   fmt.Sprintf("topic-%d", rand.Intn(100)),
		Channel: fmt.Sprintf("channel-%d", rand.Intn(100)),
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
