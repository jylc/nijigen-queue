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
	conns := make([]net.Conn, 0)
	for i := 0; i < 10; i++ {
		conns = append(conns, newConn())
	}
	for _, conn := range conns {
		outerWG.Add(1)
		go func(c net.Conn) {
			defer outerWG.Done()
			sub(c)

			var wg sync.WaitGroup
			for i := 0; i < 1000; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					pub(c)
				}()
			}
			wg.Wait()
		}(conn)
	}
	outerWG.Wait()
	time.Sleep(50 * time.Second)
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
