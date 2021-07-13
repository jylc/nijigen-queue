package nijigen_queue

import (
	"errors"
	"net"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/internal/pb"
)

type Queue struct {
	chmap map[string][]net.Conn
	lock  sync.RWMutex
}

func NewQueue() *Queue {
	return &Queue{chmap: make(map[string][]net.Conn)}
}

func (q *Queue) Subscribe(channel string, conn net.Conn) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if conns, ok := q.chmap[channel]; ok {
		q.chmap[channel] = append(conns, conn)
	} else {
		q.chmap[channel] = []net.Conn{conn}
	}
}

func (q *Queue) Publish(req *pb.Request) error {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if conns, ok := q.chmap[req.Channel]; ok {
		for _, conn := range conns {
			if err := q.publish(conn, req.Message); err != nil {
				// TODO 错误处理
				panic(err)
			}
		}
	} else {
		// TODO 保存下来，等有订阅者的时候再 push
		return errors.New("no subscriber")
	}

	return nil
}

func (q *Queue) publish(conn net.Conn, message *pb.Message) error {
	msg, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	if _, err = conn.Write(msg); err != nil {
		return err
	}

	return nil
}
