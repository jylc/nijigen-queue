package queue

import (
	"errors"
	"net"
	"sync"

	"github.com/jylc/nijigen-queue/internal/pb"
	"google.golang.org/protobuf/proto"
)

const (
	OperationSub = iota + 1
	OperationPub
)

var (
	ErrOp = errors.New("invalid operation")
)

type Queue struct {
	chmap map[string][]net.Addr
	lock  sync.RWMutex
}

func NewQueue() *Queue {
	return &Queue{chmap: make(map[string][]net.Addr)}
}

func (q *Queue) Handle(msg *pb.Message, addr net.Addr) ([]byte, error) {
	switch msg.Operation {
	case OperationSub:
		q.Subscribe(msg.Channel, addr)
		return []byte("OK"), nil
	case OperationPub:
		if err := q.Publish(msg); err != nil {
			return nil, err
		}

		return []byte("OK"), nil
	default:
		return nil, ErrOp
	}
}

func (q *Queue) Subscribe(channel string, addr net.Addr) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if addrs, ok := q.chmap[channel]; ok {
		q.chmap[channel] = append(addrs, addr)
	} else {
		q.chmap[channel] = []net.Addr{addr}
	}
}

func (q *Queue) Publish(msg *pb.Message) error {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if addrs, ok := q.chmap[msg.Channel]; ok {
		for _, addr := range addrs {
			if err := q.publish(addr, &pb.Publish{
				Channel: msg.Channel,
				Content: msg.Content,
			}); err != nil {
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

func (q *Queue) publish(addr net.Addr, pub *pb.Publish) error {
	msg, err := proto.Marshal(pub)
	if err != nil {
		return err
	}

	conn, err := net.Dial(addr.Network(), addr.String())
	if err != nil {
		return err
	}
	if _, err = conn.Write(msg); err != nil {
		return err
	}

	return nil
}
