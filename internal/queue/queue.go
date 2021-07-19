package queue

import (
	"errors"
	"sync"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	"github.com/jylc/nijigen-queue/internal/goroutine"
	"github.com/jylc/nijigen-queue/internal/pb"
)

const (
	OperationSub = iota + 1
	OperationPub
)

var (
	ErrOp = errors.New("invalid operation")

	okbytes = []byte("OK")
)

type Queue struct {
	chmap map[string][]gnet.Conn
	lock  sync.RWMutex

	pool *ants.Pool
}

func NewQueue() *Queue {
	return &Queue{
		chmap: make(map[string][]gnet.Conn),
		pool:  goroutine.Default(),
	}
}

func (q *Queue) Handle(msg *pb.Message, conn gnet.Conn) ([]byte, error) {
	switch msg.Operation {
	case OperationSub:
		q.Subscribe(msg.Channel, conn)
		return okbytes, nil
	case OperationPub:
		if err := q.Publish(msg, conn); err != nil {
			return nil, err
		}
		return okbytes, nil
	default:
		return nil, ErrOp
	}
}

func (q *Queue) Subscribe(channel string, conn gnet.Conn) {
	q.lock.Lock()
	defer q.lock.Unlock()

	logrus.Debugf("sub: [%s] subscribe [%s]", conn.RemoteAddr().String(), channel)

	if addrs, ok := q.chmap[channel]; ok {
		q.chmap[channel] = append(addrs, conn)
	} else {
		q.chmap[channel] = []gnet.Conn{conn}
	}
}

func (q *Queue) Publish(msg *pb.Message, conn gnet.Conn) error {
	q.lock.RLock()
	defer q.lock.RUnlock()

	logrus.Debugf("pub: [%s] publish channel [%s] with content [%s]", conn.RemoteAddr().String(), msg.Channel, msg.Content)

	if conns, ok := q.chmap[msg.Channel]; ok {
		for _, conn := range conns {
			if err := q.publish(conn, &pb.Publish{
				Channel: msg.Channel,
				Content: msg.Content,
			}); err != nil {
				// TODO 错误处理
				panic(err)
			}
		}
	} else {
		return errors.New("no subscriber")
	}

	return nil
}

func (q *Queue) publish(conn gnet.Conn, pub *pb.Publish) error {
	msg, err := proto.Marshal(pub)
	if err != nil {
		return err
	}

	err = q.pool.Submit(func() {
		if err = conn.AsyncWrite(msg); err != nil {
			logrus.Errorf("channel [%s] write message [%s] to [%s] error: %v", pub.Channel, pub.Content, conn.RemoteAddr(), err)
		}
	})
	if err != nil {
		return err
	}

	return nil
}
