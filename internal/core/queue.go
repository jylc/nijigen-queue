package core

import (
	"errors"
	"sync"

	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/pb"
)

const (
	OperationSub = iota + 1
	OperationPub
)

var (
	ErrOp    = errors.New("invalid operation")
	ErrNoSub = errors.New("no subscriber")

	okbytes = []byte("OK")
)

type Topic struct {
	chmap map[string]*Channel
	lock  sync.RWMutex
}

func NewTopic() *Topic {
	return &Topic{
		chmap: make(map[string]*Channel),
	}
}

func (q *Topic) fixCh(channel string) {
	if _, ok := q.chmap[channel]; !ok {
		q.lock.Lock()
		if _, ok = q.chmap[channel]; !ok {
			q.chmap[channel] = NewChannel()
		}
		q.lock.Unlock()
	}
}

func (q *Topic) Subscribe(channel string, conn gnet.Conn) {
	logrus.Infof("sub: [%s] subscribe [%s]", conn.RemoteAddr().String(), channel)

	q.fixCh(channel)

	q.lock.RLock()
	defer q.lock.RUnlock()

	ch := q.chmap[channel]
	ch.AddSubscriber(conn)
}

func (q *Topic) Publish(msg *pb.Message, conn gnet.Conn) error {
	q.lock.RLock()
	defer q.lock.RUnlock()

	logrus.Infof("pub: [%s] publish channel [%s] with content [%s]", conn.RemoteAddr().String(), msg.Channel, msg.Content)

	if ch, ok := q.chmap[msg.Channel]; ok {
		err := ch.Publish(conn, &pb.Publish{
			Channel: msg.Channel,
			Content: msg.Content,
		})
		if err != nil {
			return err
		}
	} else {
		return ErrNoSub
	}

	return nil
}
