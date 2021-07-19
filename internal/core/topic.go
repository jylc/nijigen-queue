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

func (t *Topic) fixCh(channel string) {
	if _, ok := t.chmap[channel]; !ok {
		t.lock.Lock()
		if _, ok = t.chmap[channel]; !ok {
			t.chmap[channel] = NewChannel()
		}
		t.lock.Unlock()
	}
}

func (t *Topic) Subscribe(channel string, conn gnet.Conn) {
	logrus.Infof("sub: [%s] subscribe [%s]", conn.RemoteAddr().String(), channel)

	t.fixCh(channel)

	t.lock.RLock()
	defer t.lock.RUnlock()

	ch := t.chmap[channel]
	err := ch.AddSubscriber(channel, conn)
	if err != nil {
		logrus.Errorf("AddSubscriber failed (%s)\n", err.Error())
	}
}

func (t *Topic) Publish(msg *pb.Message, conn gnet.Conn) error {
	t.lock.RLock()
	defer t.lock.RUnlock()

	logrus.Infof("pub: [%s] publish channel [%s] with content [%s]", conn.RemoteAddr().String(), msg.Topic, msg.Content)

	if ch, ok := t.chmap[msg.Topic]; ok {
		err := ch.Publish(conn, &pb.Publish{
			Channel: msg.Topic,
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
