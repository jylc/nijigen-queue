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
	name  string
	chmap map[string]*Channel
	lock  sync.RWMutex
}

func NewTopic(topic string) *Topic {
	return &Topic{
		name:  topic,
		chmap: make(map[string]*Channel),
	}
}

func (t *Topic) GetChannel(channel string) *Channel {
	t.lock.RLock()
	ch, ok := t.chmap[channel]
	t.lock.RUnlock()

	if ok {
		return ch
	}

	t.lock.Lock()
	ch, ok = t.chmap[channel]
	if ok {
		t.lock.Unlock()
		return ch
	}
	t.chmap[channel] = NewChannel(channel)
	t.lock.Unlock()

	logrus.Infof("TOPIC(%s)-Channel(%s): created", t.name, channel)

	return t.GetChannel(channel)
}

func (t *Topic) Subscribe(channel string, conn gnet.Conn) error {
	logrus.Infof("sub: [%s] subscribe TOPIC(%s)-CHANNEL(%s)", conn.RemoteAddr().String(), t.name, channel)

	ch := t.GetChannel(channel)
	err := ch.AddSubscriber(channel, conn)
	if err != nil {
		return err
	}
	return nil
}

func (t *Topic) Publish(msg *pb.Message, conn gnet.Conn) error {
	logrus.Infof("pub: [%s] publish TOPIC(%s) with content [%s]", conn.RemoteAddr().String(), t.name, msg.Content)

	for _, ch := range t.chmap {
		err := ch.Publish(conn, &pb.Publish{
			Channel: msg.Topic,
			Content: msg.Content,
		})
		if err != nil {
			logrus.Errorf("TOPIC(%s) publish to CHANNEL(%s) error: %v", t.name, ch.name, err)
		}
	}

	return nil
}
