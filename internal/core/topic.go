package core

import (
	"sync"

	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/pb"
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

func (t *Topic) Publish(msg *pb.PublicRequest, conn gnet.Conn) error {
	if msg.Channel == "" {
		logrus.Infof("pub: [%s] publish TOPIC(%s) with content [%s]", conn.RemoteAddr().String(), t.name, msg.Content)

		t.lock.RLock()
		defer t.lock.RUnlock()

		// 不指定 channel 的时候给每个 channel 都发消息
		for _, ch := range t.chmap {
			err := ch.Publish(conn, msg.Content)
			if err != nil {
				logrus.Errorf("TOPIC(%s) publish to CHANNEL(%s) error: %v", t.name, ch.name, err)
			}
		}
	} else {
		logrus.Infof("pub: [%s] publish TOPIC(%s) with content [%s]", conn.RemoteAddr().String(), t.name, msg.Content)
		if err := t.GetChannel(msg.Channel).Publish(conn, msg.Content); err != nil {
			return err
		}
	}

	return nil
}
