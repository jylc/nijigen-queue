package core

import (
	"sync"
	"sync/atomic"

	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/tools"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

type Topic struct {
	name      string
	chmap     map[string]*Channel
	lock      sync.RWMutex
	waitGroup tools.WaitGroupWrapper
	msgChan   chan *message.MetaMessage
	msgCount  int32
	nq        *NQ
}

func NewTopic(topic string, nq *NQ) *Topic {
	t := &Topic{
		name:     topic,
		chmap:    make(map[string]*Channel),
		msgChan:  make(chan *message.MetaMessage, nq.GetOpts().MaxMessageNum),
		nq:       nq,
		msgCount: 0,
	}
	t.waitGroup.Wait(t.messagePump)
	return t
}

func (t *Topic) messagePump() {
	for {
		select {
		case msg := <-t.msgChan:
			if msg.Channel == "" {
				for _, ch := range t.chmap {
					err := ch.Publish(msg.Content)
					if err != nil {
						logrus.Errorf("TOPIC(%s)-Channel(%s): publish message failed,messag:(%s)", t.name, ch.name, msg.Content)
						continue
					}
				}
			} else {
				err := t.GetChannel(msg.Channel).Publish(msg.Content)
				if err != nil {
					logrus.Errorf("TOPIC(%s)-Channel(%s): publish message failed,messag:(%s)", t.name, msg.Channel, msg.Content)
					return
				}
			}
		}
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
	err := ch.AddSubscriber(conn)
	if err != nil {
		return err
	}
	return nil
}

func (t *Topic) Publish(msg *message.MetaMessage) error {
	select {
	case t.msgChan <- msg:
		atomic.AddInt32(&t.msgCount, 1)
	}
	return nil
}
