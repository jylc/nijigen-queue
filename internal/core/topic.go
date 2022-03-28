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
	closeChan chan bool
	msgCount  int32
	nq        *NQ
}

func GetTopic(topicName string, nq *NQ) {
	var topic *Topic
	var ok bool
	if topic, ok = nq.topicMap[topicName]; !ok {
		topic = NewTopic(topicName, nq)
		nq.topicMap[topicName] = topic
		logrus.Infof("TOPIC(%s):Created", topicName)
	}
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
					go func(c *Channel) {
						err := c.Publish(msg)
						if err != nil {
							logrus.Errorf("TOPIC(%s)-Channel(%s): publish message failed,messag:(%s)", t.name, c.name, msg.Content)
						}
					}(ch)
				}
			} else {
				err := t.GetChannel(msg.Channel).Publish(msg)
				if err != nil {
					logrus.Errorf("TOPIC(%s)-Channel(%s): publish message failed,messag:(%s)", t.name, msg.Channel, msg.Content)
					return
				}
			}
		case <-t.closeChan:
			return
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
	t.chmap[channel] = NewChannel(channel, t.nq)
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
	t.lock.Lock()
	t.msgChan <- msg
	atomic.AddInt32(&t.msgCount, 1)
	t.lock.Unlock()
	return nil
}

func (t *Topic) Close() error {
	for _, channel := range t.chmap {
		if err := channel.Close(); err != nil {
			logrus.Error(err)
		}
	}
	return nil
}
