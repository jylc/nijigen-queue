package core

import (
	"errors"
	"sync"

	"github.com/jylc/nijigen-queue/configs"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/goroutine"
)

var (
	ErrOp = errors.New("invalid operation")

	pool = goroutine.Default()
)

type NQ struct {
	topicMap map[string]*Topic
	lock     sync.RWMutex
	opts     *configs.Options
}

func NewNQ() *NQ {
	return &NQ{
		topicMap: make(map[string]*Topic),
		opts:     configs.NewOptions(),
	}
}

//PushMessage nq向消费者推送消息
func (nq *NQ) PushMessage() {

}
func (nq *NQ) GetTopic(topic string) *Topic {
	nq.lock.RLock()
	t, ok := nq.topicMap[topic]
	nq.lock.RUnlock()
	if ok {
		return t
	}

	nq.lock.Lock()
	t, ok = nq.topicMap[topic]
	if ok {
		nq.lock.Unlock()
		return t
	}
	nq.topicMap[topic] = NewTopic(topic, nq)
	nq.lock.Unlock()

	logrus.Infof("TOPIC(%s): created", topic)

	return nq.GetTopic(topic)
}

func (nq *NQ) GetOpts() *configs.Options {
	return nq.opts
}

func (nq *NQ) Close() {
	for _, topic := range nq.topicMap {
		err := topic.Close()
		if err != nil {
			logrus.Error(err)
		}
	}
}
