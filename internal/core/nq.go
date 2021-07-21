package core

import (
	"sync"

	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/goroutine"
	"github.com/jylc/nijigen-queue/internal/pb"
)

var (
	pool = goroutine.Default()
)

type NQ struct {
	topicMap map[string]*Topic
	lock     sync.RWMutex
}

func NewNQ() *NQ {
	return &NQ{
		topicMap: make(map[string]*Topic),
	}
}

func (nq *NQ) Handle(msg *pb.Message, conn gnet.Conn) ([]byte, error) {
	topic := nq.GetTopic(msg.Topic)
	switch msg.Operation {
	case OperationSub:
		topic.Subscribe(msg.Topic, conn)
		return okbytes, nil
	case OperationPub:
		if err := topic.Publish(msg, conn); err != nil {
			if err == ErrNoSub {
				logrus.Warnf("publish occurd error: %v", err)
				return nil, nil
			}
			return nil, err
		}
		return okbytes, nil
	default:
		return nil, ErrOp
	}
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
	nq.topicMap[topic] = NewTopic()
	nq.lock.Unlock()

	logrus.Infof("TOPIC(%s): created", topic)

	return nq.GetTopic(topic)
}
