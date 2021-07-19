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
	if _, ok := nq.topicMap[msg.Channel]; !ok {
		nq.lock.Lock()
		if _, ok = nq.topicMap[msg.Channel]; !ok {
			nq.topicMap[msg.Channel] = NewTopic()
		}
		nq.lock.Unlock()
	}

	nq.lock.RLock()
	defer nq.lock.RUnlock()

	topic := nq.topicMap[msg.Channel] // TODO 消息增加 topic 字段
	switch msg.Operation {
	case OperationSub:
		topic.Subscribe(msg.Channel, conn)
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
