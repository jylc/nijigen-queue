package core

import (
	"errors"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/configs"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/goroutine"
	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
)

var (
	ErrOp = errors.New("invalid operation")

	pool = goroutine.Default()

	okbytes = []byte("OK")
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

func (nq *NQ) Handle(frame []byte, conn gnet.Conn) ([]byte, error) {
	request := &pb.RequestProtobuf{}
	if err := proto.Unmarshal(frame, request); err != nil {
		return nil, err
	}
	switch request.Option {
	case message.OperationSub:
		msg := message.NewNQMetaMessage(request)
		if err := nq.GetTopic(msg.Topic).Subscribe(msg.Channel, conn); err != nil {
			return nil, err
		}
		return okbytes, nil
	case message.OperationPub:
		msg := message.NewNQMetaMessage(request)
		if err := nq.GetTopic(msg.Topic).Publish(msg); err != nil {
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
	nq.topicMap[topic] = NewTopic(topic, nq)
	nq.lock.Unlock()

	logrus.Infof("TOPIC(%s): created", topic)

	return nq.GetTopic(topic)
}

func (nq *NQ) GetOpts() *configs.Options {
	return nq.opts
}
