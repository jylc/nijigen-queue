package queue

import (
	"errors"
	"net"
	"sync"

	"github.com/jylc/nijigen-queue/internal/pb"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

const (
	OperationSub = iota + 1
	OperationPub
)

var (
	ErrOp = errors.New("invalid operation")
)

type Queue struct {
	chmap         map[string][]gnet.Conn
	surplusMsgMap map[string][]*pb.Message
	lock          sync.RWMutex
}

func NewQueue() *Queue {
	return &Queue{chmap: make(map[string][]gnet.Conn), surplusMsgMap: make(map[string][]*pb.Message)}
}

func (q *Queue) Handle(msg *pb.Message, conn gnet.Conn) ([]byte, error) {
	switch msg.Operation {
	case OperationSub:
		q.Subscribe(msg.Channel, conn)
		return []byte("OK"), nil
	case OperationPub:
		if err := q.Publish(msg, conn); err != nil {
			return nil, err
		}

		return []byte("OK"), nil
	default:
		return nil, ErrOp
	}
}

func (q *Queue) Subscribe(channel string, conn gnet.Conn) {
	q.lock.Lock()
	defer q.lock.Unlock()

	logrus.Debugf("sub: [%s] subscribe [%s]", conn.RemoteAddr().String(), channel)

	if addrs, ok := q.chmap[channel]; ok {
		q.chmap[channel] = append(addrs, conn)
	} else {
		q.chmap[channel] = []gnet.Conn{conn}
		q.publishSurplusMsg(channel, conn)
	}
}

func (q *Queue) Publish(msg *pb.Message, conn gnet.Conn) error {
	q.lock.RLock()
	defer q.lock.RUnlock()

	logrus.Debugf("pub: [%s] publish channel [%s] with content [%s]", conn.RemoteAddr().String(), msg.Channel, msg.Content)

	if conns, ok := q.chmap[msg.Channel]; ok {
		for _, conn := range conns {
			if err := q.publish(conn, &pb.Publish{
				Channel: msg.Channel,
				Content: msg.Content,
			}); err != nil {
				// TODO 错误处理
				panic(err)
			}
		}
	} else {
		//按照channel存放msg
		q.surplusMsgMap[msg.Channel] = append(q.surplusMsgMap[msg.Channel], msg)
		return errors.New("no subscriber")
	}

	return nil
}

func (q *Queue) publish(conn gnet.Conn, pub *pb.Publish) error {
	msg, err := proto.Marshal(pub)
	if err != nil {
		return err
	}
	addr := conn.RemoteAddr()
	newConn, err := net.Dial(addr.Network(), addr.String())
	if err != nil {
		return err
	}
	if _, err = newConn.Write(msg); err != nil {
		return err
	}

	return nil
}

func (q *Queue) publishSurplusMsg(channel string, conn gnet.Conn) {
	//新建订阅后若存在之前的消息立即push
	if surplusMsg, ok := q.surplusMsgMap[channel]; ok {
		for _, msg := range surplusMsg {
			if err := q.publish(conn, &pb.Publish{
				Channel: channel,
				Content: msg.Content,
			}); err != nil {
				panic(err)
			}
		}
		delete(q.surplusMsgMap, channel)
	}
}
