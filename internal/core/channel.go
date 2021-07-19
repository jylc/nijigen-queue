package core

import (
	"sync"

	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	"github.com/jylc/nijigen-queue/internal/pb"
)

type Channel struct {
	m    map[string]gnet.Conn
	lock sync.RWMutex
}

func (c *Channel) AddSubscriber(channel string, conn gnet.Conn) error {
	// TODO 添加订阅者时判断是否超出最大范围
	c.lock.RLock()
	defer c.lock.RUnlock()
	if _, ok := c.m[channel]; !ok {
		c.m[channel] = conn
	}
	return nil
}

func NewChannel() *Channel {
	return &Channel{
		m: make(map[string]gnet.Conn),
	}
}

func (c *Channel) Publish(conn gnet.Conn, pub *pb.Publish) error {
	msg, err := proto.Marshal(pub)
	if err != nil {
		return err
	}

	err = pool.Submit(func() {
		if err = conn.AsyncWrite(msg); err != nil {
			logrus.Errorf("channel [%s] write message [%s] to [%s] error: %v", pub.Channel, pub.Content, conn.RemoteAddr(), err)
		}
	})
	if err != nil {
		return err
	}

	return nil
}
