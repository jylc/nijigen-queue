package core

import (
	"sync"

	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/builder"
	"github.com/jylc/nijigen-queue/internal/pb"
)

type Channel struct {
	name string
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

func NewChannel(channel string) *Channel {
	return &Channel{
		name: channel,
		m:    make(map[string]gnet.Conn),
	}
}

func (c *Channel) Publish(conn gnet.Conn, content string) error {
	buf, err := builder.MessageReceive(&pb.PublicResponse{Content: content})
	if err != nil {
		return err
	}

	err = pool.Submit(func() {
		if err = conn.AsyncWrite(buf); err != nil {
			logrus.Errorf("CHANNEL(%s) write message [%s] to [%s] error: %v", c.name, content, conn.RemoteAddr(), err)
		}
	})
	if err != nil {
		return err
	}

	return nil
}
