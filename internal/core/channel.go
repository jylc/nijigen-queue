package core

import (
	"sync"

	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
)

type Channel struct {
	name        string
	subscribers map[string]gnet.Conn
	lock        sync.RWMutex
}

func (c *Channel) AddSubscriber(conn gnet.Conn) error {
	// TODO 添加订阅者时判断是否超出最大范围
	key := conn.RemoteAddr().String()
	c.lock.RLock()
	_, ok := c.subscribers[key]
	c.lock.RUnlock()

	if ok {
		return nil
	}

	c.lock.Lock()
	_, ok = c.subscribers[key]
	if ok {
		c.lock.Unlock()
		return nil
	}

	c.subscribers[key] = conn
	c.lock.Unlock()

	return nil
}

func NewChannel(channel string) *Channel {
	return &Channel{
		name:        channel,
		subscribers: make(map[string]gnet.Conn),
	}
}

func (c *Channel) Publish(content string) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for _, conn := range c.subscribers {
		remoteAddr := conn.RemoteAddr()
		if remoteAddr == nil { // TODO 删除策略
			continue
		}
		err := pool.Submit(func() {
			_ = c.publish(conn, remoteAddr.String(), content)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Channel) publish(conn gnet.Conn, remoteAddr string, content string) error {
	if conn == nil {
		return nil
	}

	buf, err := message.BuildReceiveMessage(&pb.PublicResponse{Content: content})
	if err != nil {
		return err
	}

	if err := conn.AsyncWrite(buf); err != nil {
		logrus.Errorf("CHANNEL(%s) write message [%s] to [%s] error: %v", c.name, content, remoteAddr, err)
	}

	return nil
}
