package core

import (
	"sync"
	"time"

	"github.com/jylc/nijigen-queue/internal/queue"
	"github.com/jylc/nijigen-queue/tools"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
)

const (
	MaxPriorityQueueCapacity = 20
	MinPriorityQueueCapacity = 10
	Latency                  = 100 * time.Millisecond
	SlotNum                  = 300
	Capacity                 = 100
)

type Channel struct {
	nq *NQ

	name        string
	subscribers map[string]gnet.Conn
	lock        sync.RWMutex

	msgChan  chan *message.MetaMessage
	sendChan chan interface{}

	waitGroup tools.WaitGroupWrapper

	//TODO 延迟消息队列
	tw        *queue.TimeWheel
	deferChan chan bool
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

func NewChannel(channel string, nq *NQ) *Channel {
	sendChan := make(chan interface{})
	ch := &Channel{
		name:        channel,
		subscribers: make(map[string]gnet.Conn),
		nq:          nq,
		deferChan:   make(chan bool),
		msgChan:     make(chan *message.MetaMessage, nq.GetOpts().MaxMessageNum),
		sendChan:    sendChan,
	}
	ch.tw, _ = queue.NewNQTimeWheel(Latency, SlotNum, Capacity, sendChan)

	go ch.messagePump()
	return ch
}

func (c *Channel) messagePump() {
	for {
		var msg *message.MetaMessage
		select {
		case value := <-c.sendChan:
			item := value.(*queue.Item)
			msg = item.Value.(*message.MetaMessage)
		case value := <-c.msgChan:
			msg = value
		}
		_ = c.publish(msg)
	}
}

func (c *Channel) Publish(msg *message.MetaMessage) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if msg.Latency != 0 {
		c.tw.Set(msg.Latency, msg)
		return nil
	}
	c.msgChan <- msg
	return nil
}

func (c *Channel) publish(msg *message.MetaMessage) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, conn := range c.subscribers {
		remoteAddr := conn.RemoteAddr()
		if remoteAddr == nil { // TODO 删除策略
			continue
		}

		err := pool.Submit(func() {
			_ = c.sendMsg(conn, remoteAddr.String(), msg.Content)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Channel) sendMsg(conn gnet.Conn, remoteAddr string, content string) error {
	if conn == nil {
		return nil
	}

	buf, err := message.BuildMessage(&pb.ResponseProtobuf{Content: content})
	if err != nil {
		return err
	}

	if err := conn.AsyncWrite(buf); err != nil {
		logrus.Errorf("CHANNEL(%s) write message [%s] to [%s] error: %v", c.name, content, remoteAddr, err)
	}
	return nil
}
