package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/jylc/nijigen-queue/internal/network"
	"github.com/jylc/nijigen-queue/internal/queue"
	"github.com/jylc/nijigen-queue/tools"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
)

const (
	Latency    = 100 * time.Millisecond
	SlotNum    = 300
	Capacity   = 100
	MessageNum = 20
)

type Channel struct {
	nq *NQ

	name        string
	subscribers map[int64]gnet.Conn
	lock        sync.RWMutex

	msgChan   chan *message.MetaMessage
	sendChan  chan interface{}
	closeChan chan interface{}
	testChan  chan interface{}

	waitGroup tools.WaitGroupWrapper

	tw        *queue.TimeWheel //时间轮存放具有延迟时间的消息
	deferChan chan bool
}

func (c *Channel) AddSubscriber(conn gnet.Conn) error {
	// TODO 添加订阅者时判断是否超出最大范围
	key := conn.(*network.NQConn).ID
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
		subscribers: make(map[int64]gnet.Conn),
		nq:          nq,
		deferChan:   make(chan bool),
		msgChan:     make(chan *message.MetaMessage, MessageNum),
		sendChan:    sendChan,
		closeChan:   make(chan interface{}),
		testChan:    make(chan interface{}),
	}
	var err error
	ch.tw, err = queue.NewNQTimeWheel(Latency, SlotNum, Capacity, sendChan)
	if err != nil {
		_ = fmt.Errorf("create channel %s failed", channel)
		return nil
	}
	go ch.messagePump()
	return ch
}

func (c *Channel) messagePump() {
	for {
		var msg *message.MetaMessage
		select {
		case value := <-c.sendChan:
			if item, ok := value.(*queue.Item); ok {
				msg = item.Value.(*message.MetaMessage)
			}
		case value := <-c.msgChan:
			msg = value
		case <-c.closeChan:
			c.tw.Stop()
			return
		}
		//c.testChan <- msg
		if err := c.publish(msg); err != nil {
			logrus.Error(err)
			close(c.closeChan)
		}
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
			delete(c.subscribers, conn.(*network.NQConn).ID)
			if len(c.subscribers) == 0 {
				c.subscribers = nil
			}
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
