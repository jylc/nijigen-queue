package core

import (
	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	"time"
)

const (
	connInit = iota
	connWaitAck
	connWaitRdy
	connWaitResponse
	connClosed
)

type ClientError struct {
	errStr string
}

func (e ClientError) Error() string {
	return e.errStr
}

var (
	clientErrSub = ClientError{"ERR_SUB_CLIENT_INVALID"}
	clientErrPub = ClientError{"ERR_PUB_CLIENT_INVALID"}
	clientErrReq = ClientError{"ERR_REQ_CLIENT_INVALID"}
	clientErrAck = ClientError{"ERR_ACK_CLIENT_INVALID"}
)

// NQConn 为每一个链接创建对应的NQ Client Connection
type NQConn struct {
	gnet.Conn
	ID          int64
	state       int32
	nq          *NQ
	channel     *Channel
	ticker      *time.Ticker
	topicName   string
	channelName string
	frameChan   chan []byte
	closeChan   chan bool
	clientRdy   chan bool
	heartbeats  int32
	once        sync.Once
}

func NewNQConn(nq *NQ, conn gnet.Conn, id int64) *NQConn {
	return &NQConn{
		ID:         id,
		Conn:       conn,
		nq:         nq,
		frameChan:  make(chan []byte, 10),
		closeChan:  make(chan bool),
		clientRdy:  make(chan bool),
		state:      connInit,
		ticker:     time.NewTicker(10 * time.Second),
		heartbeats: 0,
	}
}

// React 路由转发
func (c *NQConn) React() {
	for {
		select {
		case frame := <-c.frameChan:
			var err error
			request := &pb.RequestProtobuf{}
			if err = proto.Unmarshal(frame, request); err != nil {
				logrus.Error(err)
				close(c.closeChan)
			}
			logrus.Println("NQConn:message ", request)
			msg := message.NewNQMetaMessage(request)

			switch msg.Option {
			case message.OptionSub:
				if c.state != connInit {
					logrus.Error(clientErrSub)
					close(c.closeChan)
				}
				c.topicName = msg.Topic
				c.channelName = msg.Channel
				topic := c.nq.GetTopic(c.topicName)
				c.channel, err = topic.Subscribe(c.channelName, c)
				if err != nil {
					logrus.Error(err)
					close(c.closeChan)
				}
				c.once.Do(func() {
					go c.notifyConsumer()
				})
				c.state = connWaitResponse
			case message.OptionPub:
				if c.state != connInit {
					logrus.Error(clientErrPub)
					return
				}
				c.topicName = msg.Topic
				c.channelName = msg.Channel
				topic := c.nq.GetTopic(c.topicName)
				if err = topic.Publish(msg); err != nil {
					logrus.Error(err)
					return
				}
				if err != nil {
					logrus.Error(err)
					return
				}
				c.state = connInit
			case message.OptionReq:
				if c.state != connWaitResponse {
					logrus.Error(clientErrReq)
					close(c.closeChan)
				}
				c.pushMessage()
				c.state = connWaitAck
			case message.OptionAck:
				if c.state != connWaitAck {
					logrus.Error(clientErrAck)
					close(c.closeChan)
				}
				c.state = connWaitRdy
			case message.OptionAlive:
				atomic.StoreInt32(&c.heartbeats, 0)
				logrus.Println(msg.Content)
			}
		case <-c.closeChan:
			c.state = connClosed
			return
		default:
		}
	}
}

func (c *NQConn) close() error {
	logrus.Infof("NQConn:CLIENT(%s):closing", c.Conn.RemoteAddr())
	c.closeChan <- true
	if c.channel != nil {
		c.channel.RemoveSubscriber(c)
		c.channel = nil
	}
	return nil
}
func (c *NQConn) Release() {
	err := c.close()
	if err != nil {
		return
	}
}

//notifyConsumer 当有消息进入队列需要发送时先通知消费者，获取消费者的状态，确定是否发送
func (c *NQConn) notifyConsumer() {
	for {
		var content string
		var option string
		select {
		case <-c.channel.Notify:
			logrus.Println("NQConn:notify consumer", c.RemoteAddr())
			content = "NOTIFY"
			option = message.OptionNotify
		case <-c.ticker.C:
			logrus.Println("NQConn:keep alive", c.RemoteAddr())
			content = "KEEP ALIVE"
			option = message.OptionAlive
			atomic.AddInt32(&c.heartbeats, 1)
			if atomic.LoadInt32(&c.heartbeats) == 3 {
				err := c.close()
				if err != nil {
					return
				}
				return
			}
		}
		data, err := message.BuildMessage(&pb.ResponseProtobuf{
			Topic:   c.topicName,
			Channel: c.channelName,
			Content: content,
			Option:  option,
		})
		if err != nil {
			logrus.Error(err)
			return
		}
		err = c.AsyncWrite(data)
		if err != nil {
			logrus.Error(err)
			return
		}
	}
}

func (c *NQConn) pushMessage() {
	msg := c.channel.GetMsg()
	if err := c.AsyncWrite(msg); err != nil {
		logrus.Error(err)
	}
}

func (c *NQConn) Handle(frame []byte) {
	c.frameChan <- frame
}
