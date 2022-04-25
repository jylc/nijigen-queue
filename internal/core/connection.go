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
	connWaitPub
	connWaitGet
	connWaitAck
	connWaitFin
	connWaitReq
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
	clientErrInvalid = ClientError{"ERR_CLIENT_INVALID"}
	clientErrSub     = ClientError{"ERR_SUB_CLIENT_INVALID"}
	clientErrPub     = ClientError{"ERR_PUB_CLIENT_INVALID"}
	clientErrRdy     = ClientError{"ERR_RDY_CLIENT_INVALID"}
	clientErrReq     = ClientError{"ERR_REQ_CLIENT_INVALID"}
	clientErrAck     = ClientError{"ERR_ACK_CLIENT_INVALID"}
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
	FrameChan   chan []byte
	CloseChan   chan bool
	clientRdy   chan bool
	heartbeats  int32
	once        sync.Once
}

func NewNQConn(nq *NQ, conn gnet.Conn, id int64) *NQConn {
	return &NQConn{
		ID:         id,
		Conn:       conn,
		nq:         nq,
		FrameChan:  make(chan []byte, 10),
		CloseChan:  make(chan bool),
		clientRdy:  make(chan bool),
		state:      connInit,
		ticker:     time.NewTicker(10 * time.Second),
		heartbeats: 0,
	}
}

// Rect 路由转发
func (c *NQConn) Rect(frameChan chan []byte, closeChan chan bool) {
	defer func(c *NQConn) {
		err := c.closeConn()
		if err != nil {
			logrus.Error(err)
		}
	}(c)

	for {
		select {
		case frame := <-frameChan:
			var err error
			request := &pb.RequestProtobuf{}
			if err = proto.Unmarshal(frame, request); err != nil {
				logrus.Error(err)
				return
			}
			//logrus.Println("NQConn:message bytes ", frame)
			logrus.Println("NQConn:message ", request)
			msg := message.NewNQMetaMessage(request)

			switch msg.Option {
			case message.OptionSub:
				if c.state != connInit {
					logrus.Error(clientErrSub)
					return
				}
				c.topicName = msg.Topic
				c.channelName = msg.Channel
				topic := c.nq.GetTopic(c.topicName)
				c.channel, err = topic.Subscribe(c.channelName, c)
				if err != nil {
					logrus.Error(err)
					return
				}
				c.once.Do(func() {
					go c.notifyConsumer()
				})
				c.state = connWaitRdy
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
			case message.OptionRdy:
				if c.state != connWaitRdy {
					logrus.Error(clientErrRdy)
					return
				}
				atomic.StoreInt32(&c.heartbeats, 0)

				c.state = connWaitResponse
			case message.OptionReq:
				if c.state != connWaitResponse {
					logrus.Error(clientErrReq)
					return
				}
				c.pushMessage()
				c.state = connWaitAck
			case message.OptionAck:
				if c.state != connWaitAck {
					logrus.Error(clientErrAck)
					return
				}
				c.state = connWaitRdy
			}
		case <-closeChan:
			c.state = connClosed
			return
		default:
		}
	}
}

func (c *NQConn) setState(cmd string) {
}

func (c *NQConn) closeConn() error {
	logrus.Infof("NQConn:CLIENT(%s):closing", c.Conn.RemoteAddr())
	if c.channel != nil {
		c.channel.RemoveSubscriber(c)
		c.channel = nil
	}
	err := c.Close()
	if err != nil {
		return err
	}
	return nil
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
				close(c.CloseChan)
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
