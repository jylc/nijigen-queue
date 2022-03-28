package core

import (
	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/jylc/nijigen-queue/internal/pb"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
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
	connClosed
)

var (
	okbytes = []byte("OK")
)

type ClientError struct {
	errStr string
}

func (e ClientError) Error() string {
	return e.errStr
}

var (
	clientErrInvalid = ClientError{"ERR_CLIENT_INVALID"}
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
}

func NewNQConn(nq *NQ, conn gnet.Conn, serialNum int64) *NQConn {
	return &NQConn{
		ID:        serialNum,
		Conn:      conn,
		nq:        nq,
		FrameChan: make(chan []byte, 10),
		CloseChan: make(chan bool),
		clientRdy: make(chan bool),
		state:     connInit,
	}
}

// Rect 路由转发
func (c *NQConn) Rect(FrameChan chan []byte, CloseChan chan bool) {
	defer func(c *NQConn) {
		err := c.CloseConn()
		if err != nil {
			logrus.Error(err)
		}
	}(c)
	go c.notifyConsumer()
	for {
		select {
		case frame := <-FrameChan:
			request := &pb.RequestProtobuf{}
			if err := proto.Unmarshal(frame, request); err != nil {
				logrus.Error(err)
				return
			}
			msg := message.NewNQMetaMessage(request)

			switch request.Option {
			case message.OperationSub:
				if err := c.CheckState(message.OperationSub); err != nil {
					logrus.Error(err)
					return
				}
				topic := c.nq.GetTopic(msg.Topic)
				c.channel = topic.GetChannel(msg.Channel)
				c.channelName = msg.Channel
				c.topicName = msg.Topic

				err := c.AsyncWrite(okbytes)
				if err != nil {
					return
				}
				c.ticker = time.NewTicker(5 * time.Second)
				c.SetState(message.OperationSub)
			case message.OperationPub:
				// 发布消息
				if err := c.CheckState(message.OperationPub); err != nil {
					logrus.Error(err)
					return
				}
				if err := c.nq.GetTopic(msg.Topic).Publish(msg); err != nil {
					logrus.Error(err)
					return
				}
				c.SetState(message.OperationPub)
			case message.OperationRdy:
				if err := c.CheckState(message.OperationRdy); err != nil {
					logrus.Error(err)
					return
				}
				c.pushMessage()
				c.SetState(message.OperationRdy)
			case message.OperationAck:
				if err := c.CheckState(message.OperationAck); err != nil {
					logrus.Error(err)
					return
				}
				c.SetState(message.OperationAck)
			case message.OperationGet:
			case message.OperationReq:
			case message.OperationFin:
				if err := c.CheckState(message.OperationFin); err != nil {
					logrus.Error(err)
					return
				}
				c.SetState(message.OperationFin)
			}
		case <-CloseChan:
			_ = c.CloseConn()
			c.state = connClosed
			return
		default:
		}
	}
}

func (c *NQConn) CheckState(cmd string) error {
	switch cmd {
	case message.OperationSub:
		if c.state != connInit {
			return clientErrInvalid
		}
	case message.OperationPub:
		if c.state != connInit {
			return clientErrInvalid
		}
	case message.OperationGet:
		if c.state != connWaitGet {
			return clientErrInvalid
		}
	case message.OperationAck:
		if c.state != connWaitAck {
			return clientErrInvalid
		}
	case message.OperationRdy:
		if c.state != connWaitRdy {
			return clientErrInvalid
		}
	case message.OperationFin:
		if c.state != connWaitFin {
			return clientErrInvalid
		}
	case message.OperationReq:
		if c.state != connWaitReq {
			return clientErrInvalid
		}
	}
	return nil
}

func (c *NQConn) SetState(cmd string) {
	switch cmd {
	case message.OperationSub:
		c.state = connWaitRdy
	case message.OperationPub:
		c.state = connWaitRdy
	case message.OperationGet:
		c.state = connWaitAck
	case message.OperationAck:
		c.state = connWaitFin
	case message.OperationRdy:
		c.state = connWaitAck
	case message.OperationFin:
		c.state = connWaitRdy
	case message.OperationReq:
		c.state = connWaitPub
	}
}

func (c *NQConn) CloseConn() error {
	logrus.Infof("CLIENT(%s):closing", c.Conn.RemoteAddr())
	close(c.CloseChan)
	close(c.FrameChan)
	if c.channel != nil {
		c.channel.RemoveSubscriber(c)
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
		select {
		case <-c.ticker.C:
			logrus.Println("NQConn: notify consumer")
			data, err := message.BuildMessage(&pb.ResponseProtobuf{
				Topic:   c.topicName,
				Channel: c.channelName,
				Content: "QUERY",
				Option:  "QUERY",
			})
			if err != nil {
				return
			}
			err = c.AsyncWrite(data)
			if err != nil {
				return
			}
		default:
		}
	}
}

func (c *NQConn) pushMessage() {
	msg := c.channel.GetMsg()
	if err := c.AsyncWrite(msg); err != nil {
		logrus.Error(err)
	}
}
