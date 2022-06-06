package message

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/internal/pb"
)

const (
	ReceiveType byte = 1
)

const (
	OptionSub string = "SUB"
	OptionPub string = "PUB"
	OptionAck string = "Ack"
	OptionReq string = "Req"

	OptionNotify string = "NOTIFY"
	OptionAlive  string = "KEEP ALIVE"
)

// server
const ()

type MetaMessage struct {
	Topic    string
	Channel  string
	Content  string
	Option   string
	Latency  time.Duration //延迟时间，在设置的时间发送消息
	Timeout  time.Duration //超时时间，超过这个时间消息会重发
	Deadline time.Timer    //过期时间，超过这个时间消息直接丢弃
}

func NewNQMetaMessage(request *pb.RequestProtobuf) *MetaMessage {
	return &MetaMessage{
		Topic:   request.Topic,
		Channel: request.Channel,
		Content: request.Content,
		Option:  request.Option,
		Timeout: time.Duration(request.Timeout) * time.Millisecond,
	}
}

func getLenBuf(msgBuf []byte) []byte {
	lenBuf := make([]byte, 4, 5+len(msgBuf))
	binary.BigEndian.PutUint32(lenBuf, uint32(len(msgBuf)))
	return lenBuf
}

func BuildMessage(msg proto.Message) (buf []byte, err error) {
	msgBuf, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	lenBuf := getLenBuf(msgBuf)
	return append(lenBuf, msgBuf...), nil
}

func CheckMessage(msgBuf []byte) ([]byte, error) {
	lenBuf := binary.BigEndian.Uint32(msgBuf[:4])
	if lenBuf != uint32(len(msgBuf[4:])) {
		return nil, errors.New("message has lost some bytes")
	}
	return msgBuf[4:], nil
}
