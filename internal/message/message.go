package message

import (
	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/internal/pb"
)

// client
const (
	ReceiveType byte = 1
)

// server
const (
	OperationSub string = "sub"
	OperationPub string = "pub"
)

type MetaMessage struct {
	Topic   string
	Channel string
	Content string
}

func NewNQMetaMessage(request *pb.RequestProtobuf) *MetaMessage {
	return &MetaMessage{
		Topic:   request.Topic,
		Channel: request.Channel,
		Content: request.Content,
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
