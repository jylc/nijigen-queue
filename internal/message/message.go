package message

import (
	"encoding/binary"

	"github.com/golang/protobuf/proto"
)

// client
const (
	ReceiveType byte = 1
)

// server
const (
	OperationSub byte = 1
	OperationPub byte = 2
)

func BuildSubscribeRequest(msg proto.Message) ([]byte, error) {
	msgBuf, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	lenBuf := getLenBuf(msgBuf)
	lenBuf = append(lenBuf, OperationSub)
	return append(lenBuf, msgBuf...), nil
}

func BuildPublicRequest(msg proto.Message) ([]byte, error) {
	msgBuf, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	lenBuf := getLenBuf(msgBuf)
	lenBuf = append(lenBuf, OperationPub)
	return append(lenBuf, msgBuf...), nil
}

func BuildReceiveMessage(msg proto.Message) ([]byte, error) {
	msgBuf, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	lenBuf := getLenBuf(msgBuf)
	lenBuf = append(lenBuf, ReceiveType)
	return append(lenBuf, msgBuf...), nil
}

func getLenBuf(msgBuf []byte) []byte {
	lenBuf := make([]byte, 4, 5+len(msgBuf))
	binary.BigEndian.PutUint32(lenBuf, uint32(len(msgBuf)))
	return lenBuf
}
