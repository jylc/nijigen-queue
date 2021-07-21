package builder

import (
	"encoding/binary"

	"github.com/golang/protobuf/proto"
)

const (
	MessageReceiveType byte = iota + 1
)

func MessageReceive(msg proto.Message) ([]byte, error) {
	msgBuf, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	lenBuf := getLenBuf(msgBuf)
	lenBuf = append(lenBuf, MessageReceiveType)
	return append(lenBuf, msgBuf...), nil
}

func getLenBuf(msgBuf []byte) []byte {
	lenBuf := make([]byte, 4, 5+len(msgBuf))
	binary.BigEndian.PutUint32(lenBuf, uint32(len(msgBuf)))
	return lenBuf
}
