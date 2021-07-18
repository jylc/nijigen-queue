package tools

import (
	"github.com/golang/protobuf/proto"
	pb2 "github.com/jylc/nijigen-queue/internal/pb"
)

func ParseToMessage(data []byte) (*pb2.Message, error) {
	var msg *pb2.Message
	err := proto.NewBuffer(data).Unmarshal(msg)
	return msg, err
}
