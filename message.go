package main

import (
	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/pb"
)

func ParseToMessage(data []byte) (*pb.Message, error) {
	var msg *pb.Message
	err := proto.NewBuffer(data).Unmarshal(msg)
	return msg, err
}
