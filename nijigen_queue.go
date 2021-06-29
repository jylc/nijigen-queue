package main

import (
	"errors"
	"net"

	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/pb"
)

type Queue struct {
	chmap map[string][]net.IP
}

func NewQueue() *Queue {
	return &Queue{}
}

func (q *Queue) Publish(req *pb.Request) error {
	if q.chmap == nil {
		q.chmap = make(map[string][]net.IP)
	}

	if ips, ok := q.chmap[req.Channel]; ok {
		for _, ip := range ips {
			if err := q.publish(ip, req.Message); err != nil {
				// TODO 错误处理
				panic(err)
			}
		}
	} else {
		// TODO 保存下来，等有订阅者的时候再 push
		return errors.New("no subscriber")
	}

	return nil
}

func (q *Queue) publish(ip net.IP, message *pb.Message) error {
	var conn net.Conn
	var err error

	if conn, err = net.Dial("tcp", ip.String()); err != nil {
		return err
	}
	defer conn.Close()

	msg, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	if _, err = conn.Write(msg); err != nil {
		return err
	}

	return nil
}
