package main

import (
	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/internal/queue"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/pb"
)

type Server struct {
	*gnet.EventServer

	q *queue.Queue
}

func (s *Server) OnInitComplete(srv gnet.Server) (action gnet.Action) {
	logrus.Infof("Nijigen Queue is listening on %s (multi-cores: %t, loops: %d)",
		srv.Addr.String(), srv.Multicore, srv.NumEventLoop)
	return
}

func (s *Server) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	onError := func(err []byte) {
		out = err
		action = gnet.Close
	}

	if c.Context() != nil {
		onError([]byte("err"))
		return
	}

	// handle the request
	msg := &pb.Message{}
	if err := proto.NewBuffer(frame).Unmarshal(msg); err != nil {
		onError([]byte(err.Error()))
		return
	}

	if res, err := s.q.Handle(msg, c.RemoteAddr()); err != nil {
		onError([]byte(err.Error()))
		return
	} else {
		out = res
		action = gnet.None
		return
	}
}
