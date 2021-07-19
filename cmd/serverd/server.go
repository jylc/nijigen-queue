package main

import (
	"errors"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/queue"

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
	onError := func(err error) {
		logrus.Error(err)
		out = []byte(err.Error())
		action = gnet.Close
	}

	ctx := c.Context()
	if ctx != nil {
		if err, ok := ctx.(error); ok {
			onError(err)
			return
		}
	}

	// handle the request
	msg := &pb.Message{}
	if err := proto.NewBuffer(frame).Unmarshal(msg); err != nil {
		onError(err)
		return
	}

	if res, err := s.q.Handle(msg, c); err != nil {
		onError(err)
		return
	} else {
		out = res
		action = gnet.None
		return
	}
}

func (s *Server) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	logrus.Infof("client [%s] connected", c.RemoteAddr())
	return
}

func (s *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	if errors.Is(err, io.EOF) {
		logrus.Infof("client [%s] disconnected", c.RemoteAddr())
		return
	}

	if err != nil {
		logrus.Infof("client [%s] disconnected and error occurd on close: %v", c.RemoteAddr(), err)
	}
	return
}

func (s *Server) Tick() (delay time.Duration, action gnet.Action) {
	delay = 1 * time.Second
	return
}
