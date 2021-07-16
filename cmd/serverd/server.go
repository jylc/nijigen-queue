package main

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/jylc/nijigen-queue/internal/queue"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/pb"
)

type Server struct {
	*gnet.EventServer
	q            *queue.Queue
	lastConnTime map[gnet.Conn]time.Time
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
	s.lastConnTime[c] = time.Now()

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

func (s *Server) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	logrus.Infof("server connection is opened")
	s.lastConnTime[c] = time.Now()
	return
}

func (s *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	logrus.Infof("server connection is closed")
	err = c.Close()
	if err != nil {
		logrus.Infof("close server connection failed")
		action = gnet.Close
	}
	delete(s.lastConnTime, c)
	return
}

func (s *Server) Tick() (delay time.Duration, action gnet.Action) {
	now := time.Now()
	//连接超时设置
	for conn, lastTime := range s.lastConnTime {
		if now.Sub(lastTime) > 60*time.Second {
			err := conn.Close()
			if err != nil {
				logrus.Infof("close server connection failed")
				action = gnet.Close
			}
		}
	}
	delay = 1 * time.Second
	return
}
