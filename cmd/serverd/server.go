package main

import (
	"errors"
	"github.com/jylc/nijigen-queue/tools"
	"sync/atomic"

	"github.com/jylc/nijigen-queue/internal/core"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

type Server struct {
	*gnet.EventServer
	nq           *core.NQ
	connected    int64
	disconnected int64
	waitGroup    tools.WaitGroupWrapper
}

func (s *Server) OnInitComplete(srv gnet.Server) (action gnet.Action) {
	logrus.Infof("Nijigen Queue is listening on %s (multi-cores: %t, loops: %d)",
		srv.Addr.String(), srv.Multicore, srv.NumEventLoop)
	return
}

func (s *Server) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	ctx := c.Context()
	if ctx != nil {
		if err, ok := ctx.(error); ok {
			s.onError(err)
			return
		}
	} else {
		return
	}

	nqConn, ok := ctx.(*core.NQConn)
	if ok {
		logrus.Infof("server:nqConn:%d recv request", nqConn.ID)
		nqConn.Handle(frame)
	} else {
		err := errors.New("server:cannot get connection nqConn")
		out, action = s.onError(err)
	}
	c.SetContext(nqConn)
	return
}

func (s *Server) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	atomic.AddInt64(&s.connected, 1)
	nqConn := core.NewNQConn(s.nq, c, s.connected)
	logrus.Infof("server:nqConn:%d [%s] opened", nqConn.ID, nqConn.RemoteAddr())
	c.SetContext(nqConn)
	go nqConn.React()
	return
}

func (s *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddInt64(&s.disconnected, 1)
	ctx := c.Context()
	if ctx != nil {
		var ok bool
		if err, ok = ctx.(error); ok {
			_, action = s.onError(err)
			return
		}
	}

	nqConn, ok := ctx.(*core.NQConn)
	if ok {
		nqConn.Release()
		logrus.Infof("server:nqConn:%d [%s] closed", nqConn.ID, nqConn.RemoteAddr())
	}
	return
}

func (s *Server) onError(err error) (out []byte, action gnet.Action) {
	logrus.Error(err)
	out = []byte(err.Error())
	return
}
