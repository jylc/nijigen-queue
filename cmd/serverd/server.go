package main

import (
	"errors"
	"io"
	"sync/atomic"

	"github.com/jylc/nijigen-queue/internal/core"
	"github.com/jylc/nijigen-queue/internal/network"
	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"
)

type Server struct {
	*gnet.EventServer
	nq            *core.NQ
	connSerialNum int64
	connAliveNum  int64
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

	nqConn, ok := ctx.(*network.NQConn)
	if ok && len(frame) > 0 {
		nqConn.FrameChan <- frame
	} else {
		nqConn.Close <- true
		close(nqConn.FrameChan)
		close(nqConn.Close)
		err := errors.New("cannot get connection nqConn")
		onError(err)
	}
	c.SetContext(nqConn)
	return
}

func (s *Server) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	logrus.Infof("nqConn [%s] connected", c.RemoteAddr())
	atomic.AddInt64(&s.connAliveNum, 1)
	atomic.AddInt64(&s.connSerialNum, 1)
	nqConn := network.NewNQConn(s.nq, c, s.connSerialNum)
	c.SetContext(nqConn)
	go nqConn.Rect(nqConn.FrameChan, nqConn.Close)
	return
}

func (s *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddInt64(&s.connAliveNum, -1)
	if errors.Is(err, io.EOF) {
		logrus.Infof("client [%s] disconnected", c.RemoteAddr())
		return
	}

	if err != nil {
		logrus.Infof("client [%s] disconnected and error occurd on close: %v", c.RemoteAddr(), err)
	}
	return
}
