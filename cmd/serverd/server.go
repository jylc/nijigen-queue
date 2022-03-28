package main

import (
	"errors"
	"io"
	"sync/atomic"

	"github.com/jylc/nijigen-queue/internal/core"
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

	nqConn, ok := ctx.(*core.NQConn)
	if ok && len(frame) > 0 {
		logrus.Infof("NQ CONNECT ID:%d", nqConn.ID)
		nqConn.FrameChan <- frame
	} else {
		nqConn.CloseChan <- true
		close(nqConn.FrameChan)
		close(nqConn.CloseChan)
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
	nqConn := core.NewNQConn(s.nq, c, s.connSerialNum)
	c.SetContext(nqConn)
	go nqConn.Rect(nqConn.FrameChan, nqConn.CloseChan)
	return
}

func (s *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddInt64(&s.connAliveNum, -1)
	if s.connAliveNum == 0 {
		atomic.StoreInt64(&s.connSerialNum, 0)
		s.nq.Close()
	}
	ctx := c.Context()
	if ctx != nil {
		if err, ok := ctx.(error); ok {
			logrus.Error(err)
			return
		}
	}
	nqConn, ok := c.(*core.NQConn)
	if !ok {
		logrus.Error("cannot get nq")
	}

	if errors.Is(err, io.EOF) {
		logrus.Infof("client [%s] disconnected", c.RemoteAddr())
		return
	}
	err = nqConn.Close()
	if err != nil {
		logrus.Infof("client [%s] disconnected and error occurd on close: %v", c.RemoteAddr(), err)
	}
	return
}
