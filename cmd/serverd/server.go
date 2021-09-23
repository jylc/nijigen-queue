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
	connSerialNum int32
	connAliveNum  int32
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

	client, ok := ctx.(*network.NQConn)
	if ok && len(frame) > 0 {
		client.FrameChan <- frame
	} else {
		client.Close <- true
		close(client.FrameChan)
		close(client.Close)
		err := errors.New("cannot get connection client")
		onError(err)
	}
	c.SetContext(client)
	return
}

func (s *Server) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	logrus.Infof("client [%s] connected", c.RemoteAddr())
	atomic.AddInt32(&s.connAliveNum, 1)
	atomic.AddInt32(&s.connSerialNum, 1)
	client := network.NewNQConn(s.nq, c, s.connSerialNum)
	c.SetContext(client)
	go client.Rect(client.FrameChan, client.Close)
	return
}

func (s *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddInt32(&s.connAliveNum, -1)
	if errors.Is(err, io.EOF) {
		logrus.Infof("client [%s] disconnected", c.RemoteAddr())
		return
	}

	if err != nil {
		logrus.Infof("client [%s] disconnected and error occurd on close: %v", c.RemoteAddr(), err)
	}
	return
}
