package network

import (
	"github.com/jylc/nijigen-queue/internal/core"
	"github.com/panjf2000/gnet"
)

const (
	connInit = iota
	connUnEstablish
	connEstablished
	connClosed
)

type NQConn struct {
	ID int64
	gnet.Conn
	state     int32
	nq        *core.NQ
	FrameChan chan []byte
	Close     chan bool
}

func NewNQConn(nq *core.NQ, conn gnet.Conn, serialNum int64) *NQConn {
	return &NQConn{
		ID:        serialNum,
		Conn:      conn,
		nq:        nq,
		FrameChan: make(chan []byte, 10),
		Close:     make(chan bool),
		state:     connInit,
	}
}

func (c *NQConn) Rect(FrameChan chan []byte, Close chan bool) {
	for {
		select {
		case frame := <-FrameChan:
			_, _ = c.nq.Handle(frame, c)
		case <-Close:
			_ = c.Close
			c.state = connClosed
			return
		default:
		}
	}
}
