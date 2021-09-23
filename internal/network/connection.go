package network

import (
	"github.com/jylc/nijigen-queue/internal/core"
	"github.com/panjf2000/gnet"
)

type NQConn struct {
	connSerialNum int32
	conn          gnet.Conn
	nq            *core.NQ
	FrameChan     chan []byte
	Close         chan bool
}

func NewNQConn(nq *core.NQ, conn gnet.Conn, serialNum int32) *NQConn {
	return &NQConn{
		connSerialNum: serialNum,
		conn:          conn,
		nq:            nq,
		FrameChan:     make(chan []byte, 10),
		Close:         make(chan bool),
	}
}

func (c *NQConn) Rect(FrameChan chan []byte, Close chan bool) {
	for {
		select {
		case frame := <-FrameChan:
			_, _ = c.nq.Handle(frame, c.conn)
		case <-Close:
			_ = c.conn.Close()
			return
		default:
		}
	}
}
