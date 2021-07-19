package decoder

import (
	"encoding/binary"

	"github.com/panjf2000/gnet"
)

const (
	lengthByte = 4
)

type MessageDecoder struct {
	buf []byte
}

func (m *MessageDecoder) Encode(c gnet.Conn, buf []byte) ([]byte, error) {
	if c.Context() == nil {
		return buf, nil
	}

	// TODO handle error
	return buf, nil
}

func (m *MessageDecoder) Decode(c gnet.Conn) ([]byte, error) {
	buf := c.Read()
	c.ResetBuffer()

	dataBuf := append(m.buf, buf...)
	l := lengthOfMessage(dataBuf)
	if l == -1 {
		return nil, nil
	} else if l > len(dataBuf) {
		// 请求未准备好
		m.buf = dataBuf
		return nil, nil
	}

	if l+lengthByte == len(dataBuf) {
		// 没有剩余
		m.buf = m.buf[0:0]
		return dataBuf[lengthByte:], nil
	}

	// 有剩余
	m.buf = dataBuf[l+lengthByte:]
	return dataBuf[lengthByte : lengthByte+l], nil
}

func lengthOfMessage(buf []byte) int {
	if len(buf) < lengthByte {
		return -1
	}

	lenBuf := buf[:lengthByte]
	return int(binary.BigEndian.Uint32(lenBuf))
}
