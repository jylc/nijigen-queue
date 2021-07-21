package decoder

import (
	"encoding/binary"

	"github.com/panjf2000/gnet"
)

const (
	lengthByte = 4
	typeByte   = 1
)

type MessageDecoder struct {
	buf []byte
}

func (m *MessageDecoder) Encode(c gnet.Conn, buf []byte) ([]byte, error) {
	return buf, nil
}

func (m *MessageDecoder) Decode(c gnet.Conn) ([]byte, error) {
	buf := c.Read()
	c.ResetBuffer()

	dataBuf := append(m.buf, buf...)
	curBufferLen := len(dataBuf)
	msgLen := m.lengthOfMessage(dataBuf)
	totalLen := lengthByte + typeByte + msgLen // [0,0,0,0,0,...]

	if msgLen == -1 {
		m.buf = dataBuf
		return nil, nil
	} else if totalLen > curBufferLen {
		// 请求未准备好
		m.buf = dataBuf
		return nil, nil
	} else if totalLen == curBufferLen {
		// 没有剩余
		m.buf = m.buf[0:0]
		return dataBuf[lengthByte:], nil
	} else { // msgLen+lengthByte+typeByte < curBufferLen
		// 有剩余
		m.buf = dataBuf[totalLen:]
		return dataBuf[lengthByte:totalLen], nil
	}
}

func (m *MessageDecoder) lengthOfMessage(buf []byte) int {
	if len(buf) < lengthByte {
		return -1
	}

	return int(binary.BigEndian.Uint32(buf[:lengthByte]))
}
