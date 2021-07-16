package decoder

import (
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

	// 处理请求 以及 数据流中剩下的部分
	m.buf = dataBuf[l-1:]
	return dataBuf[:l], nil
}

func lengthOfMessage(buf []byte) int {
	if len(buf) < lengthByte {
		return -1
	}

	lenBuf := buf[:lengthByte]
	return int(uint32(lenBuf[0]) | uint32(lenBuf[1])<<8 | uint32(lenBuf[2])<<16 | uint32(lenBuf[3])<<24)
}
