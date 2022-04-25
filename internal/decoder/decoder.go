package decoder

import (
	"encoding/binary"
	"errors"

	"github.com/panjf2000/gnet"
)

const (
	lengthByte = 4
	typeByte   = 0
)

var (
	ErrUnexpectedEOF = errors.New("there is no enough data")
)

type innerBuffer []byte

type MessageDecoder struct{}

func (m *MessageDecoder) Encode(c gnet.Conn, buf []byte) ([]byte, error) {
	return buf, nil
}

func (m *MessageDecoder) Decode(c gnet.Conn) ([]byte, error) {
	var (
		in     innerBuffer
		lenBuf []byte
		err    error
	)
	in = c.Read()
	lenBuf, err = in.readLen()
	if err != nil {
		return nil, err
	}

	msgLen := int(binary.BigEndian.Uint32(lenBuf))
	if len(in) < lengthByte+typeByte+msgLen {
		return nil, ErrUnexpectedEOF
	}

	fullMessage := make([]byte, typeByte+msgLen)
	copy(fullMessage, in[lengthByte:lengthByte+typeByte+msgLen])

	c.ShiftN(lengthByte + typeByte + msgLen)
	return fullMessage, nil
}

func (ib *innerBuffer) readLen() ([]byte, error) {
	if len(*ib) < lengthByte {
		return nil, ErrUnexpectedEOF
	}

	return (*ib)[:lengthByte], nil
}
