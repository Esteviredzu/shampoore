package multi

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	frameMagic0  = 'M'
	frameMagic1  = 'P'
	frameMagic2  = '1'
	frameVersion = 1
	typeData     = 1

	frameHeaderSize = 3 + 1 + 1 + 8 + 4
	maxPayloadLen   = 16 * 1024 * 1024
)

func encodeFrame(seq uint64, payload []byte) ([]byte, error) {
	if len(payload) > maxPayloadLen {
		return nil, fmt.Errorf("multi: payload too large")
	}
	out := make([]byte, frameHeaderSize+len(payload))
	out[0] = frameMagic0
	out[1] = frameMagic1
	out[2] = frameMagic2
	out[3] = frameVersion
	out[4] = typeData
	binary.BigEndian.PutUint64(out[5:13], seq)
	binary.BigEndian.PutUint32(out[13:17], uint32(len(payload)))
	copy(out[frameHeaderSize:], payload)
	return out, nil
}

func decodeFrame(buf []byte) (uint64, []byte, error) {
	if len(buf) < frameHeaderSize {
		return 0, nil, errors.New("multi: short frame")
	}
	if buf[0] != frameMagic0 || buf[1] != frameMagic1 || buf[2] != frameMagic2 {
		return 0, nil, errors.New("multi: bad magic")
	}
	if buf[3] != frameVersion {
		return 0, nil, errors.New("multi: bad version")
	}
	if buf[4] != typeData {
		return 0, nil, errors.New("multi: bad type")
	}
	seq := binary.BigEndian.Uint64(buf[5:13])
	n := binary.BigEndian.Uint32(buf[13:17])
	if int(n) != len(buf)-frameHeaderSize {
		return 0, nil, errors.New("multi: length mismatch")
	}
	return seq, buf[frameHeaderSize:], nil
}