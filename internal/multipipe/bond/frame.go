package bond

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	frameHeaderSize = 20
	maxFramePayload = 64 * 1024

	seqAck = ^uint32(0)

	controlMagic = "BP01"

	controlPing byte = 1

	controlNack byte = 2

	maxNackSeqs = 32
)

type bondFrame struct {
	seq        uint32
	ack        uint32
	ackSession uint32
	session    uint32
	payload    []byte
}

func (f bondFrame) isControl() bool { return f.seq == seqAck }

func encodeFrame(seq, ack, ackSession, session uint32, data []byte) ([]byte, error) {
	if len(data) > maxFramePayload {
		return nil, fmt.Errorf("bond: frame too large: %d", len(data))
	}
	buf := make([]byte, frameHeaderSize+len(data))
	binary.BigEndian.PutUint32(buf[0:4], seq)
	binary.BigEndian.PutUint32(buf[4:8], ack)
	binary.BigEndian.PutUint32(buf[8:12], ackSession)
	binary.BigEndian.PutUint32(buf[12:16], session)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(data)))
	copy(buf[frameHeaderSize:], data)
	return buf, nil
}

func encodeControlFrame(kind byte, session, ack, ackSession uint32, args []uint32) []byte {
	payloadLen := len(controlMagic) + 1 + 4*len(args)
	buf := make([]byte, frameHeaderSize+payloadLen)
	binary.BigEndian.PutUint32(buf[0:4], seqAck)
	binary.BigEndian.PutUint32(buf[4:8], ack)
	binary.BigEndian.PutUint32(buf[8:12], ackSession)
	binary.BigEndian.PutUint32(buf[12:16], session)
	binary.BigEndian.PutUint32(buf[16:20], uint32(payloadLen))
	off := frameHeaderSize
	copy(buf[off:], controlMagic)
	off += len(controlMagic)
	buf[off] = kind
	off++
	for _, arg := range args {
		binary.BigEndian.PutUint32(buf[off:off+4], arg)
		off += 4
	}
	return buf
}

func decodeControl(payload []byte) (byte, []uint32, bool) {
	if len(payload) < len(controlMagic)+1 {
		return 0, nil, false
	}
	if string(payload[:len(controlMagic)]) != controlMagic {
		return 0, nil, false
	}
	kind := payload[len(controlMagic)]
	rest := payload[len(controlMagic)+1:]
	if len(rest)%4 != 0 {
		return 0, nil, false
	}
	args := make([]uint32, 0, len(rest)/4)
	for off := 0; off < len(rest); off += 4 {
		args = append(args, binary.BigEndian.Uint32(rest[off:off+4]))
	}
	return kind, args, true
}

func decodeFrame(data []byte) (bondFrame, error) {
	if len(data) < frameHeaderSize {
		return bondFrame{}, io.ErrUnexpectedEOF
	}
	f := bondFrame{
		seq:        binary.BigEndian.Uint32(data[0:4]),
		ack:        binary.BigEndian.Uint32(data[4:8]),
		ackSession: binary.BigEndian.Uint32(data[8:12]),
		session:    binary.BigEndian.Uint32(data[12:16]),
	}
	n := binary.BigEndian.Uint32(data[16:20])
	if int(n) > len(data)-frameHeaderSize {
		return bondFrame{}, io.ErrUnexpectedEOF
	}
	f.payload = data[frameHeaderSize : frameHeaderSize+int(n)]
	return f, nil
}
