package httpstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	mrand "math/rand/v2"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	frameHeaderSize = 52
	frameVersion    = byte(1)
)

var frameMagic = [4]byte{'H', 'S', 'T', '1'}

type frameKind byte

const (
	frameData       frameKind = 1
	frameKeepAlive  frameKind = 2
	frameClose      frameKind = 4
	frameEpochReset frameKind = 8
)

func (k frameKind) valid() bool {
	switch k {
	case frameData, frameKeepAlive, frameClose, frameEpochReset:
		return true
	default:
		return false
	}
}

type decodedFrame struct {
	Kind      frameKind
	Epoch     uint32
	Seq       uint64
	Ack       uint64
	SessionID [16]byte
	Payload   []byte
	Messages  [][]byte
}

func encodeFrame(kind frameKind, sid [16]byte, epoch uint32, seq, ack uint64, messages [][]byte, padLen int) []byte {
	if padLen < 0 {
		padLen = 0
	}
	payloadSize := 0
	for _, msg := range messages {
		payloadSize += 4 + len(msg)
	}

	total := frameHeaderSize + payloadSize + padLen
	out := make([]byte, total)
	copy(out[0:4], frameMagic[:])
	out[4] = frameVersion
	out[5] = byte(kind)
	binary.BigEndian.PutUint16(out[6:8], 0)
	binary.BigEndian.PutUint32(out[8:12], epoch)
	binary.BigEndian.PutUint64(out[12:20], seq)
	binary.BigEndian.PutUint64(out[20:28], ack)
	binary.BigEndian.PutUint32(out[28:32], uint32(payloadSize))
	binary.BigEndian.PutUint32(out[32:36], uint32(padLen))
	copy(out[36:52], sid[:])
	offset := frameHeaderSize
	var hdr [4]byte
	for _, msg := range messages {
		binary.BigEndian.PutUint32(hdr[:], uint32(len(msg)))
		offset += copy(out[offset:], hdr[:])
		offset += copy(out[offset:], msg)
	}
	if padLen > 0 {
		fillRandom(out[offset:])
	}
	return out
}

func decodeFrame(data []byte, maxSize int) (decodedFrame, error) {
	var out decodedFrame
	if len(data) < frameHeaderSize {
		return out, errors.New("httpstream: short frame")
	}
	if data[0] != frameMagic[0] || data[1] != frameMagic[1] ||
		data[2] != frameMagic[2] || data[3] != frameMagic[3] {
		return out, errors.New("httpstream: bad frame magic")
	}
	if data[4] != frameVersion {
		return out, fmt.Errorf("httpstream: unsupported frame version %d", data[4])
	}
	kind := frameKind(data[5])
	if !kind.valid() {
		return out, fmt.Errorf("httpstream: unsupported frame kind %d", data[5])
	}
	payloadLen := int(binary.BigEndian.Uint32(data[28:32]))
	padLen := int(binary.BigEndian.Uint32(data[32:36]))
	if payloadLen < 0 || padLen < 0 {
		return out, errors.New("httpstream: negative frame length")
	}
	total := frameHeaderSize + payloadLen + padLen
	if maxSize > 0 && total > maxSize {
		return out, fmt.Errorf("httpstream: frame too large (%d > %d)", total, maxSize)
	}
	if len(data) < total {
		return out, io.ErrUnexpectedEOF
	}

	out.Kind = kind
	out.Epoch = binary.BigEndian.Uint32(data[8:12])
	out.Seq = binary.BigEndian.Uint64(data[12:20])
	out.Ack = binary.BigEndian.Uint64(data[20:28])
	copy(out.SessionID[:], data[36:52])

	if kind == frameData {
		payload := data[frameHeaderSize : frameHeaderSize+payloadLen]
		msgs, err := decodeMessages(payload)
		if err != nil {
			return out, err
		}

		out.Payload = payload
		out.Messages = msgs
	}
	return out, nil
}

func readFrame(r io.Reader, maxSize int) (decodedFrame, int, error) {
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return decodedFrame{}, 0, err
	}
	if hdr[0] != frameMagic[0] || hdr[1] != frameMagic[1] ||
		hdr[2] != frameMagic[2] || hdr[3] != frameMagic[3] {
		return decodedFrame{}, 0, errors.New("httpstream: bad frame magic")
	}
	payloadLen := int(binary.BigEndian.Uint32(hdr[28:32]))
	padLen := int(binary.BigEndian.Uint32(hdr[32:36]))
	if payloadLen < 0 || padLen < 0 {
		return decodedFrame{}, 0, errors.New("httpstream: negative frame length")
	}
	total := frameHeaderSize + payloadLen + padLen
	if maxSize > 0 && total > maxSize {
		return decodedFrame{}, 0, fmt.Errorf("httpstream: frame too large (%d > %d)", total, maxSize)
	}
	buf := make([]byte, total)
	copy(buf, hdr[:])
	if _, err := io.ReadFull(r, buf[frameHeaderSize:]); err != nil {
		return decodedFrame{}, 0, err
	}
	frame, err := decodeFrame(buf, maxSize)
	if err != nil {
		return decodedFrame{}, 0, err
	}
	return frame, total, nil
}

func decodeMessages(payload []byte) ([][]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var messages [][]byte
	offset := 0
	for offset < len(payload) {
		if offset+4 > len(payload) {
			return nil, io.ErrUnexpectedEOF
		}
		size := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if size <= 0 || offset+size > len(payload) {
			return nil, errors.New("httpstream: corrupt message batch")
		}
		messages = append(messages, payload[offset:offset+size])
		offset += size
	}
	return messages, nil
}

func fillRandom(b []byte) {
	for len(b) >= 8 {
		binary.LittleEndian.PutUint64(b, mrand.Uint64())
		b = b[8:]
	}
	if len(b) > 0 {
		v := mrand.Uint64()
		for i := range b {
			b[i] = byte(v)
			v >>= 8
		}
	}
}

type ReorderingBuffer struct {
	mu sync.Mutex

	timeout   time.Duration
	maxFrames int
	maxBytes  int

	epoch    uint32
	expected uint64
	pending  map[uint64][]byte
	bytes    int
	gapSince time.Time
}

func newReorderingBuffer(timeout time.Duration, maxFrames, maxBytes int) *ReorderingBuffer {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if maxFrames <= 0 {
		maxFrames = 4096
	}
	if maxBytes <= 0 {
		maxBytes = 16 << 20
	}
	return &ReorderingBuffer{
		timeout:   timeout,
		maxFrames: maxFrames,
		maxBytes:  maxBytes,
		expected:  1,
		pending:   make(map[uint64][]byte),
	}
}

func (r *ReorderingBuffer) Push(epoch uint32, seq uint64, payload []byte) ([][]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if seq == 0 {
		return nil, errors.New("httpstream: uplink seq must start at 1")
	}
	if r.epoch == 0 {
		r.epoch = epoch
	}
	if epoch < r.epoch {
		return nil, nil
	}
	if epoch > r.epoch {
		r.resetLocked(epoch)
	}

	if seq < r.expected {
		return nil, nil
	}
	if seq > r.expected {
		if _, exists := r.pending[seq]; exists {
			return nil, nil
		}
		if len(r.pending)+1 > r.maxFrames || r.bytes+len(payload) > r.maxBytes {
			return nil, fmt.Errorf("%w: httpstream: uplink reorder buffer overflow", transport.ErrTransportReset)
		}
		cp := append([]byte(nil), payload...)
		r.pending[seq] = cp
		r.bytes += len(cp)
		if r.gapSince.IsZero() {
			r.gapSince = time.Now()
		}
		return nil, nil
	}

	ready := [][]byte{append([]byte(nil), payload...)}
	r.expected++
	for {
		next, ok := r.pending[r.expected]
		if !ok {
			break
		}
		delete(r.pending, r.expected)
		r.bytes -= len(next)
		ready = append(ready, next)
		r.expected++
	}
	if len(r.pending) == 0 {
		r.gapSince = time.Time{}
	} else {
		r.gapSince = time.Now()
	}
	return ready, nil
}

func (r *ReorderingBuffer) CheckDeadline(now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) == 0 || r.gapSince.IsZero() {
		return nil
	}
	if now.Sub(r.gapSince) > r.timeout {
		return fmt.Errorf("%w: httpstream: uplink reorder timeout", transport.ErrTransportReset)
	}
	return nil
}

func (r *ReorderingBuffer) Delivered() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.expected == 0 {
		return 0
	}
	return r.expected - 1
}

func (r *ReorderingBuffer) Reset(epoch uint32) {
	r.mu.Lock()
	r.resetLocked(epoch)
	r.mu.Unlock()
}

func (r *ReorderingBuffer) Resume(epoch uint32, delivered uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.epoch = epoch
	r.expected = delivered + 1
	if r.expected == 0 {
		r.expected = 1
	}
	r.pending = make(map[uint64][]byte)
	r.bytes = 0
	r.gapSince = time.Time{}
}

func (r *ReorderingBuffer) resetLocked(epoch uint32) {
	r.epoch = epoch
	r.expected = 1
	r.pending = make(map[uint64][]byte)
	r.bytes = 0
	r.gapSince = time.Time{}
}
