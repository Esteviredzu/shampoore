package bond

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/logging"
)

type tubeLike interface {
	Send(ctx context.Context, data []byte) error
	Receive(ctx context.Context) ([]byte, error)
	MaxMessageSize() int
}

const (
	writeTimeout    = 5 * time.Second
	samplePeriod    = 2 * time.Second
	enqueueDeadline = 30 * time.Second
	enqueueWaitTick = 200 * time.Millisecond
)

type queuedChunk struct {
	seq  uint32
	data []byte
}

type BondConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	sched      *scheduler
	maxPayload int
	logger     logging.ContextLogger

	sendSeq atomic.Uint32
	recvSeq uint32

	readySignal chan struct{}

	mu      sync.Mutex
	reorder map[uint32][]byte
	readCh  chan []byte
	readBuf []byte

	closeOnce sync.Once
	closed    chan struct{}
	wg        sync.WaitGroup
}

func NewBondConn(
	ctx context.Context,
	names []string,
	tubes []tubeLike,
	logger logging.ContextLogger,
) *BondConn {
	c, cancel := context.WithCancel(ctx)

	maxPayload := 65535
	for _, t := range tubes {
		if m := t.MaxMessageSize(); m > 200 && m-100 < maxPayload {
			maxPayload = m - 100
		}
	}
	if maxPayload < 1024 {
		maxPayload = 1024
	}
	if maxPayload > 60000 {
		maxPayload = 60000
	}

	bc := &BondConn{
		ctx:         c,
		cancel:      cancel,
		sched:       newScheduler(names, tubes),
		maxPayload:  maxPayload,
		logger:      logger,
		readySignal: make(chan struct{}, 1),
		reorder:     make(map[uint32][]byte),
		readCh:      make(chan []byte, 512),
		closed:      make(chan struct{}),
	}

	for _, sf := range bc.sched.subflows {
		bc.wg.Add(1)
		go bc.readLoop(sf)
		bc.wg.Add(1)
		go bc.workerLoop(sf)
	}
	bc.wg.Add(1)
	go bc.statsLoop()

	return bc
}

func (bc *BondConn) MaxPayload() int { return bc.maxPayload }

func (bc *BondConn) workerLoop(sf *subflow) {
	defer bc.wg.Done()
	defer close(sf.workerDone)

	for {
		select {
		case <-bc.closed:
			return
		case chunk := <-sf.queue:
			select {
			case bc.readySignal <- struct{}{}:
			default:
			}
			bc.sendChunk(sf, chunk)
		}
	}
}

func (bc *BondConn) sendChunk(sf *subflow, chunk queuedChunk) {
	buf := make([]byte, frameHeaderSize+len(chunk.data))
	binary.BigEndian.PutUint32(buf[0:4], chunk.seq)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(chunk.data)))
	copy(buf[frameHeaderSize:], chunk.data)

	ctx, cancel := context.WithTimeout(bc.ctx, writeTimeout)
	err := sf.tube.Send(ctx, buf)
	cancel()
	if err == nil {
		sf.sentBytes.Add(int64(len(chunk.data)))
		bc.sched.MarkAlive(sf)
		return
	}

	bc.sched.MarkDead(sf, err)
	if bc.logger != nil {
		bc.logger.Warn("bond: send on ", sf.name, " failed: ", err)
	}

	alt := bc.sched.pickNonFull(sf)
	if alt == nil {
		if bc.logger != nil {
			bc.logger.Warn("bond: chunk seq=", chunk.seq,
				" lost (no other alive path)")
		}
		return
	}
	ctx2, cancel2 := context.WithTimeout(bc.ctx, writeTimeout)
	err2 := alt.tube.Send(ctx2, buf)
	cancel2()
	if err2 != nil {
		bc.sched.MarkDead(alt, err2)
		if bc.logger != nil {
			bc.logger.Warn("bond: chunk seq=", chunk.seq,
				" lost (retry on ", alt.name, " failed: ", err2, ")")
		}
		return
	}
	alt.sentBytes.Add(int64(len(chunk.data)))
	bc.sched.MarkAlive(alt)
}

func (bc *BondConn) readLoop(sf *subflow) {
	defer bc.wg.Done()
	for {
		data, err := sf.tube.Receive(bc.ctx)
		if err != nil {
			return
		}
		seq, payload, err := decodeFrame(data)
		if err != nil {
			continue
		}

		bc.mu.Lock()
		if seq < bc.recvSeq {
			bc.mu.Unlock()
			continue
		}
		if _, exists := bc.reorder[seq]; exists {
			bc.mu.Unlock()
			continue
		}
		bc.reorder[seq] = payload
		for {
			next, ok := bc.reorder[bc.recvSeq]
			if !ok {
				break
			}
			delete(bc.reorder, bc.recvSeq)
			bc.recvSeq++
			select {
			case bc.readCh <- next:
			case <-bc.closed:
				bc.mu.Unlock()
				return
			}
		}
		bc.mu.Unlock()
	}
}

func decodeFrame(data []byte) (uint32, []byte, error) {
	if len(data) < frameHeaderSize {
		return 0, nil, io.ErrUnexpectedEOF
	}
	seq := binary.BigEndian.Uint32(data[0:4])
	n := binary.BigEndian.Uint32(data[4:8])
	if int(n) > len(data)-frameHeaderSize {
		return 0, nil, io.ErrUnexpectedEOF
	}
	return seq, data[frameHeaderSize : frameHeaderSize+int(n)], nil
}

func (bc *BondConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	select {
	case <-bc.closed:
		return 0, net.ErrClosed
	default:
	}

	total := 0
	deadline := time.Now().Add(enqueueDeadline)

	for len(p) > 0 {
		n := len(p)
		if n > bc.maxPayload {
			n = bc.maxPayload
		}
		chunk := queuedChunk{
			seq:  bc.sendSeq.Add(1) - 1,
			data: make([]byte, n),
		}
		copy(chunk.data, p[:n])
		p = p[n:]

		if err := bc.enqueue(chunk, deadline); err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (bc *BondConn) enqueue(chunk queuedChunk, deadline time.Time) error {
	for {
		select {
		case <-bc.closed:
			return net.ErrClosed
		default:
		}

		if sf := bc.sched.pickNonFull(nil); sf != nil {
			select {
			case sf.queue <- chunk:
				return nil
			default:
				continue
			}
		}

		if bc.sched.allDead() {
			return fmt.Errorf("bond: all subflows dead")
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("bond: enqueue timeout (all queues full)")
		}

		select {
		case <-bc.closed:
			return net.ErrClosed
		case <-bc.readySignal:
		case <-time.After(enqueueWaitTick):
		}
	}
}

func (bc *BondConn) Read(p []byte) (int, error) {
	return bc.ReadContext(bc.ctx, p)
}

func (bc *BondConn) ReadContext(ctx context.Context, p []byte) (int, error) {
	if len(bc.readBuf) > 0 {
		n := copy(p, bc.readBuf)
		bc.readBuf = bc.readBuf[n:]
		return n, nil
	}
	select {
	case data, ok := <-bc.readCh:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, data)
		if n < len(data) {
			bc.readBuf = data[n:]
		}
		return n, nil
	case <-bc.closed:
		return 0, net.ErrClosed
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (bc *BondConn) statsLoop() {
	defer bc.wg.Done()
	ticker := time.NewTicker(samplePeriod)
	defer ticker.Stop()

	prev := make(map[string]int64)
	for {
		select {
		case <-bc.closed:
			return
		case <-ticker.C:
		}
		if bc.logger == nil {
			continue
		}
		snaps := bc.sched.Snapshot()
		parts := make([]string, 0, len(snaps))
		for _, s := range snaps {
			rate := (s.SentBytes - prev[s.Name]) / int64(samplePeriod/time.Second)
			prev[s.Name] = s.SentBytes
			tag := ""
			if s.Dead {
				tag = " DEAD"
			}
			parts = append(parts, fmt.Sprintf(
				"%s{q=%d/%d rate=%dB/s%s}",
				s.Name, s.QueueLen, s.QueueCap, rate, tag,
			))
		}
		bc.logger.Info("bond[stats] ", joinStrings(parts, " "))
	}
}

func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += sep + p
	}
	return out
}

func (bc *BondConn) Close() error {
	bc.closeOnce.Do(func() {
		drainDeadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(drainDeadline) {
			empty := true
			for _, sf := range bc.sched.subflows {
				if len(sf.queue) > 0 {
					empty = false
					break
				}
			}
			if empty {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		close(bc.closed)
		bc.cancel()
	})
	bc.wg.Wait()
	return nil
}

func (bc *BondConn) LocalAddr() net.Addr                { return dummyAddr("bond") }
func (bc *BondConn) RemoteAddr() net.Addr               { return dummyAddr("bond") }
func (bc *BondConn) SetDeadline(t time.Time) error      { return nil }
func (bc *BondConn) SetReadDeadline(t time.Time) error  { return nil }
func (bc *BondConn) SetWriteDeadline(t time.Time) error { return nil }

type dummyAddr string

func (d dummyAddr) Network() string { return string(d) }
func (d dummyAddr) String() string  { return string(d) }

var _ net.Conn = (*BondConn)(nil)
