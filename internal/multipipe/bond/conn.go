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
)

type tubeLike interface {
	Send(ctx context.Context, data []byte) error
	Receive(ctx context.Context) ([]byte, error)
	MaxMessageSize() int
}

type BondConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	sched      *scheduler
	maxPayload int

	sendSeq atomic.Uint32
	recvSeq uint32

	mu      sync.Mutex
	reorder map[uint32][]byte
	readCh  chan []byte
	readBuf []byte

	closeOnce sync.Once
	closed    chan struct{}
}

func NewBondConn(ctx context.Context, names []string, tubes []tubeLike) *BondConn {
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
		ctx:        c,
		cancel:     cancel,
		sched:      newScheduler(names, tubes),
		maxPayload: maxPayload,
		reorder:    make(map[uint32][]byte),
		readCh:     make(chan []byte, 512),
		closed:     make(chan struct{}),
	}
	for _, sf := range bc.sched.subflows {
		go bc.readLoop(sf)
	}
	return bc
}

func (bc *BondConn) MaxPayload() int { return bc.maxPayload }

func (bc *BondConn) readLoop(sf *subflow) {
	for {
		data, err := sf.tube.Receive(bc.ctx)
		if err != nil {
			return
		}
		seq, payload, err := decodeFrame(data)
		if err != nil {
			continue
		}
		bc.sched.SubInflight(sf, int64(len(payload)))

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

type writeJob struct {
	data []byte
	seq  uint32
}

func (bc *BondConn) Write(p []byte) (int, error) {
	select {
	case <-bc.closed:
		return 0, net.ErrClosed
	default:
	}

	if len(p) == 0 {
		return 0, nil
	}

	var jobs []writeJob
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > bc.maxPayload {
			n = bc.maxPayload
		}
		jobs = append(jobs, writeJob{
			data: p[:n],
			seq:  bc.sendSeq.Add(1) - 1,
		})
		total += n
		p = p[n:]
	}

	const maxParallel = 64
	sem := make(chan struct{}, maxParallel)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for _, j := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j writeJob) {
			defer wg.Done()
			defer func() { <-sem }()

			tries := len(bc.sched.subflows) + 2
			var lastErr error
			for attempt := 0; attempt < tries; attempt++ {
				sf := bc.sched.Pick()
				bc.sched.AddInflight(sf, int64(len(j.data)))

				start := time.Now()
				err := writeFrameToTube(sf.tube, j.seq, j.data)
				if err != nil {
					bc.sched.SubInflight(sf, int64(len(j.data)))
					bc.sched.MarkDead(sf, err)
					lastErr = err
					continue
				}
				bc.sched.UpdateRTT(sf, time.Since(start))
				bc.sched.MarkAlive(sf)
				return
			}

			errMu.Lock()
			if firstErr == nil {
				if lastErr == nil {
					lastErr = fmt.Errorf("bond: no alive subflows")
				}
				firstErr = lastErr
			}
			errMu.Unlock()
		}(j)
	}

	wg.Wait()
	if firstErr != nil {
		return total, firstErr
	}
	return total, nil
}

func writeFrameToTube(t tubeLike, seq uint32, data []byte) error {
	buf := make([]byte, frameHeaderSize+len(data))
	binary.BigEndian.PutUint32(buf[0:4], seq)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(data)))
	copy(buf[frameHeaderSize:], data)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return t.Send(ctx, buf)
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

func (bc *BondConn) Close() error {
	bc.closeOnce.Do(func() {
		close(bc.closed)
		bc.cancel()
	})
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
