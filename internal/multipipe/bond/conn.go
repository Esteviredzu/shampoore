package bond

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
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

	maxReorderChunks  = 1024
	defaultGapTimeout = 5 * time.Second
	minGapTick        = 10 * time.Millisecond

	maxConsecutiveResets = 8
)

var errSubflowReceiveStopped = errors.New("subflow receive stopped")

type queuedChunk struct {
	seq  uint32
	data []byte
}

type connOptions struct {
	maxReorder int
	gapTimeout time.Duration
}

func defaultConnOptions() connOptions {
	return connOptions{maxReorder: maxReorderChunks, gapTimeout: defaultGapTimeout}
}

type BondConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	sched      *scheduler
	maxPayload int
	logger     logging.ContextLogger

	maxReorder int
	gapTimeout time.Duration

	sendSeq atomic.Uint32
	recvSeq uint32

	readySignal chan struct{}

	mu       sync.Mutex
	reorder  map[uint32][]byte
	gapSince time.Time
	readCh   chan []byte
	readBuf  []byte

	readers atomic.Int32

	failOnce sync.Once
	failErr  error
	failed   chan struct{}
	readErr  chan error

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
	return newBondConn(ctx, names, tubes, logger, defaultConnOptions())
}

func newBondConn(
	ctx context.Context,
	names []string,
	tubes []tubeLike,
	logger logging.ContextLogger,
	opts connOptions,
) *BondConn {
	if opts.maxReorder <= 0 {
		opts.maxReorder = maxReorderChunks
	}
	if opts.gapTimeout <= 0 {
		opts.gapTimeout = defaultGapTimeout
	}

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
		maxReorder:  opts.maxReorder,
		gapTimeout:  opts.gapTimeout,
		readySignal: make(chan struct{}, 1),
		reorder:     make(map[uint32][]byte),
		readCh:      make(chan []byte, 512),
		failed:      make(chan struct{}),
		readErr:     make(chan error, 1),
		closed:      make(chan struct{}),
	}

	bc.readers.Store(int32(len(bc.sched.subflows)))
	for _, sf := range bc.sched.subflows {
		bc.wg.Add(1)
		go bc.readLoop(sf)
		bc.wg.Add(1)
		go bc.workerLoop(sf)
	}
	bc.wg.Add(1)
	go bc.statsLoop()
	bc.wg.Add(1)
	go bc.gapLoop()

	return bc
}

func (bc *BondConn) fail(reason error) {
	if reason == nil {
		return
	}
	bc.failOnce.Do(func() {
		if bc.logger != nil {
			bc.logger.Warn("bond: session reset: ", reason)
		}
		bc.failErr = fmt.Errorf("%w: %v", transport.ErrTransportReset, reason)
		select {
		case bc.readErr <- bc.failErr:
		default:
		}
		close(bc.failed)
		bc.cancel()
	})
}

func (bc *BondConn) terminalErr() error {
	select {
	case <-bc.failed:
		return bc.failErr
	default:
		return nil
	}
}

func (bc *BondConn) isClosed() bool {
	select {
	case <-bc.closed:
		return true
	default:
		return false
	}
}

func (bc *BondConn) MaxPayload() int { return bc.maxPayload }

func (bc *BondConn) workerLoop(sf *subflow) {
	defer bc.wg.Done()
	defer close(sf.workerDone)

	for {
		select {
		case <-bc.closed:
			return
		case <-bc.failed:
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

	tried := map[*subflow]struct{}{sf: {}}
	path := sf
	var lastErr error

	for {
		if bc.terminalErr() != nil || bc.isClosed() {
			return
		}

		ctx, cancel := context.WithTimeout(bc.ctx, writeTimeout)
		err := path.tube.Send(ctx, buf)
		cancel()
		if err == nil {
			path.sentBytes.Add(int64(len(chunk.data)))
			bc.sched.MarkAlive(path)
			return
		}

		lastErr = err
		bc.sched.MarkDead(path, err)
		if bc.logger != nil {
			bc.logger.Warn("bond: send on ", path.name, " failed: ", err)
		}

		alt := bc.sched.pickAlive(tried)
		if alt == nil {
			bc.fail(fmt.Errorf(
				"bond: chunk seq=%d lost: no alive subflow could deliver it (last error: %v)",
				chunk.seq, lastErr))
			return
		}
		tried[alt] = struct{}{}
		path = alt
	}
}

func (bc *BondConn) readLoop(sf *subflow) {
	defer bc.wg.Done()
	defer func() {

		bc.sched.MarkDead(sf, errSubflowReceiveStopped)
		if bc.readers.Add(-1) == 0 && bc.terminalErr() == nil && !bc.isClosed() {
			bc.fail(errors.New("bond: all subflows stopped receiving"))
		}
	}()

	resets := 0
	for {
		data, err := sf.tube.Receive(bc.ctx)
		if err != nil {
			if errors.Is(err, transport.ErrTransportReset) && bc.ctx.Err() == nil {

				if bc.logger != nil {
					bc.logger.Warn("bond: subflow ", sf.name, " transport reset: ", err)
				}
				resets++
				if resets > maxConsecutiveResets {

					bc.fail(fmt.Errorf("bond: subflow %s keeps resetting without delivering data", sf.name))
					return
				}
				continue
			}
			return
		}
		resets = 0

		seq, payload, err := decodeFrame(data)
		if err != nil {

			bc.fail(fmt.Errorf("bond: bad frame from %s: %w", sf.name, err))
			return
		}
		bc.deliver(seq, payload)
	}
}

func (bc *BondConn) deliver(seq uint32, payload []byte) {
	bc.mu.Lock()

	if seq < bc.recvSeq {
		bc.mu.Unlock()
		return
	}
	if _, exists := bc.reorder[seq]; exists {
		bc.mu.Unlock()
		return
	}
	if seq != bc.recvSeq && len(bc.reorder) >= bc.maxReorder {
		expected := bc.recvSeq
		bc.mu.Unlock()
		bc.fail(fmt.Errorf("bond: reorder buffer overflow at seq=%d (expected %d)", seq, expected))
		return
	}

	bc.reorder[seq] = payload
	if seq != bc.recvSeq && bc.gapSince.IsZero() {
		bc.gapSince = time.Now()
	}

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
		case <-bc.failed:
			bc.mu.Unlock()
			return
		}
	}

	if len(bc.reorder) == 0 {
		bc.gapSince = time.Time{}
	}
	bc.mu.Unlock()
}

func (bc *BondConn) gapLoop() {
	defer bc.wg.Done()

	tick := bc.gapTimeout / 4
	if tick < minGapTick {
		tick = minGapTick
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-bc.closed:
			return
		case <-bc.failed:
			return
		case <-ticker.C:
		}

		bc.mu.Lock()
		var (
			since    time.Time
			expected uint32
			buffered int
		)
		if len(bc.reorder) > 0 && !bc.gapSince.IsZero() {
			since = bc.gapSince
			expected = bc.recvSeq
			buffered = len(bc.reorder)
		}
		bc.mu.Unlock()

		if since.IsZero() {
			continue
		}
		if time.Since(since) > bc.gapTimeout {
			bc.fail(fmt.Errorf("bond: chunk seq=%d lost after %s (buffered %d chunks)",
				expected, bc.gapTimeout, buffered))
			return
		}
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
	if err := bc.terminalErr(); err != nil {
		return 0, err
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
		case <-bc.failed:
			return bc.failErr
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
		case <-bc.failed:
			return bc.failErr
		case <-bc.readySignal:
		case <-time.After(enqueueWaitTick):
		}
	}
}

func (bc *BondConn) Read(p []byte) (int, error) {

	return bc.readContext(context.Background(), p)
}

func (bc *BondConn) ReadContext(ctx context.Context, p []byte) (int, error) {
	return bc.readContext(ctx, p)
}

func (bc *BondConn) readContext(ctx context.Context, p []byte) (int, error) {
	if len(bc.readBuf) > 0 {
		n := copy(p, bc.readBuf)
		bc.readBuf = bc.readBuf[n:]
		return n, nil
	}

	select {
	case err := <-bc.readErr:
		return 0, err
	default:
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
	case err := <-bc.readErr:

		return 0, err
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
		case <-bc.failed:
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

		if bc.terminalErr() == nil {
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
