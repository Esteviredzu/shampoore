package bond

import (
	"bytes"
	"context"
	"crypto/rand"
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

	maxReorderChunks = 1024

	defaultGapTimeout = 15 * time.Second
	minGapTick        = 10 * time.Millisecond

	defaultNackInterval = 200 * time.Millisecond

	maxSentChunks = 2048
	maxSentBytes  = 8 * 1024 * 1024

	maxRetiredSessions = 8

	maxReadyChunks = 4096

	maxConsecutiveResets = 8

	keepaliveSendTimeout = 5 * time.Second

	warmupChunks = 8
)

var keepaliveInterval = 3 * time.Second

var errSubflowReceiveStopped = errors.New("subflow receive stopped")

var errPeerSessionReset = fmt.Errorf("%w: bond: peer session reset", transport.ErrTransportReset)

type chunkKind uint8

const (
	chunkPrimary chunkKind = iota

	chunkDuplicate

	chunkRetransmit
)

type queuedChunk struct {
	seq  uint32
	data []byte
	kind chunkKind

	epoch uint32
}

type connOptions struct {
	maxReorder    int
	gapTimeout    time.Duration
	nackInterval  time.Duration
	maxSentChunks int
	maxSentBytes  int

	retiredLocal []uint32
}

func defaultConnOptions() connOptions {
	return connOptions{
		maxReorder:    maxReorderChunks,
		gapTimeout:    defaultGapTimeout,
		nackInterval:  defaultNackInterval,
		maxSentChunks: maxSentChunks,
		maxSentBytes:  maxSentBytes,
	}
}

func aggregateMaxPayload(tubes []tubeLike) int {
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
	return maxPayload
}

func newSessionID() uint32 {
	var buf [4]byte
	for attempt := 0; attempt < 8; attempt++ {
		if _, err := rand.Read(buf[:]); err != nil {
			break
		}
		if id := binary.BigEndian.Uint32(buf[:]); id != 0 {
			return id
		}
	}
	return uint32(time.Now().UnixNano()) | 1
}

type BondConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	sched      *scheduler
	maxPayload int
	logger     logging.ContextLogger

	maxReorder    int
	gapTimeout    time.Duration
	nackInterval  time.Duration
	maxSentChunks int
	maxSentBytes  int

	session uint32

	readySignal chan struct{}
	nackSignal  chan struct{}

	windowSignal chan struct{}

	resetSignal chan struct{}
	softReset   atomic.Bool

	readyMu    sync.Mutex
	readyQueue [][]byte

	mu       sync.Mutex
	reorder  map[uint32][]byte
	gapSince time.Time
	readCh   chan []byte
	readBuf  []byte
	readMu   sync.Mutex

	recvSeq      uint32
	recvAckState atomic.Uint64

	remoteSession    uint32
	remoteSessionSet bool
	retiredSessions  map[uint32]struct{}
	retiredOrder     []uint32

	retiredLocal map[uint32]struct{}

	sentMu    sync.Mutex
	unacked   map[uint32][]byte
	sentBytes int
	sentAck   uint32

	sendSeq uint32

	sendEpoch atomic.Uint32

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

func newBondConnWithRetired(
	ctx context.Context,
	names []string,
	tubes []tubeLike,
	logger logging.ContextLogger,
	retired []uint32,
) *BondConn {
	opts := defaultConnOptions()
	opts.retiredLocal = append([]uint32(nil), retired...)
	return newBondConn(ctx, names, tubes, logger, opts)
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
	if opts.nackInterval <= 0 {
		opts.nackInterval = defaultNackInterval
	}
	if opts.maxSentChunks <= 0 {
		opts.maxSentChunks = maxSentChunks
	}
	if opts.maxSentBytes <= 0 {
		opts.maxSentBytes = maxSentBytes
	}

	c, cancel := context.WithCancel(ctx)
	maxPayload := aggregateMaxPayload(tubes)
	retiredLocal := make(map[uint32]struct{}, len(opts.retiredLocal))
	for _, id := range opts.retiredLocal {
		if id != 0 {
			retiredLocal[id] = struct{}{}
		}
	}

	bc := &BondConn{
		ctx:             c,
		cancel:          cancel,
		sched:           newScheduler(names, tubes),
		maxPayload:      maxPayload,
		logger:          logger,
		maxReorder:      opts.maxReorder,
		gapTimeout:      opts.gapTimeout,
		nackInterval:    opts.nackInterval,
		maxSentChunks:   opts.maxSentChunks,
		maxSentBytes:    opts.maxSentBytes,
		session:         newSessionID(),
		readySignal:     make(chan struct{}, 1),
		nackSignal:      make(chan struct{}, 1),
		windowSignal:    make(chan struct{}, 1),
		resetSignal:     make(chan struct{}, 1),
		reorder:         make(map[uint32][]byte),
		readCh:          make(chan []byte, 512),
		retiredSessions: make(map[uint32]struct{}),
		retiredLocal:    retiredLocal,
		unacked:         make(map[uint32][]byte),
		failed:          make(chan struct{}),
		readErr:         make(chan error, 1),
		closed:          make(chan struct{}),
	}

	bc.readers.Store(int32(len(bc.sched.subflows)))
	bc.sendEpoch.Store(1)
	for _, sf := range bc.sched.subflows {
		bc.wg.Add(1)
		go bc.readLoop(sf)
		bc.wg.Add(1)
		go bc.workerLoop(sf)
	}
	bc.wg.Add(1)
	go bc.dispatchLoop()
	bc.wg.Add(1)
	go bc.statsLoop()
	bc.wg.Add(1)
	go bc.gapLoop()
	bc.wg.Add(1)
	go bc.nackLoop()
	bc.wg.Add(1)
	go bc.keepaliveLoop()

	return bc
}

func (bc *BondConn) keepaliveLoop() {
	defer bc.wg.Done()

	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bc.closed:
			return
		case <-bc.failed:
			return
		case <-bc.ctx.Done():
			return
		case <-ticker.C:
		}

		ack, ackSession := bc.outboundAck()
		frame := encodeControlFrame(controlPing, bc.session, ack, ackSession, nil)
		for _, sf := range bc.sched.subflows {
			if bc.ctx.Err() != nil || bc.isClosed() || bc.terminalErr() != nil {
				return
			}
			ctx, cancel := context.WithTimeout(bc.ctx, keepaliveSendTimeout)
			err := sf.tube.Send(ctx, frame)
			cancel()
			if err != nil {
				bc.sched.MarkDead(sf, err)
				if bc.logger != nil {
					bc.logger.Debug("bond: keepalive on ", sf.name, " failed: ", err)
				}
				continue
			}
			bc.sched.MarkAlive(sf)
		}
	}
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
			sf.inflight.Add(1)
			bc.sendChunk(sf, chunk)
			sf.inflight.Add(-1)
		}
	}
}

func (bc *BondConn) sendChunk(sf *subflow, chunk queuedChunk) {

	if chunk.epoch != 0 && chunk.epoch != bc.sendEpoch.Load() {
		return
	}
	ack, ackSession := bc.outboundAck()
	buf, err := encodeFrame(chunk.seq, ack, ackSession, bc.session, chunk.data)
	if err != nil {
		bc.fail(fmt.Errorf("bond: encode chunk seq=%d: %w", chunk.seq, err))
		return
	}

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

		if bc.ctx.Err() != nil || bc.isClosed() {

			return
		}

		lastErr = err
		switch chunk.kind {
		case chunkDuplicate:

			if bc.logger != nil {
				bc.logger.Debug("bond: duplicate seq=", chunk.seq, " on ", path.name, " dropped: ", err)
			}
			return
		default:
			bc.sched.MarkDead(path, err)
			if bc.logger != nil {
				bc.logger.Warn("bond: send on ", path.name, " failed: ", err)
			}
		}

		alt := bc.sched.pickAlive(tried)
		if alt == nil {
			if chunk.kind == chunkRetransmit {

				if bc.logger != nil {
					bc.logger.Debug("bond: retransmit seq=", chunk.seq,
						" found no live subflow (last error: ", lastErr, ")")
				}
				return
			}
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
		if bc.readers.Add(-1) == 0 && bc.terminalErr() == nil && !bc.isClosed() && bc.ctx.Err() == nil {
			bc.fail(errors.New("bond: all subflows stopped receiving"))
		}
	}()

	resets := 0
	for {
		data, err := sf.tube.Receive(bc.ctx)
		if err != nil {

			if bc.ctx.Err() != nil || bc.isClosed() || bc.terminalErr() != nil {
				return
			}
			if errors.Is(err, transport.ErrTransportReset) {
				resets++
				if bc.logger != nil {
					bc.logger.Warn("bond: subflow ", sf.name, " transport reset: ", err)
				}
				if resets > maxConsecutiveResets && bc.readers.Load() > 1 {

					if bc.logger != nil {
						bc.logger.Warn("bond: subflow ", sf.name,
							" keeps resetting without delivering data; dropping it")
					}
					return
				}
				if resets > 1 {
					delay := time.Duration(resets) * 50 * time.Millisecond
					if delay > 500*time.Millisecond {
						delay = 500 * time.Millisecond
					}
					select {
					case <-bc.ctx.Done():
						return
					case <-time.After(delay):
					}
				}
				continue
			}
			return
		}
		resets = 0

		bc.sched.MarkAlive(sf)

		if len(data) == 0 {
			if bc.logger != nil {
				bc.logger.Debug("bond: ignoring empty message from ", sf.name)
			}
			continue
		}

		frame, err := decodeFrame(data)
		if err != nil {

			bc.fail(fmt.Errorf("bond: bad frame from %s (%d bytes): %w%s",
				sf.name, len(data), err, framingHint(data)))
			return
		}
		if !bc.noteRemoteSession(frame.session) {

			continue
		}
		if bc.staleLocalAck(frame.ackSession) {

			continue
		}

		if frame.ackSession == bc.session {
			bc.ackSent(frame.ack)
		}

		if frame.isControl() {

			kind, args, ok := decodeControl(frame.payload)
			if !ok {
				if bc.logger != nil {
					bc.logger.Debug("bond: ignoring unknown control frame from ", sf.name)
				}
				continue
			}
			if kind == controlNack {
				bc.handleNack(args)
			}
			continue
		}
		bc.deliver(frame.seq, frame.payload)
	}
}

func framingHint(data []byte) string {
	switch {
	case bytes.HasPrefix(data, []byte("SPV2")):
		return " (encrypted SPV2 message reached bond without SecurePipe: check encryption_password and the subflow wrapping)"
	case bytes.HasPrefix(data, []byte("TUN1")):
		return " (raw TUN1 frame reached bond: the peer is not using bond framing; check the multi strategy)"
	default:
		return ""
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
	newGap := false
	if seq != bc.recvSeq && bc.gapSince.IsZero() {
		bc.gapSince = time.Now()
		newGap = true
	}

	ready := false
	overflow := false
	for {
		next, ok := bc.reorder[bc.recvSeq]
		if !ok {
			break
		}
		delete(bc.reorder, bc.recvSeq)
		bc.recvSeq++
		if !bc.enqueueReadyLocked(next) {
			overflow = true
			break
		}
		ready = true
	}
	if ready {
		bc.recvAckState.Store(ackState(bc.recvSeq, bc.remoteSession))
	}

	if len(bc.reorder) == 0 {
		bc.gapSince = time.Time{}
	}
	bc.mu.Unlock()

	if overflow {
		bc.fail(fmt.Errorf("bond: dispatch queue overflow (consumer is not reading)"))
		return
	}

	if newGap {

		select {
		case bc.nackSignal <- struct{}{}:
		default:
		}
	}

	if ready {
		select {
		case bc.readySignal <- struct{}{}:
		default:
		}
	}
}

func ackState(ack, session uint32) uint64 {
	return uint64(ack)<<32 | uint64(session)
}

func (bc *BondConn) outboundAck() (uint32, uint32) {
	packed := bc.recvAckState.Load()
	return uint32(packed >> 32), uint32(packed)
}

func (bc *BondConn) noteRemoteSession(session uint32) bool {
	if session == 0 {
		return true
	}
	bc.mu.Lock()
	if !bc.remoteSessionSet {
		bc.remoteSession = session
		bc.remoteSessionSet = true
		bc.recvAckState.Store(ackState(bc.recvSeq, session))
		bc.mu.Unlock()
		return true
	}
	if session == bc.remoteSession {
		bc.mu.Unlock()
		return true
	}
	if _, retired := bc.retiredSessions[session]; retired {
		bc.mu.Unlock()
		return false
	}
	previous := bc.remoteSession
	bc.retireSessionLocked(previous)
	bc.remoteSession = session
	bc.reorder = make(map[uint32][]byte)
	bc.recvSeq = 0
	bc.recvAckState.Store(ackState(0, session))
	bc.gapSince = time.Time{}
	bc.mu.Unlock()

	bc.resetSentState()
	bc.signalPeerReset()

	if bc.logger != nil {
		bc.logger.Warn("bond: peer session changed (", previous, " -> ", session,
			"); resetting unacked/reorder and restarting both streams from seq=0")
	}
	return true
}

func (bc *BondConn) signalPeerReset() {
	bc.softReset.Store(true)
	select {
	case bc.resetSignal <- struct{}{}:
	default:
	}
}

func (bc *BondConn) takePeerReset() bool {
	return bc.softReset.Swap(false)
}

func (bc *BondConn) staleLocalAck(ackSession uint32) bool {
	if ackSession == 0 || ackSession == bc.session || len(bc.retiredLocal) == 0 {
		return false
	}
	_, retired := bc.retiredLocal[ackSession]
	return retired
}

func (bc *BondConn) retireSessionLocked(session uint32) {
	if session == 0 {
		return
	}
	if _, ok := bc.retiredSessions[session]; ok {
		return
	}
	bc.retiredSessions[session] = struct{}{}
	bc.retiredOrder = append(bc.retiredOrder, session)
	for len(bc.retiredOrder) > maxRetiredSessions {
		delete(bc.retiredSessions, bc.retiredOrder[0])
		bc.retiredOrder = bc.retiredOrder[1:]
	}
}

func (bc *BondConn) reserveChunk(data []byte) (queuedChunk, error) {
	for {
		select {
		case <-bc.closed:
			return queuedChunk{}, net.ErrClosed
		case <-bc.failed:
			return queuedChunk{}, bc.failErr
		default:
		}

		bc.sentMu.Lock()
		if bc.sentWindowOpenLocked() {
			seq := bc.sendSeq
			bc.sendSeq++
			bc.unacked[seq] = data
			bc.sentBytes += len(data)
			chunk := queuedChunk{
				seq:   seq,
				data:  data,
				kind:  chunkPrimary,
				epoch: bc.sendEpoch.Load(),
			}
			bc.sentMu.Unlock()
			return chunk, nil
		}
		bc.sentMu.Unlock()

		select {
		case <-bc.closed:
			return queuedChunk{}, net.ErrClosed
		case <-bc.failed:
			return queuedChunk{}, bc.failErr
		case <-bc.ctx.Done():
			if err := bc.terminalErr(); err != nil {
				return queuedChunk{}, err
			}
			return queuedChunk{}, net.ErrClosed
		case <-bc.windowSignal:
		}
	}
}

func (bc *BondConn) sentWindowOpenLocked() bool {
	if len(bc.unacked) >= bc.maxSentChunks {
		return false
	}
	return bc.sentBytes < bc.maxSentBytes || len(bc.unacked) == 0
}

func (bc *BondConn) signalSendWindow() {
	select {
	case bc.windowSignal <- struct{}{}:
	default:
	}
}

func (bc *BondConn) ackSent(ack uint32) {
	if ack == 0 {
		return
	}
	bc.sentMu.Lock()

	if ack <= bc.sentAck || ack > bc.sendSeq {
		bc.sentMu.Unlock()
		return
	}
	if ack-bc.sentAck > uint32(len(bc.unacked)) {
		for seq, data := range bc.unacked {
			if seq < ack {
				bc.sentBytes -= len(data)
				delete(bc.unacked, seq)
			}
		}
	} else {
		for seq := bc.sentAck; seq != ack; seq++ {
			if data, ok := bc.unacked[seq]; ok {
				bc.sentBytes -= len(data)
				delete(bc.unacked, seq)
			}
		}
	}
	bc.sentAck = ack
	bc.sentMu.Unlock()
	bc.signalSendWindow()
}

func (bc *BondConn) resetSentState() {
	bc.sentMu.Lock()
	bc.sendEpoch.Add(1)
	bc.sendSeq = 0
	bc.sentAck = 0
	bc.unacked = make(map[uint32][]byte)
	bc.sentBytes = 0
	bc.sentMu.Unlock()

	bc.signalSendWindow()
}

func (bc *BondConn) sentChunk(seq uint32) ([]byte, uint32, bool) {
	bc.sentMu.Lock()
	defer bc.sentMu.Unlock()
	data, ok := bc.unacked[seq]
	return data, bc.sendEpoch.Load(), ok
}

func (bc *BondConn) unackedCount() int {
	bc.sentMu.Lock()
	defer bc.sentMu.Unlock()
	return len(bc.unacked)
}

func (bc *BondConn) missingSeqs() []uint32 {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if len(bc.reorder) == 0 || bc.gapSince.IsZero() {
		return nil
	}
	highest := bc.recvSeq
	for seq := range bc.reorder {
		if seq > highest {
			highest = seq
		}
	}
	limit := bc.recvSeq + maxNackSeqs
	if limit < bc.recvSeq || limit > highest {
		limit = highest
	}
	missing := make([]uint32, 0, maxNackSeqs)
	for seq := bc.recvSeq; seq <= limit; seq++ {
		if _, ok := bc.reorder[seq]; !ok {
			missing = append(missing, seq)
		}
	}
	return missing
}

func (bc *BondConn) nackLoop() {
	defer bc.wg.Done()

	ticker := time.NewTicker(bc.nackInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bc.closed:
			return
		case <-bc.failed:
			return
		case <-bc.ctx.Done():
			return
		case <-bc.nackSignal:
		case <-ticker.C:
		}
		bc.sendNack()
	}
}

func (bc *BondConn) sendNack() {
	if bc.terminalErr() != nil || bc.isClosed() || bc.ctx.Err() != nil {
		return
	}
	missing := bc.missingSeqs()
	if len(missing) == 0 {
		return
	}
	sf := bc.sched.pickControl()
	if sf == nil {
		return
	}
	ack, ackSession := bc.outboundAck()
	frame := encodeControlFrame(controlNack, bc.session, ack, ackSession, missing)
	ctx, cancel := context.WithTimeout(bc.ctx, writeTimeout)
	err := sf.tube.Send(ctx, frame)
	cancel()
	if err != nil {
		bc.sched.MarkDead(sf, err)
		if bc.logger != nil {
			bc.logger.Debug("bond: nack on ", sf.name, " failed: ", err)
		}
		return
	}
	bc.sched.MarkAlive(sf)
	if bc.logger != nil {
		bc.logger.Debug("bond: nack seqs=", missing, " via ", sf.name)
	}
}

func (bc *BondConn) handleNack(seqs []uint32) {
	if len(seqs) == 0 || bc.terminalErr() != nil || bc.isClosed() || bc.ctx.Err() != nil {
		return
	}
	bc.wg.Add(1)
	go func() {
		defer bc.wg.Done()
		retransmitted := 0
		for _, seq := range seqs {
			if bc.terminalErr() != nil || bc.isClosed() || bc.ctx.Err() != nil {
				return
			}
			data, epoch, ok := bc.sentChunk(seq)
			if !ok {
				continue
			}
			sf := bc.sched.pickControl()
			if sf == nil {
				return
			}
			bc.sendChunk(sf, queuedChunk{seq: seq, data: data, kind: chunkRetransmit, epoch: epoch})
			retransmitted++
		}
		if retransmitted > 0 && bc.logger != nil {
			bc.logger.Debug("bond: retransmitted ", retransmitted, " chunks after nack")
		}
	}()
}

func (bc *BondConn) enqueueReadyLocked(chunk []byte) bool {
	bc.readyMu.Lock()
	defer bc.readyMu.Unlock()
	if len(bc.readyQueue) >= maxReadyChunks {
		return false
	}
	bc.readyQueue = append(bc.readyQueue, chunk)
	return true
}

func (bc *BondConn) dequeueReady() []byte {
	bc.readyMu.Lock()
	defer bc.readyMu.Unlock()
	if len(bc.readyQueue) == 0 {
		return nil
	}
	chunk := bc.readyQueue[0]
	bc.readyQueue[0] = nil
	bc.readyQueue = bc.readyQueue[1:]
	return chunk
}

func (bc *BondConn) dispatchLoop() {
	defer bc.wg.Done()
	for {
		if chunk := bc.dequeueReady(); chunk != nil {
			select {
			case bc.readCh <- chunk:
				continue
			case <-bc.closed:
				return
			case <-bc.failed:
				return
			case <-bc.ctx.Done():
				return
			}
		}
		select {
		case <-bc.closed:
			return
		case <-bc.failed:
			return
		case <-bc.ctx.Done():
			return
		case <-bc.readySignal:
		}
	}
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

	for len(p) > 0 {
		n := len(p)
		if n > bc.maxPayload {
			n = bc.maxPayload
		}
		data := make([]byte, n)
		copy(data, p[:n])
		p = p[n:]

		chunk, err := bc.reserveChunk(data)
		if err != nil {
			return total, err
		}

		deadline := time.Now().Add(enqueueDeadline)
		if err := bc.enqueue(chunk, deadline); err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (bc *BondConn) enqueue(chunk queuedChunk, deadline time.Time) error {
	if chunk.seq == 0 {
		return bc.enqueueFirst(chunk, deadline)
	}
	_, err := bc.enqueueChunk(chunk, deadline)
	return err
}

func (bc *BondConn) enqueueFirst(chunk queuedChunk, deadline time.Time) error {
	primary, err := bc.enqueueChunk(chunk, deadline)
	if err != nil {
		return err
	}

	dup := chunk
	dup.kind = chunkDuplicate
	copies := 0
	for _, sf := range bc.sched.subflows {
		if sf == primary || bc.sched.isDead(sf) {
			continue
		}
		select {

		case sf.queue <- dup:
			copies++
		default:
		}
	}
	if copies > 0 && bc.logger != nil {
		bc.logger.Debug("bond: first chunk seq=0 duplicated on ", copies, " extra subflows")
	}
	return nil
}

func (bc *BondConn) enqueueChunk(chunk queuedChunk, deadline time.Time) (*subflow, error) {
	for {
		select {
		case <-bc.closed:
			return nil, net.ErrClosed
		case <-bc.failed:
			return nil, bc.failErr
		default:
		}

		if sf := bc.sched.pickNonFull(nil, chunk.seq < warmupChunks); sf != nil {
			select {
			case sf.queue <- chunk:
				return sf, nil
			default:
				continue
			}
		}

		if bc.sched.allDead() {
			return nil, fmt.Errorf("bond: all subflows dead")
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("bond: enqueue timeout (all queues full)")
		}

		select {
		case <-bc.closed:
			return nil, net.ErrClosed
		case <-bc.failed:
			return nil, bc.failErr
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
	bc.readMu.Lock()
	if len(bc.readBuf) > 0 {
		n := copy(p, bc.readBuf)
		bc.readBuf = bc.readBuf[n:]
		bc.readMu.Unlock()
		return n, nil
	}
	bc.readMu.Unlock()

	if err := bc.terminalErr(); err != nil {
		select {
		case buffered := <-bc.readErr:
			if buffered != nil {
				return 0, buffered
			}
		default:
		}
		return 0, err
	}

	if bc.takePeerReset() {
		return 0, errPeerSessionReset
	}

	select {
	case err := <-bc.readErr:
		return 0, err
	default:
	}

	for {
		select {
		case data, ok := <-bc.readCh:
			if !ok {
				return 0, io.EOF
			}
			n := copy(p, data)
			if n < len(data) {
				bc.readMu.Lock()
				bc.readBuf = data[n:]
				bc.readMu.Unlock()
			}
			return n, nil
		case err := <-bc.readErr:
			return 0, err
		case <-bc.resetSignal:

			if bc.takePeerReset() {
				return 0, errPeerSessionReset
			}
		case <-bc.failed:
			return 0, bc.failErr
		case <-bc.closed:
			return 0, net.ErrClosed
		case <-ctx.Done():
			return 0, ctx.Err()
		}
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
			} else if !s.Confirmed {
				tag = " cold"
			}
			parts = append(parts, fmt.Sprintf(
				"%s{q=%d/%d rate=%dB/s%s}",
				s.Name, s.QueueLen, s.QueueCap, rate, tag,
			))
		}
		bc.logger.Info("bond[stats] ", joinStrings(parts, " "), " unacked=", bc.unackedCount())
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

const closeDrainTimeout = 2 * time.Second

func (bc *BondConn) Close() error {
	bc.closeOnce.Do(func() {
		if bc.terminalErr() == nil {
			deadline := time.Now().Add(closeDrainTimeout)
			for time.Now().Before(deadline) {
				if bc.sched.empty() {
					break
				}
				select {
				case <-bc.ctx.Done():
				case <-time.After(5 * time.Millisecond):
				}
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
