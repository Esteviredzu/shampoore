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
	enqueueWaitTick = 20 * time.Millisecond

	maxReorderChunks = 4096

	defaultGapTimeout = 15 * time.Second
	minGapTick        = 10 * time.Millisecond

	defaultNackInterval = 200 * time.Millisecond

	maxSentChunks = 4096
	maxSentBytes  = 16 * 1024 * 1024

	minDynamicWindowBytes  = 512 * 1024
	maxDynamicWindowBytes  = 32 * 1024 * 1024
	minDynamicWindowChunks = 2048
	maxDynamicWindowChunks = 8192

	ackStrideChunks = 8
	ackDelayMax     = 10 * time.Millisecond

	tlpCheckInterval    = 50 * time.Millisecond
	tlpOverheadMaxRatio = 0.05

	maxRetiredSessions = 8

	maxReadyChunks = 4096

	maxConsecutiveResets = 8

	keepaliveSendTimeout = 5 * time.Second
	warmupPingInterval   = 250 * time.Millisecond
	warmupPingDuration   = 2 * time.Second

	warmupChunks = 8
)

type sentEntry struct {
	data           []byte
	size           int
	sf             *subflow
	sentAt         time.Time
	delivered0     int64
	deliveredTime0 time.Time
	sacked         bool
	retransmitted  bool
	inflightActive bool
}

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

	session   uint32
	createdAt time.Time

	readySignal chan struct{}
	nackSignal  chan struct{}
	ackSignal   chan struct{}
	schedSignal chan struct{}

	windowSignal chan struct{}

	resetSignal chan struct{}
	softReset   atomic.Bool

	readyMu    sync.Mutex
	readyQueue [][]byte

	mu               sync.Mutex
	reorder          map[uint32][]byte
	gapSince         time.Time
	readCh           chan []byte
	readBuf          []byte
	readMu           sync.Mutex
	unackedRecvCount int
	highestRecvSeq   uint32

	recvSeq      uint32
	recvAckState atomic.Uint64

	remoteSession    uint32
	remoteSessionSet bool
	retiredSessions  map[uint32]struct{}
	retiredOrder     []uint32

	retiredLocal map[uint32]struct{}

	sentMu           sync.Mutex
	unacked          map[uint32]*sentEntry
	sentBytes        int
	sentAck          uint32
	highestSackedSeq uint32

	totalPrimaryBytes    atomic.Int64
	totalRetransmitBytes atomic.Int64
	retransmitCount      atomic.Int64

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
		createdAt:       time.Now(),
		readySignal:     make(chan struct{}, 1),
		nackSignal:      make(chan struct{}, 1),
		ackSignal:       make(chan struct{}, 1),
		schedSignal:     make(chan struct{}, 1),
		windowSignal:    make(chan struct{}, 1),
		resetSignal:     make(chan struct{}, 1),
		reorder:         make(map[uint32][]byte),
		readCh:          make(chan []byte, 512),
		retiredSessions: make(map[uint32]struct{}),
		retiredLocal:    retiredLocal,
		unacked:         make(map[uint32]*sentEntry),
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
	go bc.ackLoop()
	bc.wg.Add(1)
	go bc.tlpLoop()
	bc.wg.Add(1)
	go bc.keepaliveLoop()

	return bc
}

func (bc *BondConn) elapsedMs() uint32 {
	ms := time.Since(bc.createdAt).Milliseconds()
	if ms < 0 {
		ms = 0
	}
	return uint32(ms)
}

func (bc *BondConn) sendKeepaliveAll() {
	ack, ackSession := bc.outboundAck()
	txMs := bc.elapsedMs()
	for _, sf := range bc.sched.subflows {
		if bc.ctx.Err() != nil || bc.isClosed() || bc.terminalErr() != nil {
			return
		}
		probeID := sf.probeSeq.Add(1)
		frame := encodeControlFrame(controlPing, bc.session, ack, ackSession, encodePingArgs(probeID, txMs))
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

func (bc *BondConn) keepaliveLoop() {
	defer bc.wg.Done()

	warmupUntil := bc.createdAt.Add(warmupPingDuration)
	interval := warmupPingInterval
	if keepaliveInterval < interval {
		interval = keepaliveInterval
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-bc.closed:
			return
		case <-bc.failed:
			return
		case <-bc.ctx.Done():
			return
		case <-timer.C:
		}

		bc.sendKeepaliveAll()

		next := keepaliveInterval
		if time.Now().Before(warmupUntil) && warmupPingInterval < next {
			next = warmupPingInterval
		}
		timer.Reset(next)
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

func (bc *BondConn) signalSched() {
	select {
	case bc.schedSignal <- struct{}{}:
	default:
	}
}

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
			if chunk.kind == chunkPrimary {
				sf.queuedBytes.Add(-int64(len(chunk.data)))
			}
			bc.signalSched()
			sf.inflight.Add(1)
			bc.sendChunk(sf, chunk)
			sf.inflight.Add(-1)
		}
	}
}

func (bc *BondConn) attachChunkInflight(seq uint32, path *subflow, size int, sentAt time.Time, del0 int64, delT0 time.Time) bool {
	bc.sentMu.Lock()
	defer bc.sentMu.Unlock()
	entry, ok := bc.unacked[seq]
	if !ok || entry.sacked {
		return false
	}
	if entry.inflightActive && entry.sf != nil {
		entry.sf.inflightBytes.Add(-int64(entry.size))
		entry.inflightActive = false
	}
	entry.sf = path
	entry.sentAt = sentAt
	entry.delivered0 = del0
	entry.deliveredTime0 = delT0
	entry.inflightActive = true
	path.inflightBytes.Add(int64(size))
	return true
}

func (bc *BondConn) detachChunkInflight(seq uint32, path *subflow) {
	bc.sentMu.Lock()
	defer bc.sentMu.Unlock()
	entry, ok := bc.unacked[seq]
	if !ok || !entry.inflightActive || entry.sf != path {
		return
	}
	path.inflightBytes.Add(-int64(entry.size))
	entry.inflightActive = false
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

		if chunk.kind == chunkPrimary {
			del0, delT0 := path.metrics.deliverySnapshot()
			bc.attachChunkInflight(chunk.seq, path, len(chunk.data), time.Now(), del0, delT0)
		}

		ctx, cancel := context.WithTimeout(bc.ctx, writeTimeout)
		err := path.tube.Send(ctx, buf)
		cancel()
		if err == nil {
			path.sentBytes.Add(int64(len(chunk.data)))
			if chunk.kind == chunkPrimary {
				bc.totalPrimaryBytes.Add(int64(len(chunk.data)))
			} else if chunk.kind == chunkRetransmit {
				bc.totalRetransmitBytes.Add(int64(len(chunk.data)))
				bc.retransmitCount.Add(1)
			}
			bc.sched.MarkAlive(path)
			return
		}

		if chunk.kind == chunkPrimary {
			bc.detachChunkInflight(chunk.seq, path)
			bc.signalSched()
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
			switch kind {
			case controlPing:
				if probeID, txMs, hasArgs := decodePingArgs(args); hasArgs {
					bc.replyPong(sf, probeID, txMs)
				}
			case controlPong:
				if _, txMs, hasArgs := decodePingArgs(args); hasArgs {
					nowMs := bc.elapsedMs()
					rttMs := nowMs - txMs
					if rttMs < 60_000 {
						rtt := time.Duration(rttMs) * time.Millisecond
						if rtt < minSampleRTT {
							rtt = minSampleRTT
						}
						bc.sched.RecordRTT(sf, rtt)
						bc.signalSched()
					}
				}
			case controlAck:
				if highestRecv, ranges, hasArgs := decodeSackArgs(args); hasArgs {
					bc.handleSack(highestRecv, ranges)
				}
			case controlNack:
				bc.handleNack(args)
			}
			continue
		}
		bc.deliver(frame.seq, frame.payload)
	}
}

func (bc *BondConn) replyPong(sf *subflow, probeID, txMs uint32) {
	if bc.terminalErr() != nil || bc.isClosed() || bc.ctx.Err() != nil {
		return
	}
	bc.wg.Add(1)
	go func() {
		defer bc.wg.Done()
		ack, ackSession := bc.outboundAck()
		frame := encodeControlFrame(controlPong, bc.session, ack, ackSession, encodePingArgs(probeID, txMs))
		ctx, cancel := context.WithTimeout(bc.ctx, keepaliveSendTimeout)
		err := sf.tube.Send(ctx, frame)
		cancel()
		if err != nil {
			bc.sched.MarkDead(sf, err)
			return
		}
		bc.sched.MarkAlive(sf)
	}()
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
	if seq > bc.highestRecvSeq {
		bc.highestRecvSeq = seq
	}
	bc.unackedRecvCount++
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
	needAck := bc.unackedRecvCount >= ackStrideChunks || (len(bc.reorder) > 0 && bc.unackedRecvCount >= ackStrideChunks/2)
	bc.mu.Unlock()

	if overflow {
		bc.fail(fmt.Errorf("bond: dispatch queue overflow (consumer is not reading)"))
		return
	}

	if needAck {
		select {
		case bc.ackSignal <- struct{}{}:
		default:
		}
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
	bc.highestRecvSeq = 0
	bc.unackedRecvCount = 0
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
			bc.unacked[seq] = &sentEntry{
				data: data,
				size: len(data),
			}
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

func (bc *BondConn) dynamicWindowLimits() (maxChunks, maxBytes int) {
	maxChunks = bc.maxSentChunks
	maxBytes = bc.maxSentBytes

	if maxChunks < maxSentChunks || maxBytes < maxSentBytes {
		return maxChunks, maxBytes
	}

	totalBW, activeMaxRTT, measured := bc.sched.totalBandwidthAndActiveMaxRTT()
	if !measured || totalBW <= 0 {
		return maxChunks, maxBytes
	}

	bdpBytes := int(2.0 * totalBW * activeMaxRTT.Seconds())
	if bdpBytes < minDynamicWindowBytes {
		bdpBytes = minDynamicWindowBytes
	}
	if bdpBytes > maxDynamicWindowBytes {
		bdpBytes = maxDynamicWindowBytes
	}
	avgChunk := bc.maxPayload
	if avgChunk <= 0 {
		avgChunk = 12000
	}
	bdpChunks := bdpBytes / avgChunk
	if bdpChunks < minDynamicWindowChunks {
		bdpChunks = minDynamicWindowChunks
	}
	if bdpChunks > maxDynamicWindowChunks {
		bdpChunks = maxDynamicWindowChunks
	}
	if bdpChunks > bc.maxReorder*2 {
		bdpChunks = bc.maxReorder * 2
	}
	return bdpChunks, bdpBytes
}

func (bc *BondConn) sentWindowOpenLocked() bool {
	maxChunks, maxBytes := bc.dynamicWindowLimits()
	if len(bc.unacked) >= maxChunks {
		return false
	}
	return bc.sentBytes < maxBytes || len(bc.unacked) == 0
}

func (bc *BondConn) signalSendWindow() {
	select {
	case bc.windowSignal <- struct{}{}:
	default:
	}
}

func (bc *BondConn) finishEntryLocked(entry *sentEntry, now time.Time, freeData bool) {
	if entry == nil {
		return
	}
	if entry.inflightActive && entry.sf != nil {
		entry.sf.inflightBytes.Add(-int64(entry.size))
		entry.inflightActive = false
	}
	if !entry.sacked {
		if entry.sf != nil && !entry.sentAt.IsZero() {
			bc.sched.RecordDelivery(entry.sf, entry.size, entry.sentAt, entry.delivered0, entry.deliveredTime0, entry.retransmitted)
		}
		entry.sacked = true
		bc.sentBytes -= entry.size
		if bc.sentBytes < 0 {
			bc.sentBytes = 0
		}
	}
	if freeData {
		entry.data = nil
	}
}

func (bc *BondConn) ackSent(ack uint32) {
	if ack == 0 {
		return
	}
	now := time.Now()
	bc.sentMu.Lock()

	if ack <= bc.sentAck || ack > bc.sendSeq {
		bc.sentMu.Unlock()
		return
	}
	if ack-bc.sentAck > uint32(len(bc.unacked)) {
		for seq, entry := range bc.unacked {
			if seq < ack {
				bc.finishEntryLocked(entry, now, true)
				delete(bc.unacked, seq)
			}
		}
	} else {
		for seq := bc.sentAck; seq != ack; seq++ {
			if entry, ok := bc.unacked[seq]; ok {
				bc.finishEntryLocked(entry, now, true)
				delete(bc.unacked, seq)
			}
		}
	}
	bc.sentAck = ack
	if ack > 0 && ack-1 > bc.highestSackedSeq {
		bc.highestSackedSeq = ack - 1
	}
	bc.sentMu.Unlock()
	bc.signalSendWindow()
	bc.signalSched()
}

func (bc *BondConn) handleSack(highestRecv uint32, ranges []sackRange) {
	now := time.Now()
	changed := false
	bc.sentMu.Lock()
	if highestRecv > bc.highestSackedSeq && highestRecv < bc.sendSeq {
		bc.highestSackedSeq = highestRecv
	}
	for _, r := range ranges {
		end := r.end
		if end >= bc.sendSeq {
			if bc.sendSeq == 0 {
				continue
			}
			end = bc.sendSeq - 1
		}
		for seq := r.start; seq <= end; seq++ {
			entry, ok := bc.unacked[seq]
			if !ok || entry.sacked {
				continue
			}
			bc.finishEntryLocked(entry, now, true)
			if seq > bc.highestSackedSeq {
				bc.highestSackedSeq = seq
			}
			changed = true
			if seq == ^uint32(0) {
				break
			}
		}
	}
	bc.sentMu.Unlock()
	if changed {
		bc.signalSendWindow()
		bc.signalSched()
	}
}

func (bc *BondConn) resetSentState() {
	bc.sentMu.Lock()
	for _, entry := range bc.unacked {
		if entry != nil && entry.inflightActive && entry.sf != nil {
			entry.sf.inflightBytes.Add(-int64(entry.size))
			entry.inflightActive = false
		}
	}
	bc.sendEpoch.Add(1)
	bc.sendSeq = 0
	bc.sentAck = 0
	bc.highestSackedSeq = 0
	bc.unacked = make(map[uint32]*sentEntry)
	bc.sentBytes = 0
	bc.sentMu.Unlock()

	bc.signalSendWindow()
	bc.signalSched()
}

func (bc *BondConn) sentChunk(seq uint32) ([]byte, uint32, bool) {
	bc.sentMu.Lock()
	defer bc.sentMu.Unlock()
	entry, ok := bc.unacked[seq]
	if !ok || entry == nil || entry.sacked || len(entry.data) == 0 {
		return nil, 0, false
	}
	return entry.data, bc.sendEpoch.Load(), true
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

func (bc *BondConn) ackLoop() {
	defer bc.wg.Done()

	ticker := time.NewTicker(ackDelayMax)
	defer ticker.Stop()

	for {
		select {
		case <-bc.closed:
			return
		case <-bc.failed:
			return
		case <-bc.ctx.Done():
			return
		case <-bc.ackSignal:
			bc.sendAckFrame()
		case <-ticker.C:
			bc.sendAckFrame()
		}
	}
}

func (bc *BondConn) buildSackSnapshot() (highestRecv uint32, ranges []sackRange, pending bool) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if bc.unackedRecvCount == 0 {
		return 0, nil, false
	}
	bc.unackedRecvCount = 0
	highestRecv = bc.highestRecvSeq
	if len(bc.reorder) == 0 {
		return highestRecv, nil, true
	}

	limit := bc.highestRecvSeq
	maxSpan := uint32(bc.maxReorder) * 2
	if maxSpan > 0 && limit > bc.recvSeq+maxSpan {
		limit = bc.recvSeq + maxSpan
	}

	inRange := false
	var end uint32
	for seq := limit; ; seq-- {
		_, ok := bc.reorder[seq]
		if ok && !inRange {
			end = seq
			inRange = true
		} else if !ok && inRange {
			ranges = append(ranges, sackRange{start: seq + 1, end: end})
			inRange = false
			if len(ranges) >= maxSackRanges {
				break
			}
		}
		if seq <= bc.recvSeq {
			break
		}
	}
	if inRange && len(ranges) < maxSackRanges {
		ranges = append(ranges, sackRange{start: bc.recvSeq, end: end})
	}
	return highestRecv, ranges, true
}

func (bc *BondConn) sendAckFrame() {
	if bc.terminalErr() != nil || bc.isClosed() || bc.ctx.Err() != nil {
		return
	}
	highestRecv, ranges, pending := bc.buildSackSnapshot()
	if !pending {
		return
	}
	sf := bc.sched.pickFastest(nil)
	if sf == nil {
		return
	}
	ack, ackSession := bc.outboundAck()
	frame := encodeControlFrame(controlAck, bc.session, ack, ackSession, encodeSackArgs(highestRecv, ranges))
	ctx, cancel := context.WithTimeout(bc.ctx, writeTimeout)
	err := sf.tube.Send(ctx, frame)
	cancel()
	if err != nil {
		bc.sched.MarkDead(sf, err)
		return
	}
	bc.sched.MarkAlive(sf)
}

func (bc *BondConn) tlpLoop() {
	defer bc.wg.Done()

	ticker := time.NewTicker(tlpCheckInterval)
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
		bc.checkSpeculativeTLP()
	}
}

func (bc *BondConn) checkSpeculativeTLP() {
	now := time.Now()
	bc.sentMu.Lock()
	headSeq := bc.sentAck
	entry, ok := bc.unacked[headSeq]
	if !ok || entry == nil || entry.sacked || entry.retransmitted || entry.sf == nil || entry.sentAt.IsZero() || len(entry.data) == 0 {
		bc.sentMu.Unlock()
		return
	}
	origSF := entry.sf
	sentAt := entry.sentAt
	age := now.Sub(sentAt)
	reorderSpan := 0
	if bc.highestSackedSeq > headSeq {
		reorderSpan = int(bc.highestSackedSeq - headSeq)
	}
	rto := bc.sched.rtoFor(origSF)

	overdue := age > rto
	holPressure := reorderSpan > (bc.maxReorder*65)/100 && age > 50*time.Millisecond
	if !overdue && !holPressure {
		bc.sentMu.Unlock()
		return
	}

	primaryBytes := bc.totalPrimaryBytes.Load()
	retransBytes := bc.totalRetransmitBytes.Load()
	if !overdue && primaryBytes > 0 && float64(retransBytes+int64(entry.size)) > float64(primaryBytes)*tlpOverheadMaxRatio {
		bc.sentMu.Unlock()
		return
	}

	fastSF := bc.sched.pickFastest(origSF)
	if fastSF == nil {
		fastSF = bc.sched.pickFastest(nil)
	}
	if fastSF == nil {
		bc.sentMu.Unlock()
		return
	}
	if holPressure && !overdue {
		origSRTT, _, _, _ := origSF.metrics.rttStats(now)
		fastSRTT, _, _, _ := fastSF.metrics.rttStats(now)
		remOrig := origSRTT/2 - age
		if remOrig <= fastSRTT/2 {
			bc.sentMu.Unlock()
			return
		}
	}

	entry.retransmitted = true
	data := entry.data
	epoch := bc.sendEpoch.Load()
	bc.sentMu.Unlock()

	bc.wg.Add(1)
	go func() {
		defer bc.wg.Done()
		bc.sendChunk(fastSF, queuedChunk{seq: headSeq, data: data, kind: chunkRetransmit, epoch: epoch})
	}()
}

func (bc *BondConn) handleNack(seqs []uint32) {
	if len(seqs) == 0 || bc.terminalErr() != nil || bc.isClosed() || bc.ctx.Err() != nil {
		return
	}
	bc.wg.Add(1)
	go func() {
		defer bc.wg.Done()
		now := time.Now()
		retransmitted := 0
		for _, seq := range seqs {
			if bc.terminalErr() != nil || bc.isClosed() || bc.ctx.Err() != nil {
				return
			}
			bc.sentMu.Lock()
			entry, ok := bc.unacked[seq]
			if !ok || entry == nil || entry.sacked || len(entry.data) == 0 {
				bc.sentMu.Unlock()
				continue
			}
			origSF := entry.sf
			sentAt := entry.sentAt

			if origSF != nil && !sentAt.IsZero() && !bc.sched.isDead(origSF) {
				if expDelay, measured := bc.sched.expectedNackDelay(origSF); measured && now.Sub(sentAt) < expDelay {
					bc.sentMu.Unlock()
					continue
				}
			}
			entry.retransmitted = true
			data := entry.data
			epoch := bc.sendEpoch.Load()
			bc.sentMu.Unlock()

			sf := bc.sched.pickFastest(origSF)
			if sf == nil {
				sf = bc.sched.pickControl()
			}
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

		if sf := bc.sched.pickDelayAware(len(chunk.data), bc.maxReorder, chunk.seq < warmupChunks); sf != nil {
			if chunk.kind == chunkPrimary {
				sf.queuedBytes.Add(int64(len(chunk.data)))
			}
			select {
			case sf.queue <- chunk:
				return sf, nil
			default:
				if chunk.kind == chunkPrimary {
					sf.queuedBytes.Add(-int64(len(chunk.data)))
				}
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
		case <-bc.schedSignal:
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
				"%s{q=%d/%d rate=%dB/s bw=%dB/s rtt=%dms inflight=%dB%s}",
				s.Name, s.QueueLen, s.QueueCap, rate, s.BandwidthBps,
				s.SRTT.Milliseconds(), s.InflightBytes, tag,
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
