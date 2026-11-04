package iods

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

const (
	typeData byte = 1
	typeAck  byte = 2
	typePing byte = 3
	typePong byte = 4

	dataHdrSize = 1 + 4
	ackHdrSize  = 1 + 4
	pingHdrSize = 1 + 8

	pingInterval   = 200 * time.Millisecond
	pingTimeout    = 5 * time.Second
	rttEWMAAlpha   = 0.2
	defaultRTT     = 50 * time.Millisecond
	incomingBuffer = 4096
	maxReorderBuf  = 8192
	outQueueSize   = 64

	throughputSampleInterval = 500 * time.Millisecond
	throughputEWMAAlpha      = 0.3
	defaultThroughput        = 50_000
	quantum                  = 1500

	retransmitTick    = 200 * time.Millisecond
	minRetransmitTime = 2 * time.Second
	maxBackoffShift   = 4
)

type Subflow struct {
	Name      string
	Transport transport.Transport
}

type Config struct {
	Subflows []Subflow
	Logger   logging.ContextLogger
}

type subflow struct {
	name      string
	transport transport.Transport

	rtt        time.Duration
	lastPongAt time.Time

	ackedBytes     atomic.Int64
	lastAckedBytes int64
	lastSampleAt   time.Time
	throughput     float64

	deficit int

	outCh chan []byte

	dead   bool
	deadAt time.Time
}

type unackedFrame struct {
	seq      uint32
	data     []byte
	sentAt   time.Time
	subflow  *subflow
	attempts int
}

type Transport struct {
	subflows []*subflow
	logger   logging.ContextLogger

	incoming  chan []byte
	deliverCh chan struct{}
	allDead   chan struct{}

	mu             sync.Mutex
	nextSeq        uint32
	nextExpect     uint32
	buf            map[uint32][]byte
	pendingDeliver [][]byte
	deadCount      int
	rrIndex        int

	unackedMu sync.Mutex
	unacked   map[uint32]*unackedFrame

	ctx      context.Context
	cancel   context.CancelFunc
	started  bool
	wg       sync.WaitGroup
	deadOnce sync.Once
}

var _ transport.Transport = (*Transport)(nil)

func New(cfg Config) (*Transport, error) {
	if len(cfg.Subflows) == 0 {
		return nil, errors.New("iods: no subflows")
	}
	t := &Transport{
		logger:    cfg.Logger,
		incoming:  make(chan []byte, incomingBuffer),
		deliverCh: make(chan struct{}, 1),
		allDead:   make(chan struct{}),
		buf:       make(map[uint32][]byte),
		unacked:   make(map[uint32]*unackedFrame),
	}
	now := time.Now()
	for _, s := range cfg.Subflows {
		if s.Transport == nil {
			return nil, errors.New("iods: nil transport in subflow " + s.Name)
		}
		t.subflows = append(t.subflows, &subflow{
			name:         s.Name,
			transport:    s.Transport,
			lastSampleAt: now,
			throughput:   defaultThroughput,
			outCh:        make(chan []byte, outQueueSize),
		})
	}
	return t, nil
}

func (t *Transport) Start(ctx context.Context) error {
	if t.started {
		return nil
	}
	for i, sf := range t.subflows {
		if err := sf.transport.Start(ctx); err != nil {
			for j := i - 1; j >= 0; j-- {
				_ = t.subflows[j].transport.Stop(context.Background())
			}
			return err
		}
	}
	t.ctx, t.cancel = context.WithCancel(ctx)
	t.started = true

	t.wg.Add(3)
	go t.deliverLoop()
	go t.statsLoop()
	go t.retransmitLoop()

	for _, sf := range t.subflows {
		t.wg.Add(1)
		go t.sendLoop(sf)
		t.wg.Add(1)
		go t.readLoop(sf)
		t.wg.Add(1)
		go t.pingLoop(sf)
	}
	return nil
}

func (t *Transport) Stop(ctx context.Context) error {
	if !t.started {
		return nil
	}
	t.started = false
	if t.cancel != nil {
		t.cancel()
	}
	t.wg.Wait()
	var lastErr error
	for i := len(t.subflows) - 1; i >= 0; i-- {
		if err := t.subflows[i].transport.Stop(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (t *Transport) Send(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	t.mu.Lock()
	seq := t.nextSeq
	t.nextSeq++
	t.mu.Unlock()

	frame := make([]byte, dataHdrSize+len(data))
	frame[0] = typeData
	binary.BigEndian.PutUint32(frame[1:5], seq)
	copy(frame[dataHdrSize:], data)

	t.unackedMu.Lock()
	uf := &unackedFrame{seq: seq, data: frame, sentAt: time.Now()}
	t.unacked[seq] = uf
	t.unackedMu.Unlock()

	tried := make(map[*subflow]bool, len(t.subflows))
	for {
		t.mu.Lock()
		sf := t.pickLockedSkipping(len(frame), tried)
		t.mu.Unlock()

		if sf == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.ctx.Done():
				return io.EOF
			case <-t.allDead:
				return errors.New("iods: all subflows dead")
			case <-time.After(5 * time.Millisecond):
			}
			tried = make(map[*subflow]bool, len(t.subflows))
			continue
		}
		tried[sf] = true

		if t.trySend(sf, frame) {
			t.unackedMu.Lock()
			uf.subflow = sf
			t.unackedMu.Unlock()
			return nil
		}
	}
}

func (t *Transport) pickLockedSkipping(dataLen int, skip map[*subflow]bool) *subflow {
	n := len(t.subflows)
	var totalWeight float64
	for _, sf := range t.subflows {
		if !sf.dead && !skip[sf] && sf.throughput > 0 {
			totalWeight += sf.throughput
		}
	}
	if totalWeight <= 0 {
		return nil
	}
	for attempt := 0; attempt < 2*n; attempt++ {
		t.rrIndex = (t.rrIndex + 1) % n
		sf := t.subflows[t.rrIndex]
		if sf.dead || skip[sf] {
			continue
		}
		weight := sf.throughput / totalWeight
		sf.deficit += int(float64(quantum) * weight * float64(n))
		if sf.deficit >= dataLen {
			sf.deficit -= dataLen
			return sf
		}
	}
	var best *subflow
	bestDeficit := 1 << 30
	for _, sf := range t.subflows {
		if sf.dead || skip[sf] {
			continue
		}
		if sf.deficit < bestDeficit {
			bestDeficit = sf.deficit
			best = sf
		}
	}
	if best != nil {
		best.deficit -= dataLen
	}
	return best
}

func (t *Transport) trySend(sf *subflow, frame []byte) bool {
	t.mu.Lock()
	dead := sf.dead
	t.mu.Unlock()
	if dead {
		return false
	}
	select {
	case sf.outCh <- frame:
		return true
	default:
		return false
	}
}

func (t *Transport) sendLoop(sf *subflow) {
	defer t.wg.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case frame := <-sf.outCh:
			if err := sf.transport.Send(t.ctx, frame); err != nil {
				if t.ctx.Err() != nil {
					return
				}
				t.logger.Warn("iods: send on ", sf.name, " failed: ", err)
				t.markDead(sf)
				return
			}
		}
	}
}

func (t *Transport) readLoop(sf *subflow) {
	defer t.wg.Done()
	for {
		raw, err := sf.transport.Receive(t.ctx)
		if err != nil {
			if t.ctx.Err() != nil {
				return
			}
			t.logger.Warn("iods: subflow ", sf.name, " died: ", err)
			t.markDead(sf)
			return
		}
		if len(raw) < 1 {
			continue
		}
		switch raw[0] {
		case typeData:
			t.handleData(raw, sf)
		case typeAck:
			t.handleAck(raw)
		case typePing:
			t.handlePing(raw, sf)
		case typePong:
			t.handlePong(raw, sf)
		}
	}
}

func (t *Transport) markDead(sf *subflow) {
	t.mu.Lock()
	if sf.dead {
		t.mu.Unlock()
		return
	}
	sf.dead = true
	sf.deadAt = time.Now()
	t.deadCount++
	allGone := t.deadCount == len(t.subflows)
	t.mu.Unlock()

	if allGone {
		t.deadOnce.Do(func() { close(t.allDead) })
	}
}

func (t *Transport) handleData(raw []byte, sf *subflow) {
	if len(raw) < dataHdrSize {
		return
	}
	seq := binary.BigEndian.Uint32(raw[1:5])
	payload := make([]byte, len(raw)-dataHdrSize)
	copy(payload, raw[dataHdrSize:])

	ack := make([]byte, ackHdrSize)
	ack[0] = typeAck
	binary.BigEndian.PutUint32(ack[1:5], seq)
	go func() {
		_ = sf.transport.Send(t.ctx, ack)
	}()

	t.mu.Lock()
	if seq < t.nextExpect {
		t.mu.Unlock()
		return
	}
	if seq > t.nextExpect+maxReorderBuf {
		minSeq := seq
		for s := range t.buf {
			if s < minSeq {
				minSeq = s
			}
		}
		t.logger.Warn("iods: reorder overflow seq=", seq,
			" expected=", t.nextExpect, "; skipping to ", minSeq)
		for s := t.nextExpect; s < minSeq; s++ {
			delete(t.buf, s)
		}
		t.nextExpect = minSeq
	}
	if _, exists := t.buf[seq]; !exists {
		t.buf[seq] = payload
	}
	for {
		d, ok := t.buf[t.nextExpect]
		if !ok {
			break
		}
		delete(t.buf, t.nextExpect)
		t.nextExpect++
		t.pendingDeliver = append(t.pendingDeliver, d)
	}
	hasPending := len(t.pendingDeliver) > 0
	t.mu.Unlock()

	if hasPending {
		select {
		case t.deliverCh <- struct{}{}:
		default:
		}
	}
}

func (t *Transport) handleAck(raw []byte) {
	if len(raw) < ackHdrSize {
		return
	}
	seq := binary.BigEndian.Uint32(raw[1:5])

	t.unackedMu.Lock()
	f, ok := t.unacked[seq]
	if ok {
		delete(t.unacked, seq)
	}
	t.unackedMu.Unlock()

	if ok && f.subflow != nil {
		f.subflow.ackedBytes.Add(int64(len(f.data)))
	}
}

func (t *Transport) handlePing(raw []byte, sf *subflow) {
	if len(raw) < pingHdrSize {
		return
	}
	sentAt := binary.BigEndian.Uint64(raw[1:9])
	pong := make([]byte, pingHdrSize)
	pong[0] = typePong
	binary.BigEndian.PutUint64(pong[1:9], sentAt)
	_ = sf.transport.Send(t.ctx, pong)
}

func (t *Transport) handlePong(raw []byte, sf *subflow) {
	if len(raw) < pingHdrSize {
		return
	}
	sentAt := int64(binary.BigEndian.Uint64(raw[1:9]))
	sample := time.Duration(time.Now().UnixNano() - sentAt)
	if sample <= 0 {
		return
	}
	t.mu.Lock()
	if sf.rtt == 0 {
		sf.rtt = sample
	} else {
		sf.rtt = time.Duration(float64(sf.rtt)*(1-rttEWMAAlpha) + float64(sample)*rttEWMAAlpha)
	}
	sf.lastPongAt = time.Now()
	t.mu.Unlock()
}

func (t *Transport) deliverLoop() {
	defer t.wg.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-t.deliverCh:
		}
		t.mu.Lock()
		batch := t.pendingDeliver
		t.pendingDeliver = nil
		t.mu.Unlock()
		for _, d := range batch {
			select {
			case t.incoming <- d:
			case <-t.ctx.Done():
				return
			}
		}
	}
}

func (t *Transport) statsLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(throughputSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.sampleThroughputs()
		}
	}
}

func (t *Transport) sampleThroughputs() {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, sf := range t.subflows {
		if sf.dead {
			continue
		}
		acked := sf.ackedBytes.Load()
		delta := acked - sf.lastAckedBytes
		dt := now.Sub(sf.lastSampleAt).Seconds()
		if dt > 0 && delta > 0 {
			sample := float64(delta) / dt
			sf.throughput = throughputEWMAAlpha*sample + (1-throughputEWMAAlpha)*sf.throughput
		}
		sf.lastAckedBytes = acked
		sf.lastSampleAt = now
	}
}

func (t *Transport) retransmitLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(retransmitTick)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.retransmitExpired()
		}
	}
}

func (t *Transport) retransmitExpired() {
	now := time.Now()
	t.unackedMu.Lock()
	var expired []*unackedFrame
	for _, f := range t.unacked {
		if f.subflow == nil {
			continue
		}
		if f.subflow.dead {
			expired = append(expired, f)
			continue
		}
		timeout := 3 * f.subflow.rtt
		if timeout < minRetransmitTime {
			timeout = minRetransmitTime
		}
		shift := f.attempts
		if shift > maxBackoffShift {
			shift = maxBackoffShift
		}
		timeout <<= shift
		if now.Sub(f.sentAt) > timeout {
			expired = append(expired, f)
		}
	}
	t.unackedMu.Unlock()

	for _, f := range expired {
		t.retransmit(f)
	}
}

func (t *Transport) retransmit(f *unackedFrame) {
	t.mu.Lock()
	var newSf *subflow
	for _, sf := range t.subflows {
		if sf.dead || sf == f.subflow {
			continue
		}
		if newSf == nil || sf.throughput > newSf.throughput {
			newSf = sf
		}
	}
	t.mu.Unlock()
	if newSf == nil {
		return
	}
	if !t.trySend(newSf, f.data) {
		return
	}
	t.unackedMu.Lock()
	old := f.subflow
	f.subflow = newSf
	f.sentAt = time.Now()
	f.attempts++
	att := f.attempts
	t.unackedMu.Unlock()

	if old != nil {
		t.logger.Info("iods: retransmit seq=", f.seq,
			" from ", old.name, " to ", newSf.name,
			" attempt=", att)
	}
}

func (t *Transport) pingLoop(sf *subflow) {
	defer t.wg.Done()
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
		}
		t.mu.Lock()
		dead := sf.dead
		lastPong := sf.lastPongAt
		t.mu.Unlock()
		if dead {
			return
		}
		if !lastPong.IsZero() && time.Since(lastPong) > pingTimeout {
			t.logger.Warn("iods: subflow ", sf.name,
				" no pong for ", time.Since(lastPong), ", marking dead")
			t.markDead(sf)
			return
		}
		ping := make([]byte, pingHdrSize)
		ping[0] = typePing
		binary.BigEndian.PutUint64(ping[1:9], uint64(time.Now().UnixNano()))
		_ = sf.transport.Send(t.ctx, ping)
	}
}

func (t *Transport) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.ctx.Done():
		return nil, io.EOF
	case <-t.allDead:
		return nil, io.EOF
	case d := <-t.incoming:
		return d, nil
	}
}

func (t *Transport) MaxMessageSize() int {
	min := 0
	for _, sf := range t.subflows {
		m := sf.transport.MaxMessageSize() - dataHdrSize
		if m <= 0 {
			continue
		}
		if min == 0 || m < min {
			min = m
		}
	}
	return min
}

func (t *Transport) BytesDelivered() int64 {
	var total int64
	for _, sf := range t.subflows {
		total += sf.transport.BytesDelivered()
	}
	return total
}

func (t *Transport) BytesAcked() int64 {
	var total int64
	for _, sf := range t.subflows {
		total += sf.transport.BytesAcked()
	}
	return total
}

func (t *Transport) Throughputs() map[string]float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]float64, len(t.subflows))
	for _, sf := range t.subflows {
		out[sf.name] = sf.throughput
	}
	return out
}

func (t *Transport) UnackedCount() int {
	t.unackedMu.Lock()
	defer t.unackedMu.Unlock()
	return len(t.unacked)
}
