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

	pingInterval = 250 * time.Millisecond
	pingTimeout  = 15 * time.Second
	rttEWMAAlpha = 0.2
	defaultRTT   = 50 * time.Millisecond

	incomingBuffer = 4096
	outQueueSize   = 512

	defaultReorderBuf = 1 << 20
	maxReorderBufCap  = 1 << 20

	throughputSampleInterval = 500 * time.Millisecond
	throughputEWMAAlpha      = 0.3
	defaultThroughput        = 50000
	minUsefulThroughput      = 1000

	retransmitTick  = 100 * time.Millisecond
	minRTO          = 200 * time.Millisecond
	maxBackoffShift = 4

	probationPackets = 5

	maxDuration = time.Duration(1 << 62)
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

	rtt            time.Duration
	lastPongAt     time.Time
	throughput     float64
	lastAckedBytes int64
	lastSampleAt   time.Time
	dead           bool
	deadAt         time.Time
	probation      int

	ackedBytes atomic.Int64
	outCh      chan []byte
	ctr        transport.Counters
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

	mu             sync.Mutex
	nextSeq        uint32
	nextExpect     uint32
	buf            map[uint32][]byte
	pendingDeliver [][]byte
	deadCount      int
	reorderBufSize int

	unackedMu sync.Mutex
	unacked   map[uint32]*unackedFrame

	ctr transport.Counters

	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	wg      sync.WaitGroup
}

var _ transport.Transport = (*Transport)(nil)
var _ transport.Telemetry = (*Transport)(nil)

func New(cfg Config) (*Transport, error) {
	if len(cfg.Subflows) == 0 {
		return nil, errors.New("iods: no subflows")
	}
	t := &Transport{
		logger:         cfg.Logger,
		incoming:       make(chan []byte, incomingBuffer),
		deliverCh:      make(chan struct{}, 1),
		buf:            make(map[uint32][]byte),
		unacked:        make(map[uint32]*unackedFrame),
		reorderBufSize: defaultReorderBuf,
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

func (t *Transport) WalkLayers() []transport.Layer {
	out := []transport.Layer{{
		Kind: "transport", Type: "iods",
		Snapshot: t.ctr.Snapshot(),
	}}
	for _, sf := range t.subflows {
		if tw, ok := sf.transport.(transport.Telemetry); ok {
			for _, l := range tw.WalkLayers() {
				if l.Name == "" {
					l.Name = sf.name
				}
				out = append(out, l)
			}
		}
	}
	return out
}

func (sf *subflow) estimateCompletionLocked(dataLen int) time.Duration {
	rtt := sf.rtt
	if rtt <= 0 {
		rtt = defaultRTT
	}
	tp := sf.throughput
	if tp <= 0 {
		tp = defaultThroughput
	}
	transmit := time.Duration(float64(dataLen) / tp * float64(time.Second))
	queueBytes := int64(len(sf.outCh)) * 1500
	queue := time.Duration(float64(queueBytes) / tp * float64(time.Second))
	return rtt + transmit + queue
}

func (t *Transport) pickLocked(dataLen int) *subflow {

	for _, sf := range t.subflows {
		if !sf.dead && sf.probation > 0 {
			sf.probation--
			return sf
		}
	}

	var best *subflow
	bestEst := maxDuration
	for _, sf := range t.subflows {
		if sf.dead {
			continue
		}
		est := sf.estimateCompletionLocked(dataLen)
		if est < bestEst {
			bestEst = est
			best = sf
		}
	}
	return best
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

	t.ctr.UsefulSent.Add(int64(len(data)))

	uf := &unackedFrame{seq: seq, data: frame, sentAt: time.Now()}
	t.unackedMu.Lock()
	t.unacked[seq] = uf
	t.unackedMu.Unlock()

	for {
		if t.trySendFrame(uf) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.ctx.Done():
			return io.EOF
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (t *Transport) trySendFrame(uf *unackedFrame) bool {
	t.mu.Lock()
	sf := t.pickLocked(len(uf.data))
	t.mu.Unlock()
	if sf == nil {
		return false
	}

	select {
	case sf.outCh <- uf.data:
		t.unackedMu.Lock()
		uf.subflow = sf
		uf.sentAt = time.Now()
		uf.attempts++
		t.unackedMu.Unlock()
		t.ctr.NetworkSent.Add(int64(len(uf.data)))
		sf.ctr.NetworkSent.Add(int64(len(uf.data)))
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
			t.mu.Lock()
			dead := sf.dead
			t.mu.Unlock()
			if dead {
				continue
			}
			if err := sf.transport.Send(t.ctx, frame); err != nil {
				if t.ctx.Err() != nil {
					return
				}
				t.logger.Warn("iods: send on ", sf.name, " failed: ", err)
				t.markDead(sf)
			}
		}
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
		if f.sentAt.IsZero() {
			expired = append(expired, f)
			continue
		}
		shift := f.attempts - 1
		if shift < 0 {
			shift = 0
		}
		if shift > maxBackoffShift {
			shift = maxBackoffShift
		}
		rto := minRTO << shift
		if now.Sub(f.sentAt) > rto {
			expired = append(expired, f)
		}
	}
	t.unackedMu.Unlock()

	for _, f := range expired {
		if !t.trySendFrame(f) {
			t.logger.Debug("iods: retransmit failed seq=", f.seq)
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
			t.logger.Warn("iods: subflow ", sf.name, " read error: ", err)
			t.markDead(sf)
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
			continue
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

func (t *Transport) handleData(raw []byte, sf *subflow) {
	if len(raw) < dataHdrSize {
		return
	}
	seq := binary.BigEndian.Uint32(raw[1:5])
	payload := make([]byte, len(raw)-dataHdrSize)
	copy(payload, raw[dataHdrSize:])

	t.ctr.NetworkRecv.Add(int64(len(raw)))
	sf.ctr.NetworkRecv.Add(int64(len(raw)))

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

	if seq > t.nextExpect+uint32(t.reorderBufSize) {
		want := int(seq - t.nextExpect)
		if want > maxReorderBufCap {
			want = maxReorderBufCap
		}
		if want > t.reorderBufSize {
			t.reorderBufSize = want
			t.logger.Info("iods: reorder buffer expanded to ", want,
				" (seq=", seq, " expect=", t.nextExpect, ")")
		}
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
	defer t.mu.Unlock()

	if sf.dead {
		sf.dead = false
		sf.throughput = defaultThroughput
		sf.lastSampleAt = time.Now()
		sf.lastAckedBytes = sf.ackedBytes.Load()
		sf.rtt = 0
		sf.probation = probationPackets
		if t.deadCount > 0 {
			t.deadCount--
		}
		t.logger.Info("iods: subflow ", sf.name, " revived (pong, was dead ",
			time.Since(sf.deadAt).Round(time.Second), ")")
	}
	if sf.rtt == 0 {
		sf.rtt = sample
	} else {
		sf.rtt = time.Duration(float64(sf.rtt)*(1-rttEWMAAlpha) +
			float64(sample)*rttEWMAAlpha)
	}
	sf.lastPongAt = time.Now()
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
	alive := len(t.subflows) - t.deadCount
	t.mu.Unlock()

	t.logger.Warn("iods: subflow ", sf.name, " marked dead (alive=",
		alive, "/", len(t.subflows), ")")

	t.unackedMu.Lock()
	invalidated := 0
	for _, f := range t.unacked {
		if f.subflow == sf {
			f.sentAt = time.Time{}
			invalidated++
		}
	}
	t.unackedMu.Unlock()
	if invalidated > 0 {
		t.logger.Warn("iods: invalidated ", invalidated,
			" unacked frames from dead subflow ", sf.name)
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

		if !dead && !lastPong.IsZero() && time.Since(lastPong) > pingTimeout {
			t.logger.Warn("iods: subflow ", sf.name, " no pong for ",
				time.Since(lastPong).Round(time.Second), ", marking dead")
			t.markDead(sf)
		}

		ping := make([]byte, pingHdrSize)
		ping[0] = typePing
		binary.BigEndian.PutUint64(ping[1:9], uint64(time.Now().UnixNano()))
		go func() {
			_ = sf.transport.Send(t.ctx, ping)
		}()
	}
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
			sf.throughput = throughputEWMAAlpha*sample +
				(1-throughputEWMAAlpha)*sf.throughput
			if sf.throughput < minUsefulThroughput {
				sf.throughput = minUsefulThroughput
			}
		}
		sf.lastAckedBytes = acked
		sf.lastSampleAt = now
	}
}

func (t *Transport) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.ctx.Done():
		return nil, io.EOF
	case d := <-t.incoming:
		t.ctr.UsefulRecv.Add(int64(len(d)))
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
