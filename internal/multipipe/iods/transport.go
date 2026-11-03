package iods

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

const (
	typeData byte = 1
	typePing byte = 2
	typePong byte = 3

	dataHdrSize = 1 + 4 + 8
	pingHdrSize = 1 + 8

	pingInterval     = 200 * time.Millisecond
	rttEWMAAlpha     = 0.2
	defaultRTT       = 50 * time.Millisecond
	incomingBuffer   = 4096
	maxReorderBuffer = 8192

	throughputSampleInterval = 500 * time.Millisecond
	throughputEWMAAlpha      = 0.3

	defaultThroughput = 50000
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
	name       string
	transport  transport.Transport
	rtt        time.Duration
	estArrival time.Time
	dead       bool

	throughput   float64
	lastAcked    int64
	lastSampleAt time.Time
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
	lastAssigned   time.Time
	deadCount      int

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
	}
	now := time.Now()
	for _, s := range cfg.Subflows {
		if s.Transport == nil {
			return nil, errors.New("iods: nil transport in subflow " + s.Name)
		}
		t.subflows = append(t.subflows, &subflow{
			name:         s.Name,
			transport:    s.Transport,
			lastAcked:    s.Transport.BytesAcked(),
			lastSampleAt: now,
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

	t.wg.Add(1)
	go t.deliverLoop()

	t.wg.Add(1)
	go t.statsLoop()

	for _, sf := range t.subflows {
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
	now := time.Now()

	t.mu.Lock()
	sf := t.pickLocked(now, len(data))
	if sf == nil {
		t.mu.Unlock()
		return errors.New("iods: all subflows dead")
	}
	seq := t.nextSeq
	t.nextSeq++
	t.mu.Unlock()

	frame := make([]byte, dataHdrSize+len(data))
	frame[0] = typeData
	binary.BigEndian.PutUint32(frame[1:5], seq)
	binary.BigEndian.PutUint64(frame[5:13], uint64(now.UnixNano()))
	copy(frame[dataHdrSize:], data)

	if err := sf.transport.Send(ctx, frame); err != nil {
		t.mu.Lock()
		if t.nextSeq == seq+1 {
			t.nextSeq = seq
		}
		t.mu.Unlock()
		return fmt.Errorf("iods: send on %s: %w", sf.name, err)
	}
	return nil
}

func (t *Transport) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.ctx.Done():
		return nil, io.EOF
	case <-t.allDead:
		return nil, io.EOF
	case data := <-t.incoming:
		return data, nil
	}
}

func (t *Transport) MaxMessageSize() int {
	min := 0
	for _, sf := range t.subflows {
		m := sf.transport.MaxMessageSize()
		if m <= 0 {
			continue
		}
		m -= dataHdrSize
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

func (t *Transport) pickLocked(now time.Time, dataLen int) *subflow {
	var (
		chosen     *subflow
		chosenTime time.Time
		fallback   *subflow
		fallbackT  time.Time
	)

	for _, sf := range t.subflows {
		if sf.dead {
			continue
		}

		rtt := sf.rtt
		if rtt <= 0 {
			rtt = defaultRTT
		}

		tp := sf.throughput
		if tp <= 0 {
			tp = defaultThroughput
		}

		segmentTime := time.Duration(float64(dataLen) / tp * float64(time.Second))

		base := sf.estArrival
		if base.Before(now) {
			base = now
		}
		arrival := base.Add(rtt/2 + segmentTime)

		if arrival.Before(t.lastAssigned) {
			if fallback == nil || arrival.After(fallbackT) {
				fallback = sf
				fallbackT = arrival
			}
			continue
		}

		if chosen == nil || arrival.Before(chosenTime) {
			chosen = sf
			chosenTime = arrival
		}
	}

	if chosen == nil {
		chosen = fallback
		chosenTime = fallbackT
	}
	if chosen != nil {
		chosen.estArrival = chosenTime
		t.lastAssigned = chosenTime
	}
	return chosen
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
			t.mu.Lock()
			sf.dead = true
			t.deadCount++
			allGone := t.deadCount == len(t.subflows)
			t.mu.Unlock()
			if allGone {
				t.deadOnce.Do(func() { close(t.allDead) })
			}
			return
		}
		if len(raw) < 1 {
			continue
		}
		switch raw[0] {
		case typeData:
			t.handleData(raw)
		case typePing:
			t.handlePing(raw, sf)
		case typePong:
			t.handlePong(raw, sf)
		}
	}
}

func (t *Transport) handleData(raw []byte) {
	if len(raw) < dataHdrSize {
		return
	}
	seq := binary.BigEndian.Uint32(raw[1:5])
	payload := make([]byte, len(raw)-dataHdrSize)
	copy(payload, raw[dataHdrSize:])

	t.mu.Lock()

	if seq < t.nextExpect {
		t.mu.Unlock()
		return
	}
	if seq > t.nextExpect+maxReorderBuffer {
		t.mu.Unlock()
		t.logger.Warn("iods: reorder buffer overflow seq=", seq,
			" expected=", t.nextExpect, "; resetting transport")
		if t.cancel != nil {
			t.cancel()
		}
		return
	}
	if _, exists := t.buf[seq]; !exists {
		t.buf[seq] = payload
	}

	for {
		data, ok := t.buf[t.nextExpect]
		if !ok {
			break
		}
		delete(t.buf, t.nextExpect)
		t.nextExpect++
		t.pendingDeliver = append(t.pendingDeliver, data)
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

		for _, data := range batch {
			select {
			case t.incoming <- data:
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

		acked := sf.transport.BytesAcked()
		delta := acked - sf.lastAcked
		dt := now.Sub(sf.lastSampleAt).Seconds()

		if dt <= 0 || delta <= 0 {
			sf.lastAcked = acked
			sf.lastSampleAt = now
			continue
		}

		sample := float64(delta) / dt
		if sf.throughput == 0 {
			sf.throughput = sample
		} else {
			sf.throughput = throughputEWMAAlpha*sample +
				(1-throughputEWMAAlpha)*sf.throughput
		}

		sf.lastAcked = acked
		sf.lastSampleAt = now
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
		t.mu.Unlock()
		if dead {
			return
		}

		ping := make([]byte, pingHdrSize)
		ping[0] = typePing
		binary.BigEndian.PutUint64(ping[1:9], uint64(time.Now().UnixNano()))
		if err := sf.transport.Send(t.ctx, ping); err != nil {
			t.logger.Warn("iods: ping on ", sf.name, " failed: ", err)
		}
	}
}
