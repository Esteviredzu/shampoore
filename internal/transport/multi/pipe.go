package multi

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

const (
	creditFactor  = 8
	healthTimeout = 5 * time.Second
	pollInterval  = 100 * time.Millisecond
	maxQueueBytes = 2 * 1024 * 1024
	minSendChunk  = 64
	queueDepth    = 8192
)

type Subflow struct {
	Name      string
	Transport transport.Transport
	Weight    int
}

type subflowState struct {
	Subflow
	sentBytes         atomic.Int64
	lastAcked         atomic.Int64
	lastProgressNanos atomic.Int64
	lastSendNanos     atomic.Int64
	dead              atomic.Bool
	limit             atomic.Int64
}

func (s *subflowState) weight() int64 {
	if s.Weight <= 0 {
		return 1
	}
	return int64(s.Weight)
}

type pendingSeg struct {
	seq     uint64
	payload []byte
	data    []byte
	sentTo  *subflowState
	offset  int64
}

type MultiPipe struct {
	subs   []*subflowState
	logger logging.ContextLogger

	minMax atomic.Int64

	queue chan *pendingSeg
	retry chan *pendingSeg

	inFlightMu sync.Mutex
	inFlight   map[uint64]*pendingSeg
	queueBytes atomic.Int64

	nextSeq atomic.Uint64

	recvMu      sync.Mutex
	reassembly  map[uint64][]byte
	nextRecvSeq uint64
	recvCh      chan []byte

	bytesDelivered atomic.Int64
	closed         atomic.Bool

	ctx     context.Context
	cancel  context.CancelFunc
	started atomic.Bool
	wg      sync.WaitGroup
}

func New(subs []Subflow, logger logging.ContextLogger) (*MultiPipe, error) {
	if len(subs) == 0 {
		return nil, errors.New("multi: at least one subflow required")
	}
	mp := &MultiPipe{
		logger:     logger,
		inFlight:   make(map[uint64]*pendingSeg),
		reassembly: make(map[uint64][]byte),
		recvCh:     make(chan []byte, 512),
		queue:      make(chan *pendingSeg, queueDepth),
		retry:      make(chan *pendingSeg, queueDepth),
	}
	for _, sf := range subs {
		if sf.Transport == nil {
			return nil, errors.New("multi: subflow " + sf.Name + ": nil transport")
		}
		st := &subflowState{Subflow: sf}
		st.limit.Store(int64(creditFactor * sf.Transport.MaxMessageSize()))
		mp.subs = append(mp.subs, st)
	}
	mp.recalcMinMax()
	return mp, nil
}

func (mp *MultiPipe) Start(ctx context.Context) error {
	if !mp.started.CompareAndSwap(false, true) {
		return nil
	}
	for _, sf := range mp.subs {
		if err := sf.Transport.Start(ctx); err != nil {
			for _, s := range mp.subs {
				if s == sf {
					break
				}
				_ = s.Transport.Stop(context.Background())
			}
			mp.started.Store(false)
			return err
		}
	}
	mp.ctx, mp.cancel = context.WithCancel(ctx)
	for _, sf := range mp.subs {
		mp.wg.Add(1)
		go mp.receiveLoop(sf)
	}
	mp.wg.Add(2)
	go mp.dispatchLoop()
	go mp.pollLoop()
	mp.logger.Info("multi: started; subflows=", len(mp.subs), " minMax=", mp.minMax.Load())
	return nil
}

func (mp *MultiPipe) Stop(ctx context.Context) error {
	if !mp.started.Load() {
		return nil
	}
	mp.closed.Store(true)
	mp.cancel()
	mp.wg.Wait()
	for _, sf := range mp.subs {
		_ = sf.Transport.Stop(ctx)
	}
	return nil
}

func (mp *MultiPipe) MaxMessageSize() int   { return int(mp.minMax.Load()) }
func (mp *MultiPipe) BytesDelivered() int64 { return mp.bytesDelivered.Load() }

func (mp *MultiPipe) BytesAcked() int64 {
	var acked int64
	for _, sf := range mp.subs {
		acked += sf.lastAcked.Load()
	}
	return acked
}

func (mp *MultiPipe) Send(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if mp.closed.Load() {
		return errors.New("multi: closed")
	}
	chunkSize := int(mp.minMax.Load())
	if chunkSize <= 0 {
		return errors.New("multi: no live subflows")
	}
	for off := 0; off < len(data); off += chunkSize {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}
		payload := make([]byte, end-off)
		copy(payload, data[off:end])
		seq := mp.nextSeq.Add(1) - 1
		frame, err := encodeFrame(seq, payload)
		if err != nil {
			return err
		}
		for mp.queueBytes.Load() > maxQueueBytes {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-mp.ctx.Done():
				return errors.New("multi: closed")
			case <-time.After(10 * time.Millisecond):
			}
		}
		seg := &pendingSeg{seq: seq, payload: payload, data: frame}
		select {
		case mp.queue <- seg:
			mp.queueBytes.Add(int64(len(payload)))
		case <-ctx.Done():
			return ctx.Err()
		case <-mp.ctx.Done():
			return errors.New("multi: closed")
		}
	}
	return nil
}

func (mp *MultiPipe) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-mp.ctx.Done():
		return nil, io.EOF
	case data := <-mp.recvCh:
		mp.bytesDelivered.Add(int64(len(data)))
		return data, nil
	}
}

func (mp *MultiPipe) dispatchLoop() {
	defer mp.wg.Done()
	for {
		var seg *pendingSeg
		select {
		case <-mp.ctx.Done():
			return
		case seg = <-mp.retry:
		case seg = <-mp.queue:
		}
		if seg == nil {
			continue
		}
		for {
			err := mp.trySend(seg)
			if err == nil {
				break
			}
			select {
			case <-mp.ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
		mp.queueBytes.Add(-int64(len(seg.payload)))
	}
}

func (mp *MultiPipe) trySend(seg *pendingSeg) error {
	sf := mp.pickSubflow(seg)
	if sf == nil {
		return errors.New("no subflow with credit")
	}
	mp.inFlightMu.Lock()
	mp.inFlight[seg.seq] = seg
	mp.inFlightMu.Unlock()

	err := sf.Transport.Send(mp.ctx, seg.data)

	mp.inFlightMu.Lock()
	if err != nil {
		delete(mp.inFlight, seg.seq)
		mp.inFlightMu.Unlock()
		return err
	}
	seg.sentTo = sf
	seg.offset = sf.sentBytes.Load()
	sf.sentBytes.Add(int64(len(seg.data)))
	now := time.Now().UnixNano()
	sf.lastSendNanos.Store(now)
	sf.lastProgressNanos.Store(now)
	mp.inFlightMu.Unlock()
	return nil
}

func (mp *MultiPipe) pickSubflow(seg *pendingSeg) *subflowState {
	mp.inFlightMu.Lock()
	defer mp.inFlightMu.Unlock()
	need := int64(len(seg.data))
	var best *subflowState
	bestScore := int64(math.MaxInt64)
	for _, sf := range mp.subs {
		if sf.dead.Load() {
			continue
		}
		inFlight := sf.sentBytes.Load() - sf.lastAcked.Load()
		if inFlight+need > sf.limit.Load() {
			continue
		}
		score := inFlight / sf.weight()
		if score < bestScore {
			best = sf
			bestScore = score
		}
	}
	return best
}

func (mp *MultiPipe) pollLoop() {
	defer mp.wg.Done()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-mp.ctx.Done():
			return
		case <-ticker.C:
		}
		now := time.Now().UnixNano()
		for _, sf := range mp.subs {
			acked := sf.Transport.BytesAcked()
			prev := sf.lastAcked.Swap(acked)
			if acked > prev {
				sf.lastProgressNanos.Store(now)
				if sf.dead.CompareAndSwap(true, false) {
					mp.logger.Info("multi: subflow ", sf.Name, " revived")
					mp.recalcMinMax()
				}
			}
			if sf.dead.Load() {
				continue
			}
			inFlight := sf.sentBytes.Load() - acked
			if inFlight <= 0 {
				continue
			}
			last := sf.lastSendNanos.Load()
			if lp := sf.lastProgressNanos.Load(); lp > last {
				last = lp
			}
			if now-last > int64(healthTimeout) {
				mp.markDead(sf, "no progress")
			}
		}
		mp.cleanupAcked()
	}
}

func (mp *MultiPipe) cleanupAcked() {
	mp.inFlightMu.Lock()
	defer mp.inFlightMu.Unlock()
	for seq, seg := range mp.inFlight {
		if seg.sentTo == nil {
			delete(mp.inFlight, seq)
			continue
		}
		if seg.sentTo.lastAcked.Load() >= seg.offset+int64(len(seg.data)) {
			delete(mp.inFlight, seq)
		}
	}
}

func (mp *MultiPipe) markDead(sf *subflowState, reason string) {
	if !sf.dead.CompareAndSwap(false, true) {
		return
	}
	mp.logger.Warn("multi: subflow ", sf.Name, " dead: ", reason)
	mp.inFlightMu.Lock()
	var retries []*pendingSeg
	for seq, seg := range mp.inFlight {
		if seg.sentTo == sf {
			delete(mp.inFlight, seq)
			retries = append(retries, seg)
		}
	}
	mp.inFlightMu.Unlock()
	for _, seg := range retries {
		seg.sentTo = nil
		seg.offset = 0
		select {
		case mp.retry <- seg:
		case <-mp.ctx.Done():
			return
		}
	}
	mp.recalcMinMax()
}

func (mp *MultiPipe) receiveLoop(sf *subflowState) {
	defer mp.wg.Done()
	for {
		select {
		case <-mp.ctx.Done():
			return
		default:
		}
		data, err := sf.Transport.Receive(mp.ctx)
		if err != nil {
			if mp.ctx.Err() != nil {
				return
			}
			mp.markDead(sf, "receive: "+err.Error())
			return
		}
		seq, payload, err := decodeFrame(data)
		if err != nil {
			mp.logger.Warn("multi: decode: ", err)
			continue
		}
		mp.handleIncoming(seq, payload)
	}
}

func (mp *MultiPipe) handleIncoming(seq uint64, payload []byte) {
	mp.recvMu.Lock()
	if seq < mp.nextRecvSeq {
		mp.recvMu.Unlock()
		return
	}
	if _, ok := mp.reassembly[seq]; ok {
		mp.recvMu.Unlock()
		return
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	mp.reassembly[seq] = cp

	var out [][]byte
	for {
		d, ok := mp.reassembly[mp.nextRecvSeq]
		if !ok {
			break
		}
		delete(mp.reassembly, mp.nextRecvSeq)
		mp.nextRecvSeq++
		out = append(out, d)
	}
	mp.recvMu.Unlock()

	for _, d := range out {
		select {
		case mp.recvCh <- d:
		case <-mp.ctx.Done():
			return
		}
	}
}

func (mp *MultiPipe) recalcMinMax() {
	min := math.MaxInt
	n := 0
	for _, sf := range mp.subs {
		if sf.dead.Load() {
			continue
		}
		v := sf.Transport.MaxMessageSize()
		if v < min {
			min = v
		}
		n++
	}
	if n == 0 {
		mp.minMax.Store(0)
		return
	}
	min -= frameHeaderSize
	if min < minSendChunk {
		min = minSendChunk
	}
	mp.minMax.Store(int64(min))
}

var _ transport.Transport = (*MultiPipe)(nil)