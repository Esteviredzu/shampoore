package bond

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type simPacket struct {
	data      []byte
	deliverAt time.Time
}

type simLink struct {
	rateBps       float64
	owd           time.Duration
	batchInterval time.Duration

	mu       sync.Mutex
	txFreeAt time.Time

	wire   chan simPacket
	recvCh chan []byte
	done   chan struct{}

	sentBytes atomic.Int64
	recvBytes atomic.Int64
}

func newSimLink(rateBps float64, owd, batchInterval time.Duration) *simLink {
	l := &simLink{
		rateBps:       rateBps,
		owd:           owd,
		batchInterval: batchInterval,
		wire:          make(chan simPacket, 2048),
		recvCh:        make(chan []byte, 2048),
		done:          make(chan struct{}),
	}
	go l.deliverLoop()
	return l
}

func (l *simLink) close() {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
}

func (l *simLink) send(ctx context.Context, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)

	txDur := time.Duration(float64(len(cp)) / l.rateBps * float64(time.Second))
	now := time.Now()

	l.mu.Lock()
	if l.txFreeAt.Before(now) {
		l.txFreeAt = now
	}

	waitUntil := l.txFreeAt.Add(-25 * time.Millisecond)
	l.txFreeAt = l.txFreeAt.Add(txDur)
	deliverAt := l.txFreeAt.Add(l.owd)
	l.mu.Unlock()

	if d := time.Until(waitUntil); d > 0 {
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-l.done:
			t.Stop()
			return io.ErrClosedPipe
		}
	}

	l.sentBytes.Add(int64(len(cp)))
	pkt := simPacket{data: cp, deliverAt: deliverAt}
	select {
	case l.wire <- pkt:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
		return io.ErrClosedPipe
	}
}

func (l *simLink) deliverLoop() {
	if l.batchInterval > 0 {
		l.deliverBatchedLoop()
		return
	}
	for {
		select {
		case <-l.done:
			return
		case pkt := <-l.wire:
			if d := time.Until(pkt.deliverAt); d > 0 {
				t := time.NewTimer(d)
				select {
				case <-t.C:
				case <-l.done:
					t.Stop()
					return
				}
			}
			l.recvBytes.Add(int64(len(pkt.data)))
			select {
			case l.recvCh <- pkt.data:
			case <-l.done:
				return
			}
		}
	}
}

func (l *simLink) deliverBatchedLoop() {
	ticker := time.NewTicker(l.batchInterval)
	defer ticker.Stop()

	var pending []simPacket
	for {
		select {
		case <-l.done:
			return
		case pkt := <-l.wire:
			pending = append(pending, pkt)
		case now := <-ticker.C:
		drain:
			for {
				select {
				case pkt := <-l.wire:
					pending = append(pending, pkt)
				default:
					break drain
				}
			}
			keep := pending[:0]
			for _, pkt := range pending {
				if !now.Before(pkt.deliverAt) {
					l.recvBytes.Add(int64(len(pkt.data)))
					select {
					case l.recvCh <- pkt.data:
					case <-l.done:
						return
					}
				} else {
					keep = append(keep, pkt)
				}
			}
			pending = keep
		}
	}
}

type simTube struct {
	tx      *simLink
	rx      *simLink
	maxSize int
}

func newSimTubePair(mbps float64, rtt, batchInterval time.Duration) (*simTube, *simTube, func()) {
	rateBps := mbps * 1_000_000.0 / 8.0
	owd := rtt / 2
	fwd := newSimLink(rateBps, owd, batchInterval)
	rev := newSimLink(rateBps, owd, batchInterval)
	a := &simTube{tx: fwd, rx: rev, maxSize: 12000}
	b := &simTube{tx: rev, rx: fwd, maxSize: 12000}
	cleanup := func() {
		fwd.close()
		rev.close()
	}
	return a, b, cleanup
}

func (s *simTube) Send(ctx context.Context, data []byte) error {
	return s.tx.send(ctx, data)
}

func (s *simTube) Receive(ctx context.Context) ([]byte, error) {
	select {
	case data := <-s.rx.recvCh:
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.rx.done:
		return nil, io.EOF
	}
}

func (s *simTube) MaxMessageSize() int { return s.maxSize }

type hetSimResult struct {
	goodputMbps     float64
	theoreticalMbps float64
	efficiencyPct   float64
	retransmitPct   float64
	maxUnacked      int
	maxReorder      int
	perTubeBytes    [3]int64
}

func runHeterogeneousSim(
	t testing.TB,
	siloBatch time.Duration,
	warmupDur, measureDur time.Duration,
) hetSimResult {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		ch1Mbps = 20.0
		ch2Mbps = 10.0
		ch3Mbps = 3.0
		sumMbps = ch1Mbps + ch2Mbps + ch3Mbps
	)

	a1, b1, c1 := newSimTubePair(ch1Mbps, 40*time.Millisecond, 0)
	defer c1()
	a2, b2, c2 := newSimTubePair(ch2Mbps, 120*time.Millisecond, 0)
	defer c2()
	a3, b3, c3 := newSimTubePair(ch3Mbps, 900*time.Millisecond, siloBatch)
	defer c3()

	names := []string{"raw_ws", "livekit", "silo"}
	sender := NewBondConn(ctx, names, []tubeLike{a1, a2, a3}, nil)
	defer func() { _ = sender.Close() }()
	receiver := NewBondConn(ctx, names, []tubeLike{b1, b2, b3}, nil)
	defer func() { _ = receiver.Close() }()

	var (
		measuredBytes atomic.Int64
		measuring     atomic.Bool
		integrityErr  atomic.Pointer[error]
		maxUnacked    atomic.Int32
		maxReorder    atomic.Int32
	)

	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				u := int32(sender.unackedCount())
				if u > maxUnacked.Load() {
					maxUnacked.Store(u)
				}
				receiver.mu.Lock()
				r := int32(len(receiver.reorder))
				receiver.mu.Unlock()
				if r > maxReorder.Load() {
					maxReorder.Store(r)
				}
			}
		}
	}()

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buf := make([]byte, 32*1024)
		var expectedByte byte
		for {
			n, err := receiver.ReadContext(ctx, buf)
			if n > 0 {
				for i := 0; i < n; i++ {
					if buf[i] != expectedByte {
						e := io.ErrUnexpectedEOF
						integrityErr.Store(&e)
						return
					}
					expectedByte++
				}
				if measuring.Load() {
					measuredBytes.Add(int64(n))
				}
			}
			if err != nil {
				return
			}
		}
	}()

	writeDone := make(chan error, 1)
	stopWrite := make(chan struct{})
	go func() {
		chunk := make([]byte, sender.MaxPayload())
		var b byte
		for {
			select {
			case <-stopWrite:
				writeDone <- nil
				return
			default:
			}
			for i := range chunk {
				chunk[i] = b
				b++
			}
			if _, err := sender.Write(chunk); err != nil {
				writeDone <- err
				return
			}
		}
	}()

	time.Sleep(warmupDur)

	startPrimary := sender.totalPrimaryBytes.Load()
	startRetrans := sender.totalRetransmitBytes.Load()
	startTube := [3]int64{a1.tx.sentBytes.Load(), a2.tx.sentBytes.Load(), a3.tx.sentBytes.Load()}

	measuring.Store(true)
	start := time.Now()
	time.Sleep(measureDur)
	elapsed := time.Since(start)
	measuring.Store(false)

	endPrimary := sender.totalPrimaryBytes.Load()
	endRetrans := sender.totalRetransmitBytes.Load()
	endTube := [3]int64{a1.tx.sentBytes.Load(), a2.tx.sentBytes.Load(), a3.tx.sentBytes.Load()}

	close(stopWrite)
	cancel()
	<-writeDone
	<-readDone
	<-monitorDone

	if p := integrityErr.Load(); p != nil {
		t.Fatalf("data integrity corruption detected: %v", *p)
	}
	if err := sender.terminalErr(); err != nil {
		t.Fatalf("sender suffered session reset: %v", err)
	}
	if err := receiver.terminalErr(); err != nil {
		t.Fatalf("receiver suffered session reset: %v", err)
	}

	gotBytes := measuredBytes.Load()
	goodputMbps := (float64(gotBytes) * 8.0) / elapsed.Seconds() / 1_000_000.0
	effPct := (goodputMbps / sumMbps) * 100.0

	dPrimary := endPrimary - startPrimary
	dRetrans := endRetrans - startRetrans
	var retransPct float64
	if dPrimary > 0 {
		retransPct = float64(dRetrans) / float64(dPrimary) * 100.0
	}

	return hetSimResult{
		goodputMbps:     goodputMbps,
		theoreticalMbps: sumMbps,
		efficiencyPct:   effPct,
		retransmitPct:   retransPct,
		maxUnacked:      int(maxUnacked.Load()),
		maxReorder:      int(maxReorder.Load()),
		perTubeBytes: [3]int64{
			endTube[0] - startTube[0],
			endTube[1] - startTube[1],
			endTube[2] - startTube[2],
		},
	}
}

func TestBondHeterogeneousThroughput(t *testing.T) {
	res := runHeterogeneousSim(t, 0, 2500*time.Millisecond, 5*time.Second)

	t.Logf("heterogeneous bond: goodput=%.2f/%.2f Mbps (%.1f%%), retransmit=%.2f%%, maxUnacked=%d, maxReorder=%d, tubes=[raw_ws=%dKB, livekit=%dKB, silo=%dKB]",
		res.goodputMbps, res.theoreticalMbps, res.efficiencyPct,
		res.retransmitPct, res.maxUnacked, res.maxReorder,
		res.perTubeBytes[0]/1024, res.perTubeBytes[1]/1024, res.perTubeBytes[2]/1024,
	)

	if res.efficiencyPct < 80.0 {
		t.Fatalf("bond efficiency = %.1f%% (%.2f/%.2f Mbps), want >= 80%%",
			res.efficiencyPct, res.goodputMbps, res.theoreticalMbps)
	}
	if res.retransmitPct > 5.0 {
		t.Fatalf("retransmit overhead = %.2f%%, want <= 5%%", res.retransmitPct)
	}
	for i, name := range []string{"raw_ws", "livekit", "silo"} {
		if res.perTubeBytes[i] == 0 {
			t.Fatalf("subflow %s carried 0 bytes", name)
		}
	}
}

func TestBondHeterogeneousWithS3Batching(t *testing.T) {
	res := runHeterogeneousSim(t, 250*time.Millisecond, 2500*time.Millisecond, 5*time.Second)

	t.Logf("heterogeneous bond (S3 250ms batch): goodput=%.2f/%.2f Mbps (%.1f%%), retransmit=%.2f%%, maxUnacked=%d, maxReorder=%d, tubes=[raw_ws=%dKB, livekit=%dKB, silo=%dKB]",
		res.goodputMbps, res.theoreticalMbps, res.efficiencyPct,
		res.retransmitPct, res.maxUnacked, res.maxReorder,
		res.perTubeBytes[0]/1024, res.perTubeBytes[1]/1024, res.perTubeBytes[2]/1024,
	)

	if res.efficiencyPct < 80.0 {
		t.Fatalf("bond (S3 batch) efficiency = %.1f%% (%.2f/%.2f Mbps), want >= 80%%",
			res.efficiencyPct, res.goodputMbps, res.theoreticalMbps)
	}
	if res.retransmitPct > 6.0 {
		t.Fatalf("retransmit overhead = %.2f%%, want <= 6%%", res.retransmitPct)
	}
}

func BenchmarkBondHeterogeneous(b *testing.B) {
	b.ResetTimer()
	var totalEff, totalGoodput, totalRetrans float64
	var maxReorder int
	for i := 0; i < b.N; i++ {
		res := runHeterogeneousSim(b, 0, 2*time.Second, 3*time.Second)
		totalEff += res.efficiencyPct
		totalGoodput += res.goodputMbps
		totalRetrans += res.retransmitPct
		if res.maxReorder > maxReorder {
			maxReorder = res.maxReorder
		}
	}
	n := float64(b.N)
	b.ReportMetric(totalGoodput/n, "Mbps")
	b.ReportMetric(totalEff/n, "efficiency_%")
	b.ReportMetric(totalRetrans/n, "retransmit_%")
	b.ReportMetric(float64(maxReorder), "max_reorder")
}
