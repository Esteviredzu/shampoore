package bond

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	queueCap = 64

	minSampleRTT = 500 * time.Microsecond
	maxSampleRTT = 30 * time.Second
	defaultSRTT  = 100 * time.Millisecond

	minBandwidthBps     = 16 * 1024.0
	defaultBandwidthBps = 1024 * 1024.0

	bwWindowBuckets  = 12
	minRTTBuckets    = 3
	minRTTBucketSpan = 10 * time.Second

	inflightBDPFactor = 1.35
	minInflightChunks = 6
	reorderGuardRatio = 0.55
)

type bwSample struct {
	at   time.Time
	rate float64
}

type minRTTSample struct {
	at  time.Time
	rtt time.Duration
}

type tubeMetrics struct {
	mu sync.Mutex

	srtt       time.Duration
	rttvar     time.Duration
	rttSamples int

	minRTTHist [minRTTBuckets]minRTTSample
	minRTTIdx  int

	bwHist    [bwWindowBuckets]bwSample
	bwIdx     int
	btlBw     float64
	bwSamples int

	deliveredBytes int64
	deliveredTime  time.Time
}

func (m *tubeMetrics) recordRTT(now time.Time, rtt time.Duration) {
	if rtt < minSampleRTT {
		rtt = minSampleRTT
	}
	if rtt > maxSampleRTT {
		rtt = maxSampleRTT
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.rttSamples == 0 {
		m.srtt = rtt
		m.rttvar = rtt / 2
	} else {
		diff := m.srtt - rtt
		if diff < 0 {
			diff = -diff
		}

		m.rttvar = (3*m.rttvar + diff) / 4
		m.srtt = (7*m.srtt + rtt) / 8
	}
	m.rttSamples++

	cur := &m.minRTTHist[m.minRTTIdx]
	if cur.at.IsZero() || now.Sub(cur.at) >= minRTTBucketSpan {
		m.minRTTIdx = (m.minRTTIdx + 1) % minRTTBuckets
		m.minRTTHist[m.minRTTIdx] = minRTTSample{at: now, rtt: rtt}
	} else if rtt < cur.rtt {
		cur.rtt = rtt
	}
}

func (m *tubeMetrics) rttStats(now time.Time) (srtt, rttvar, minRTT time.Duration, samples int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rttSamples == 0 {
		return defaultSRTT, defaultSRTT / 2, defaultSRTT, 0
	}
	srtt = m.srtt
	rttvar = m.rttvar
	minRTT = srtt
	cutoff := now.Add(-minRTTBuckets * minRTTBucketSpan)
	for _, s := range m.minRTTHist {
		if !s.at.IsZero() && s.at.After(cutoff) && s.rtt > 0 && s.rtt < minRTT {
			minRTT = s.rtt
		}
	}
	return srtt, rttvar, minRTT, m.rttSamples
}

func (m *tubeMetrics) deliverySnapshot() (int64, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deliveredBytes, m.deliveredTime
}

func (m *tubeMetrics) recordDelivery(now time.Time, size int, sentAt time.Time, delivered0 int64, deliveredTime0 time.Time, retransmitted bool) {
	if size <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.deliveredBytes += int64(size)
	m.deliveredTime = now

	if retransmitted {
		return
	}

	dt := now.Sub(sentAt)
	if !deliveredTime0.IsZero() {
		if ackDt := now.Sub(deliveredTime0); ackDt > dt {
			dt = ackDt
		}
	}
	if dt < minSampleRTT {
		dt = minSampleRTT
	}
	deltaBytes := m.deliveredBytes - delivered0
	if deltaBytes < int64(size) {
		deltaBytes = int64(size)
	}
	rate := float64(deltaBytes) / dt.Seconds()
	if rate < minBandwidthBps {
		rate = minBandwidthBps
	}

	window := 2 * time.Second
	if m.rttSamples > 0 {
		if w := 6 * m.srtt; w > window {
			window = w
		}
	}
	bucketSpan := window / bwWindowBuckets
	if bucketSpan < 50*time.Millisecond {
		bucketSpan = 50 * time.Millisecond
	}

	cur := &m.bwHist[m.bwIdx]
	if cur.at.IsZero() || now.Sub(cur.at) >= bucketSpan {
		m.bwIdx = (m.bwIdx + 1) % bwWindowBuckets
		m.bwHist[m.bwIdx] = bwSample{at: now, rate: rate}
	} else if rate > cur.rate {
		cur.rate = rate
	}
	m.bwSamples++

	cutoff := now.Add(-window)
	var maxRate float64
	for _, s := range m.bwHist {
		if !s.at.IsZero() && s.at.After(cutoff) && s.rate > maxRate {
			maxRate = s.rate
		}
	}
	if maxRate > 0 {
		m.btlBw = maxRate
	}
}

func (m *tubeMetrics) bandwidth(now time.Time) (float64, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.bwSamples == 0 {
		return defaultBandwidthBps, 0
	}
	window := 2 * time.Second
	if m.rttSamples > 0 {
		if w := 6 * m.srtt; w > window {
			window = w
		}
	}
	cutoff := now.Add(-window)
	var maxRate float64
	for _, s := range m.bwHist {
		if !s.at.IsZero() && s.at.After(cutoff) && s.rate > maxRate {
			maxRate = s.rate
		}
	}
	if maxRate > 0 {
		m.btlBw = maxRate
	}
	if m.btlBw < minBandwidthBps {
		return minBandwidthBps, m.bwSamples
	}
	return m.btlBw, m.bwSamples
}

type subflow struct {
	name string
	tube tubeLike

	queue      chan queuedChunk
	workerDone chan struct{}

	deadUntil     atomic.Int64
	errorCount    atomic.Int64
	sentBytes     atomic.Int64
	inflight      atomic.Int32
	queuedBytes   atomic.Int64
	inflightBytes atomic.Int64
	probeSeq      atomic.Uint32

	confirmed atomic.Bool

	metrics tubeMetrics
}

type scheduler struct {
	subflows []*subflow
	cursor   atomic.Uint32
}

func newScheduler(names []string, tubes []tubeLike) *scheduler {
	s := &scheduler{}
	for i := range tubes {
		s.subflows = append(s.subflows, &subflow{
			name:       names[i],
			tube:       tubes[i],
			queue:      make(chan queuedChunk, queueCap),
			workerDone: make(chan struct{}),
		})
	}
	return s
}

func (s *scheduler) MarkDead(sf *subflow, _ error) {

	n := sf.errorCount.Add(1)
	shift := n - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 4 {
		shift = 4
	}
	backoff := time.Second << shift
	if backoff > 10*time.Second {
		backoff = 10 * time.Second
	}
	sf.deadUntil.Store(time.Now().Add(backoff).UnixNano())
}

func (s *scheduler) MarkAlive(sf *subflow) {
	sf.errorCount.Store(0)
	sf.deadUntil.Store(0)
	sf.confirmed.Store(true)
}

func (s *scheduler) pickNonFull(exclude *subflow, preferConfirmed bool) *subflow {
	return s.pick(exclude, nil, preferConfirmed)
}

func (s *scheduler) pickAlive(exclude map[*subflow]struct{}) *subflow {
	return s.pick(nil, exclude, true)
}

func (s *scheduler) pick(skipOne *subflow, skipSet map[*subflow]struct{}, preferConfirmed bool) *subflow {
	if len(s.subflows) == 0 {
		return nil
	}
	start := int(s.cursor.Add(1) - 1)
	if preferConfirmed {
		if sf := s.scan(start, skipOne, skipSet, scanConfirmed); sf != nil {
			return sf
		}
	}
	return s.scan(start, skipOne, skipSet, scanAny)
}

type scanFilter uint8

const (
	scanAny scanFilter = iota
	scanConfirmed
)

func (s *scheduler) scan(start int, skipOne *subflow, skipSet map[*subflow]struct{}, filter scanFilter) *subflow {
	now := time.Now().UnixNano()
	for i := 0; i < len(s.subflows); i++ {
		sf := s.subflows[(start+i)%len(s.subflows)]
		if sf == skipOne {
			continue
		}
		if skipSet != nil {
			if _, skip := skipSet[sf]; skip {
				continue
			}
		}
		if sf.deadUntil.Load() > now {
			continue
		}
		if filter == scanConfirmed && !sf.confirmed.Load() {
			continue
		}
		if len(sf.queue) >= cap(sf.queue) {
			continue
		}
		return sf
	}
	return nil
}

func (s *scheduler) RecordRTT(sf *subflow, rtt time.Duration) {
	if sf == nil {
		return
	}
	sf.metrics.recordRTT(time.Now(), rtt)
}

func (s *scheduler) RecordDelivery(sf *subflow, size int, sentAt time.Time, delivered0 int64, deliveredTime0 time.Time, retransmitted bool) {
	if sf == nil {
		return
	}
	sf.metrics.recordDelivery(time.Now(), size, sentAt, delivered0, deliveredTime0, retransmitted)
}

func (s *scheduler) rtoFor(sf *subflow) time.Duration {
	if sf == nil {
		return 200 * time.Millisecond
	}
	srtt, rttvar, _, samples := sf.metrics.rttStats(time.Now())
	if samples == 0 {
		return 200 * time.Millisecond
	}
	margin := 4 * rttvar
	if margin < 50*time.Millisecond {
		margin = 50 * time.Millisecond
	}
	return srtt + margin
}

func (s *scheduler) expectedNackDelay(sf *subflow) (time.Duration, bool) {
	if sf == nil {
		return 0, false
	}
	now := time.Now()
	srtt, rttvar, _, samples := sf.metrics.rttStats(now)
	if samples == 0 {
		return 0, false
	}
	fastSRTT := srtt
	nowNs := now.UnixNano()
	for _, cand := range s.subflows {
		if cand.deadUntil.Load() > nowNs {
			continue
		}
		cSRTT, _, _, cSamples := cand.metrics.rttStats(now)
		if cSamples > 0 && cSRTT < fastSRTT {
			fastSRTT = cSRTT
		}
	}
	return srtt/2 + fastSRTT/2 + 2*rttvar + 120*time.Millisecond, true
}

func (s *scheduler) totalBandwidthAndActiveMaxRTT() (totalBW float64, activeMaxRTT time.Duration, measured bool) {
	now := time.Now()
	nowNs := now.UnixNano()
	var maxAllRTT time.Duration
	for _, sf := range s.subflows {
		if sf.deadUntil.Load() > nowNs {
			continue
		}
		bw, bwSamples := sf.metrics.bandwidth(now)
		srtt, rttvar, _, rttSamples := sf.metrics.rttStats(now)
		if bwSamples > 0 {
			measured = true
		}
		totalBW += bw
		effRTT := srtt + 2*rttvar
		if rttSamples > 0 {
			if effRTT > maxAllRTT {
				maxAllRTT = effRTT
			}
			if sf.inflightBytes.Load() > 0 || sf.queuedBytes.Load() > 0 {
				if effRTT > activeMaxRTT {
					activeMaxRTT = effRTT
				}
			}
		}
	}
	if activeMaxRTT == 0 {
		activeMaxRTT = maxAllRTT
	}
	if activeMaxRTT == 0 {
		activeMaxRTT = defaultSRTT
	}
	return totalBW, activeMaxRTT, measured
}

func (s *scheduler) pickDelayAware(chunkSize, maxReorder int, preferConfirmed bool) *subflow {
	if len(s.subflows) == 0 {
		return nil
	}
	now := time.Now()
	nowNs := now.UnixNano()

	if preferConfirmed {
		if sf := s.pickDelayAwarePass(now, nowNs, chunkSize, maxReorder, scanConfirmed); sf != nil {
			return sf
		}
	}
	return s.pickDelayAwarePass(now, nowNs, chunkSize, maxReorder, scanAny)
}

func (s *scheduler) pickDelayAwarePass(now time.Time, nowNs int64, chunkSize, maxReorder int, filter scanFilter) *subflow {
	if chunkSize <= 0 {
		chunkSize = 1024
	}
	type candInfo struct {
		sf      *subflow
		arrival float64
		bw      float64
		srtt    time.Duration
		capped  bool
	}

	cands := make([]candInfo, 0, len(s.subflows))
	var totalBW float64
	minArrival := math.MaxFloat64

	var fastSRTT time.Duration
	for _, sf := range s.subflows {
		if sf.deadUntil.Load() > nowNs {
			continue
		}
		srtt, _, _, rttSamples := sf.metrics.rttStats(now)
		if rttSamples > 0 && (fastSRTT == 0 || srtt < fastSRTT) {
			fastSRTT = srtt
		}
	}

	for _, sf := range s.subflows {
		if sf.deadUntil.Load() > nowNs {
			continue
		}
		if filter == scanConfirmed && !sf.confirmed.Load() {
			continue
		}
		if len(sf.queue) >= cap(sf.queue) {
			continue
		}

		srtt, rttvar, minRTT, rttSamples := sf.metrics.rttStats(now)
		bw, bwSamples := sf.metrics.bandwidth(now)
		totalBW += bw

		queued := sf.queuedBytes.Load()
		inflight := sf.inflightBytes.Load()
		totalOutstanding := queued + inflight

		capped := false
		minCap := float64(minInflightChunks * chunkSize)
		if rttSamples > 0 && bwSamples > 0 {

			retOWD := srtt / 2
			if fastSRTT > 0 && fastSRTT/2 < retOWD {
				retOWD = fastSRTT / 2
			}
			loopRTT := srtt/2 + retOWD + rttvar + 30*time.Millisecond
			bdpCap := inflightBDPFactor * bw * loopRTT.Seconds()
			capBytes := bdpCap
			if capBytes < minCap {
				capBytes = minCap
			}
			if float64(totalOutstanding) >= capBytes {
				capped = true
			}
		} else if len(s.subflows) > 1 {

			if float64(totalOutstanding) >= minCap {
				capped = true
			}
		}

		wireBDP := bw * minRTT.Seconds()
		inflightExcess := float64(inflight) - wireBDP
		if inflightExcess < 0 {
			inflightExcess = 0
		}
		queueDelay := (float64(queued) + inflightExcess + float64(chunkSize)) / bw
		arrival := queueDelay + srtt.Seconds()/2.0
		if arrival < minArrival {
			minArrival = arrival
		}

		cands = append(cands, candInfo{
			sf:      sf,
			arrival: arrival,
			bw:      bw,
			srtt:    srtt,
			capped:  capped,
		})
	}

	if len(cands) == 0 {
		return nil
	}

	bestIdx := -1
	bestArrival := math.MaxFloat64
	start := int(s.cursor.Add(1)-1) % len(cands)
	for offset := 0; offset < len(cands); offset++ {
		idx := (start + offset) % len(cands)
		c := cands[idx]
		if c.capped {
			continue
		}
		if maxReorder > 0 && len(cands) > 1 {
			deltaT := c.arrival - minArrival
			otherBW := totalBW - c.bw
			if deltaT > 0 && otherBW > 0 {
				estReorderChunks := (deltaT * otherBW) / float64(chunkSize)
				if estReorderChunks > float64(maxReorder)*reorderGuardRatio {
					if c.sf.inflightBytes.Load() > 0 || c.sf.queuedBytes.Load() > 0 {
						continue
					}
				}
			}
		}
		if c.arrival < bestArrival {
			bestArrival = c.arrival
			bestIdx = idx
		}
	}

	if bestIdx >= 0 {
		return cands[bestIdx].sf
	}

	anyMeasured := false
	for _, c := range cands {
		_, _, _, rttN := c.sf.metrics.rttStats(now)
		_, bwN := c.sf.metrics.bandwidth(now)
		if rttN > 0 || bwN > 0 {
			anyMeasured = true
			break
		}
	}
	if !anyMeasured {
		return cands[start].sf
	}
	for offset := 0; offset < len(cands); offset++ {
		idx := (start + offset) % len(cands)
		if cands[idx].sf.inflightBytes.Load() == 0 && cands[idx].sf.queuedBytes.Load() == 0 {
			return cands[idx].sf
		}
	}
	return nil
}

func (s *scheduler) pickFastest(exclude *subflow) *subflow {
	if len(s.subflows) == 0 {
		return nil
	}
	now := time.Now()
	nowNs := now.UnixNano()
	for pass := 0; pass < 2; pass++ {
		var best *subflow
		var bestRTT time.Duration
		for _, sf := range s.subflows {
			if sf == exclude {
				continue
			}
			if sf.deadUntil.Load() > nowNs {
				continue
			}
			if pass == 0 && !sf.confirmed.Load() {
				continue
			}
			srtt, _, _, _ := sf.metrics.rttStats(now)
			if best == nil || srtt < bestRTT {
				best = sf
				bestRTT = srtt
			}
		}
		if best != nil {
			return best
		}
	}
	return nil
}

func (s *scheduler) pickControl() *subflow {
	if sf := s.pickFastest(nil); sf != nil {
		return sf
	}
	if len(s.subflows) == 0 {
		return nil
	}
	start := int(s.cursor.Add(1) - 1)
	now := time.Now().UnixNano()
	for pass := 0; pass < 2; pass++ {
		for i := 0; i < len(s.subflows); i++ {
			sf := s.subflows[(start+i)%len(s.subflows)]
			if sf.deadUntil.Load() > now {
				continue
			}
			if pass == 0 && !sf.confirmed.Load() {
				continue
			}
			return sf
		}
	}
	return nil
}

func (s *scheduler) isDead(sf *subflow) bool {
	return sf.deadUntil.Load() > time.Now().UnixNano()
}

func (s *scheduler) empty() bool {
	for _, sf := range s.subflows {
		if len(sf.queue) > 0 || sf.inflight.Load() > 0 {
			return false
		}
	}
	return true
}

func (s *scheduler) allDead() bool {
	now := time.Now().UnixNano()
	for _, sf := range s.subflows {
		if sf.deadUntil.Load() <= now {
			return false
		}
	}
	return true
}

type SubflowSnapshot struct {
	Name          string
	QueueLen      int
	QueueCap      int
	SentBytes     int64
	Dead          bool
	Pending       int
	Confirmed     bool
	SRTT          time.Duration
	RTTVAR        time.Duration
	BandwidthBps  int64
	InflightBytes int64
}

func (s *scheduler) Snapshot() []SubflowSnapshot {
	now := time.Now()
	nowNs := now.UnixNano()
	out := make([]SubflowSnapshot, 0, len(s.subflows))
	for _, sf := range s.subflows {
		srtt, rttvar, _, samples := sf.metrics.rttStats(now)
		if samples == 0 {
			srtt = 0
			rttvar = 0
		}
		bw, bwSamples := sf.metrics.bandwidth(now)
		var bwInt int64
		if bwSamples > 0 {
			bwInt = int64(bw)
		}
		out = append(out, SubflowSnapshot{
			Name:          sf.name,
			QueueLen:      len(sf.queue),
			QueueCap:      cap(sf.queue),
			SentBytes:     sf.sentBytes.Load(),
			Dead:          sf.deadUntil.Load() > nowNs,
			Pending:       int(sf.inflight.Load()),
			Confirmed:     sf.confirmed.Load(),
			SRTT:          srtt,
			RTTVAR:        rttvar,
			BandwidthBps:  bwInt,
			InflightBytes: sf.inflightBytes.Load(),
		})
	}
	return out
}
