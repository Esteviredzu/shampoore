package flowmux

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	coldStartFloor int64 = 1024

	probeDuration = 30 * time.Second

	softProbeEvery = 30

	fastThresholdNum = 1
	fastThresholdDen = 2

	logEvery = 15

	subflowFailCooldown = 5 * time.Second

	rttMedianWindow    = 5
	bwPeakTicks        = 3
	scoreRefRTT        = 200 * time.Millisecond
	maxLatencyPenalty  = 10.0
	errDecayHalfLife   = 30 * time.Second
	errPenaltyStrength = 0.7
	minReliability     = 0.05
)

type Config struct {
	FlushThreshold int
	FlushInterval  time.Duration
	SampleInterval time.Duration
	StatsTTL       time.Duration

	UserID     string
	ExitNodeID string
}

func DefaultConfig() Config {
	return Config{
		FlushThreshold: 12000,
		FlushInterval:  20 * time.Millisecond,
		SampleInterval: 2 * time.Second,
		StatsTTL:       transport.DefaultStatsCacheTTL,
	}
}

type subflow struct {
	name      string
	transport transport.Transport
	mux       *tunnel.TunnelMultiplexer

	throughput  atomic.Int64
	recentRate  atomic.Int64
	active      atomic.Int64
	totalOpened atomic.Uint64
	failUntil   atomic.Int64

	healthMu     sync.Mutex
	rttHist      [rttMedianWindow]time.Duration
	rttHistCount int
	rttHistIdx   int
	medianRTT    time.Duration
	rttvar       time.Duration
	queuedBytes  int64
	bwHist       [bwPeakTicks]int64
	bwHistIdx    int
	lastResets   uint64
	errorRate    float64

	lastSentBytes int64
	lastRecvBytes int64
	lastSampleAt  time.Time
}

type cachedLayers struct {
	at     time.Time
	layers []transport.Layer
}

type FlowMux struct {
	subflows []*subflow
	logger   logging.ContextLogger
	cfg      Config

	incoming    chan multiplexing.Stream
	pickCount   atomic.Uint32
	firstOpenAt atomic.Int64
	layerCache  atomic.Pointer[cachedLayers]

	ctx    context.Context
	cancel context.CancelFunc

	stateMu sync.Mutex
	started bool
	wg      sync.WaitGroup
}

var _ multiplexing.StreamMultiplexer = (*FlowMux)(nil)
var _ transport.Telemetry = (*FlowMux)(nil)

func New(
	transports []transport.Transport,
	names []string,
	logger logging.ContextLogger,
	cfg Config,
) (*FlowMux, error) {
	if len(transports) == 0 {
		return nil, fmt.Errorf("flowmux: no transports")
	}
	if len(names) != len(transports) {
		return nil, fmt.Errorf("flowmux: names length mismatch")
	}
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = 2 * time.Second
	}
	if cfg.StatsTTL <= 0 {
		cfg.StatsTTL = transport.DefaultStatsCacheTTL
	}
	m := &FlowMux{
		logger:   logger,
		cfg:      cfg,
		incoming: make(chan multiplexing.Stream, 64),
	}
	for i := range transports {
		tm := tunnel.New(transports[i], logger, tunnel.Config{
			FlushThreshold: cfg.FlushThreshold,
			FlushInterval:  cfg.FlushInterval,
			UserID:         cfg.UserID,
			ExitNodeID:     cfg.ExitNodeID,
		})
		m.subflows = append(m.subflows, &subflow{
			name:         names[i],
			transport:    transports[i],
			mux:          tm,
			lastSampleAt: time.Now(),
		})
	}
	return m, nil
}

func (m *FlowMux) NotifyNetworkChanged() {
	for _, sf := range m.subflows {
		if n, ok := sf.transport.(interface{ NotifyNetworkChanged() }); ok {
			n.NotifyNetworkChanged()
		}
	}
}

func (m *FlowMux) Start(ctx context.Context) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	if m.started {
		return nil
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.drainIncomingStreams()
	for _, sf := range m.subflows {
		if err := sf.mux.Start(ctx); err != nil {
			for _, s := range m.subflows {
				_ = s.mux.Stop(context.Background())
			}
			m.cancel()
			m.ctx = nil
			m.cancel = nil
			return fmt.Errorf("flowmux: start %s: %w", sf.name, err)
		}
		m.logger.Info("flowmux: subflow ", sf.name, " started")
	}
	for _, sf := range m.subflows {
		m.wg.Add(1)
		go m.acceptLoop(sf)
	}
	m.wg.Add(1)
	go m.sampleLoop()
	m.started = true
	return nil
}

func (m *FlowMux) Stop(ctx context.Context) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	if !m.started {
		return nil
	}
	m.started = false
	m.layerCache.Store(nil)
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	var lastErr error
	for i := len(m.subflows) - 1; i >= 0; i-- {
		if err := m.subflows[i].mux.Stop(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (m *FlowMux) WalkLayers() []transport.Layer {
	now := time.Now()
	if cached := m.layerCache.Load(); cached != nil && now.Sub(cached.at) < m.cfg.StatsTTL {
		return cached.layers
	}
	layers := m.walkLayers()
	m.layerCache.Store(&cachedLayers{at: now, layers: layers})
	return layers
}

func (m *FlowMux) walkLayers() []transport.Layer {
	var appSent, appRecv int64
	var wires []transport.Layer
	for _, sf := range m.subflows {
		for _, l := range sf.mux.WalkLayers() {
			switch l.Kind {
			case "mux":
				appSent += l.Snapshot.UsefulSent
				appRecv += l.Snapshot.UsefulRecv
			case "pipe":
				if l.Name == "" {
					l.Name = sf.name
				}
				wires = append(wires, l)
			}
		}
	}
	agg := transport.Layer{
		Kind: "mux", Type: "flowmux",
		Snapshot: transport.Snapshot{
			UsefulSent: appSent, UsefulRecv: appRecv,
			Timestamp: time.Now(),
		},
	}
	return append([]transport.Layer{agg}, wires...)
}

func (m *FlowMux) OpenStream(ctx context.Context, meta map[string]any) (multiplexing.Stream, error) {
	if len(m.subflows) == 0 {
		return nil, fmt.Errorf("flowmux: no subflow")
	}
	var lastErr error
	for attempt := 0; attempt < len(m.subflows); attempt++ {
		sf := m.pickSubflow()
		if sf == nil {
			break
		}
		s, err := sf.mux.OpenStream(ctx, meta)
		if err == nil {
			sf.failUntil.Store(0)
			sf.active.Add(1)
			m.logger.Info("flowmux: new stream on ", sf.name,
				" (active=", sf.active.Load(),
				" tp=", sf.throughput.Load(), " B/s)")
			return &wrappedStream{Stream: s, sf: sf}, nil
		}
		lastErr = err
		sf.recordError(1.0)
		sf.failUntil.Store(time.Now().Add(subflowFailCooldown).UnixNano())
		m.logger.Warn("flowmux: open on ", sf.name, " failed, trying another subflow: ", err)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("flowmux: no healthy subflow")
	}
	return nil, fmt.Errorf("flowmux: open failed on all subflows: %w", lastErr)
}

func (m *FlowMux) AcceptStream(ctx context.Context) (multiplexing.Stream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case s, ok := <-m.incoming:
		if !ok {
			return nil, fmt.Errorf("flowmux: closed")
		}
		return s, nil
	}
}

type wrappedStream struct {
	multiplexing.Stream
	sf     *subflow
	closed atomic.Bool
}

func (w *wrappedStream) Read(p []byte) (int, error) {
	n, err := w.Stream.Read(p)
	if err != nil && w.closed.CompareAndSwap(false, true) {
		w.sf.active.Add(-1)
	}
	return n, err
}

func (w *wrappedStream) Close() error {
	if w.closed.CompareAndSwap(false, true) {
		w.sf.active.Add(-1)
	}
	return w.Stream.Close()
}

func (m *FlowMux) drainIncomingStreams() {
	for {
		select {
		case s := <-m.incoming:
			if err := s.Close(); err != nil {
				m.logger.Debug("flowmux: drain incoming stream: ", err)
			}
		default:
			return
		}
	}
}

func (m *FlowMux) acceptLoop(sf *subflow) {
	defer m.wg.Done()
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}
		s, err := sf.mux.AcceptStream(m.ctx)
		if err != nil {
			if m.ctx.Err() != nil {
				return
			}
			m.logger.Warn("flowmux: accept on ", sf.name, ": ", err)
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		sf.active.Add(1)
		sf.totalOpened.Add(1)
		wrapped := &wrappedStream{Stream: s, sf: sf}
		select {
		case m.incoming <- wrapped:
		case <-m.ctx.Done():
			_ = wrapped.Close()
			return
		}
	}
}

func (m *FlowMux) pickSubflow() *subflow {
	now := time.Now().UnixNano()
	candidates := m.candidates(now)
	n := len(candidates)
	if n == 0 {
		m.logger.Warn("flowmux: every subflow is in failure cooldown")
		return nil
	}
	if n == 1 {
		candidates[0].totalOpened.Add(1)
		return candidates[0]
	}

	count := m.pickCount.Add(1)
	if count == 1 {
		m.firstOpenAt.Store(time.Now().UnixNano())
	}

	firstNs := m.firstOpenAt.Load()
	if firstNs > 0 && time.Since(time.Unix(0, firstNs)) < probeDuration {
		sf := candidates[(count-1)%uint32(n)]
		sf.totalOpened.Add(1)
		return sf
	}

	if count%softProbeEvery == 0 {
		sf := candidates[rand.IntN(n)]
		sf.totalOpened.Add(1)
		m.logger.Info("flowmux: soft-probe -> ", sf.name,
			" (tp=", sf.throughput.Load(), " B/s)")
		return sf
	}

	var totalActive int64
	for _, sf := range candidates {
		totalActive += sf.active.Load()
	}
	avgActive := float64(totalActive) / float64(len(candidates))
	if avgActive < 1.0 {
		avgActive = 1.0
	}

	scores := make([]float64, len(candidates))
	var maxScore float64
	for i, sf := range candidates {
		sc := sf.compositeScore(avgActive)
		scores[i] = sc
		if sc > maxScore {
			maxScore = sc
		}
	}
	threshold := maxScore * float64(fastThresholdNum) / float64(fastThresholdDen)
	if threshold < float64(coldStartFloor)*minReliability {
		threshold = float64(coldStartFloor) * minReliability
	}

	var pool []*subflow
	var weights []float64
	var total float64
	for i, sf := range candidates {
		if scores[i] >= threshold {
			pool = append(pool, sf)
			weights = append(weights, scores[i])
			total += scores[i]
		}
	}
	if len(pool) == 0 {
		pool = candidates
		weights = scores
		for _, sc := range scores {
			total += sc
		}
	}
	if total <= 0 {
		sf := pool[rand.IntN(len(pool))]
		sf.totalOpened.Add(1)
		return sf
	}

	r := rand.Float64() * total
	var cum float64
	for i, w := range weights {
		cum += w
		if r < cum {
			pool[i].totalOpened.Add(1)
			return pool[i]
		}
	}
	sf := pool[len(pool)-1]
	sf.totalOpened.Add(1)
	return sf
}

func (m *FlowMux) candidates(now int64) []*subflow {
	alive := make([]*subflow, 0, len(m.subflows))
	for _, sf := range m.subflows {
		if sf.failUntil.Load() > now {
			continue
		}
		alive = append(alive, sf)
	}
	if len(alive) == 0 {
		return append([]*subflow(nil), m.subflows...)
	}
	return alive
}

func (m *FlowMux) sampleLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.SampleInterval)
	defer ticker.Stop()

	tick := 0
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Now()
		for _, sf := range m.subflows {
			var sentBytes, recvBytes int64
			for _, l := range sf.mux.WalkLayers() {
				if l.Kind == "mux" {
					sentBytes = l.Snapshot.UsefulSent
					recvBytes = l.Snapshot.UsefulRecv
					break
				}
			}
			dt := now.Sub(sf.lastSampleAt).Seconds()
			if dt > 0 {
				dSent := sentBytes - sf.lastSentBytes
				dRecv := recvBytes - sf.lastRecvBytes
				sample := dSent
				if dRecv > sample {
					sample = dRecv
				}
				if sample < 0 {
					sample = 0
				}
				bps := int64(float64(sample) / dt)
				sf.recentRate.Store(bps)

				effBps := sf.recordBWSample(bps)
				old := sf.throughput.Load()
				sf.throughput.Store(nextBandwidthScore(old, effBps, sf.active.Load()))
				sf.updateHealth(sf.mux.Health(), time.Duration(dt*float64(time.Second)))
			}
			sf.lastSentBytes = sentBytes
			sf.lastRecvBytes = recvBytes
			sf.lastSampleAt = now
		}

		tick++
		firstNs := m.firstOpenAt.Load()
		inProbe := firstNs > 0 && time.Since(time.Unix(0, firstNs)) < probeDuration+2*time.Second
		if inProbe || tick%logEvery == 0 {
			m.logThroughputs(inProbe)
		}
	}
}

func (m *FlowMux) logThroughputs(inProbe bool) {
	tag := "steady"
	if inProbe {
		tag = "PROBE"
	}
	var totalStreams uint64
	var totalBytes int64
	for _, sf := range m.subflows {
		totalStreams += sf.totalOpened.Load()
		for _, l := range sf.mux.WalkLayers() {
			if l.Kind == "mux" {
				totalBytes += l.Snapshot.UsefulSent + l.Snapshot.UsefulRecv
				break
			}
		}
	}

	var maxTP int64
	for _, sf := range m.subflows {
		if tp := sf.throughput.Load(); tp > maxTP {
			maxTP = tp
		}
	}
	threshold := maxTP * fastThresholdNum / fastThresholdDen

	parts := make([]string, len(m.subflows))
	for i, sf := range m.subflows {
		var bSent, bRecv int64
		for _, l := range sf.mux.WalkLayers() {
			if l.Kind == "mux" {
				bSent = l.Snapshot.UsefulSent
				bRecv = l.Snapshot.UsefulRecv
				break
			}
		}
		streams := sf.totalOpened.Load()
		streamShare := 0.0
		if totalStreams > 0 {
			streamShare = float64(streams) / float64(totalStreams) * 100
		}
		bytes := bSent + bRecv
		byteShare := 0.0
		if totalBytes > 0 {
			byteShare = float64(bytes) / float64(totalBytes) * 100
		}
		excluded := ""
		if sf.throughput.Load() < threshold {
			excluded = " EXCLUDED"
		}
		parts[i] = fmt.Sprintf(
			"%s{tp=%dB/s recent=%dB/s streams=%d(%.0f%%) bytes=%d(%.0f%%) active=%d%s}",
			sf.name, sf.throughput.Load(), sf.recentRate.Load(),
			streams, streamShare, bytes, byteShare, sf.active.Load(), excluded)
	}
	m.logger.Info("flowmux[", tag, "] threshold=", threshold,
		"B/s: ", joinStrings(parts, " "))
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

func nextScore(old, bps int64) int64 {
	if bps <= 0 {
		return old * 3 / 4
	}
	return (old*7 + bps*3) / 10
}

func nextBandwidthScore(old, bps, active int64) int64 {
	if bps <= 0 {
		if active <= 0 {

			decayed := old * 15 / 16
			if decayed < coldStartFloor && old >= coldStartFloor {
				return coldStartFloor
			}
			return decayed
		}
		return old * 3 / 4
	}
	return (old*7 + bps*3) / 10
}

func (sf *subflow) recordBWSample(bps int64) int64 {
	sf.healthMu.Lock()
	defer sf.healthMu.Unlock()
	sf.bwHist[sf.bwHistIdx] = bps
	sf.bwHistIdx = (sf.bwHistIdx + 1) % bwPeakTicks
	if bps > 0 {
		return bps
	}

	var peak int64
	for _, v := range sf.bwHist {
		if v > peak {
			peak = v
		}
	}
	if peak > 0 {
		return peak / 2
	}
	return 0
}

func (sf *subflow) recordError(weight float64) {
	if weight <= 0 {
		return
	}
	sf.healthMu.Lock()
	sf.errorRate += weight
	sf.healthMu.Unlock()
}

func (sf *subflow) updateHealth(h tunnel.HealthSnapshot, dt time.Duration) {
	sf.healthMu.Lock()
	defer sf.healthMu.Unlock()

	if dt > 0 && sf.errorRate > 0 {
		decay := math.Pow(0.5, dt.Seconds()/errDecayHalfLife.Seconds())
		sf.errorRate *= decay
		if sf.errorRate < 1e-4 {
			sf.errorRate = 0
		}
	}

	if h.Resets > sf.lastResets {
		sf.errorRate += float64(h.Resets - sf.lastResets)
		sf.lastResets = h.Resets
	}

	sf.queuedBytes = h.QueuedBytes
	if h.SRTT > 0 {
		sf.rttHist[sf.rttHistIdx] = h.SRTT
		sf.rttHistIdx = (sf.rttHistIdx + 1) % rttMedianWindow
		if sf.rttHistCount < rttMedianWindow {
			sf.rttHistCount++
		}
		tmp := make([]time.Duration, sf.rttHistCount)
		copy(tmp, sf.rttHist[:sf.rttHistCount])
		slices.Sort(tmp)
		sf.medianRTT = tmp[len(tmp)/2]
		sf.rttvar = h.RTTVAR
	}
}

func (sf *subflow) compositeScore(avgActive float64) float64 {
	tp := sf.throughput.Load()
	active := sf.active.Load()

	sf.healthMu.Lock()
	medRTT := sf.medianRTT
	rttvar := sf.rttvar
	queued := sf.queuedBytes
	errRate := sf.errorRate
	sf.healthMu.Unlock()

	return computeCompositeScore(tp, medRTT, rttvar, active, avgActive, queued, errRate)
}

func computeCompositeScore(
	effBW int64,
	medianRTT, rttvar time.Duration,
	active int64,
	avgActive float64,
	queuedBytes int64,
	errRate float64,
) float64 {
	if effBW < coldStartFloor {
		effBW = coldStartFloor
	}
	if avgActive < 1.0 {
		avgActive = 1.0
	}

	latencyPenalty := 1.0
	if medianRTT > 0 {
		effRTT := float64(medianRTT) + 0.5*float64(rttvar)
		latencyPenalty = 1.0 + effRTT/float64(scoreRefRTT)
		if latencyPenalty > maxLatencyPenalty {
			latencyPenalty = maxLatencyPenalty
		}
	}

	loadPenalty := 1.0 + 0.25*(float64(active)/avgActive)
	if queuedBytes > 0 {
		loadPenalty += 0.5 * (float64(queuedBytes) / float64(effBW))
	}

	reliability := math.Exp(-errPenaltyStrength * errRate)
	if reliability < minReliability {
		reliability = minReliability
	}

	return (float64(effBW) / (latencyPenalty * loadPenalty)) * reliability
}

func (m *FlowMux) Throughputs() map[string]int64 {
	out := make(map[string]int64, len(m.subflows))
	for _, sf := range m.subflows {
		out[sf.name] = sf.throughput.Load()
	}
	return out
}
