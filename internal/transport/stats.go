package transport

import (
	"encoding/json"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultStatsCacheTTL = 750 * time.Millisecond

type Snapshot struct {
	NetworkSent  int64     `json:"network_sent"`
	NetworkRecv  int64     `json:"network_recv"`
	UsefulSent   int64     `json:"useful_sent"`
	UsefulRecv   int64     `json:"useful_recv"`
	FramesSent   int64     `json:"frames_sent"`
	FramesRecv   int64     `json:"frames_recv"`
	MessagesSent int64     `json:"messages_sent"`
	MessagesRecv int64     `json:"messages_recv"`
	Timestamp    time.Time `json:"timestamp"`
}

func (s Snapshot) Efficiency() float64 {
	tu := s.UsefulSent + s.UsefulRecv
	tw := s.NetworkSent + s.NetworkRecv
	if tw <= 0 {
		return 0
	}
	return float64(tu) / float64(tw)
}

type Layer struct {
	Kind     string
	Type     string
	Name     string
	Snapshot Snapshot
}

type Telemetry interface {
	WalkLayers() []Layer
}

type Counters struct {
	NetworkSent  atomic.Int64
	NetworkRecv  atomic.Int64
	UsefulSent   atomic.Int64
	UsefulRecv   atomic.Int64
	FramesSent   atomic.Int64
	FramesRecv   atomic.Int64
	MessagesSent atomic.Int64
	MessagesRecv atomic.Int64
}

func (c *Counters) Snapshot() Snapshot {
	return Snapshot{
		NetworkSent:  c.NetworkSent.Load(),
		NetworkRecv:  c.NetworkRecv.Load(),
		UsefulSent:   c.UsefulSent.Load(),
		UsefulRecv:   c.UsefulRecv.Load(),
		FramesSent:   c.FramesSent.Load(),
		FramesRecv:   c.FramesRecv.Load(),
		MessagesSent: c.MessagesSent.Load(),
		MessagesRecv: c.MessagesRecv.Load(),
		Timestamp:    time.Now(),
	}
}

type Rates struct {
	WireSentBPS   float64 `json:"wire_sent_bps"`
	WireRecvBPS   float64 `json:"wire_recv_bps"`
	UsefulSentBPS float64 `json:"useful_sent_bps"`
	UsefulRecvBPS float64 `json:"useful_recv_bps"`
	Efficiency    float64 `json:"efficiency"`
	TxEfficiency  float64 `json:"tx_efficiency"`
	RxEfficiency  float64 `json:"rx_efficiency"`
}

type RateMeter struct {
	mu     sync.Mutex
	last   Snapshot
	lastAt time.Time
	has    bool
}

func (m *RateMeter) Update(s Snapshot) Rates {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var r Rates
	if m.has {
		dt := now.Sub(m.lastAt).Seconds()
		if dt > 0 {
			r.WireSentBPS = float64(s.NetworkSent-m.last.NetworkSent) / dt
			r.WireRecvBPS = float64(s.NetworkRecv-m.last.NetworkRecv) / dt
			r.UsefulSentBPS = float64(s.UsefulSent-m.last.UsefulSent) / dt
			r.UsefulRecvBPS = float64(s.UsefulRecv-m.last.UsefulRecv) / dt
		}
	}

	totalUseful := s.UsefulSent + s.UsefulRecv
	totalWire := s.NetworkSent + s.NetworkRecv
	if totalWire > 0 {
		r.Efficiency = float64(totalUseful) / float64(totalWire)
	}
	if s.NetworkSent > 0 {
		r.TxEfficiency = float64(s.UsefulSent) / float64(s.NetworkSent)
	}
	if s.NetworkRecv > 0 {
		r.RxEfficiency = float64(s.UsefulRecv) / float64(s.NetworkRecv)
	}

	m.last = s
	m.lastAt = now
	m.has = true
	return r
}

type SubflowStats struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	WireSent     int64   `json:"wire_sent"`
	WireRecv     int64   `json:"wire_recv"`
	UsefulSent   int64   `json:"useful_sent"`
	UsefulRecv   int64   `json:"useful_recv"`
	Efficiency   float64 `json:"efficiency"`
	TxEfficiency float64 `json:"tx_efficiency"`
	RxEfficiency float64 `json:"rx_efficiency"`
	Share        float64 `json:"share"`
	Rates        Rates   `json:"rates"`
}

type AggregatedStats struct {
	Overall  Snapshot       `json:"overall"`
	Rates    Rates          `json:"rates"`
	Subflows []SubflowStats `json:"subflows,omitempty"`
}

type JSONCache struct {
	ttl time.Duration
	now func() time.Time

	mu       sync.Mutex
	valid    bool
	cachedAt time.Time
	payload  []byte
}

func NewJSONCache(ttl time.Duration) *JSONCache {
	if ttl <= 0 {
		ttl = DefaultStatsCacheTTL
	}
	return &JSONCache{ttl: ttl, now: time.Now}
}

func (c *JSONCache) Get(build func() ([]byte, error)) ([]byte, error) {
	if c == nil {
		return build()
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	if c.valid && now.Sub(c.cachedAt) < c.ttl {
		return c.payload, nil
	}

	payload, err := build()
	if err != nil {
		return nil, err
	}
	c.payload = payload
	c.cachedAt = now
	c.valid = true
	return payload, nil
}

func (c *JSONCache) Invalidate() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.valid = false
	c.cachedAt = time.Time{}
	c.payload = nil
	c.mu.Unlock()
}

type StatsAggregator struct {
	ttl time.Duration
	now func() time.Time

	mu     sync.Mutex
	meters map[string]*RateMeter

	hasCache bool
	cachedAt time.Time
	cached   AggregatedStats
	cachedJS []byte
}

func NewStatsAggregator() *StatsAggregator {
	return NewStatsAggregatorWithTTL(DefaultStatsCacheTTL)
}

func NewStatsAggregatorWithTTL(ttl time.Duration) *StatsAggregator {
	if ttl <= 0 {
		ttl = DefaultStatsCacheTTL
	}
	return &StatsAggregator{
		ttl:    ttl,
		now:    time.Now,
		meters: map[string]*RateMeter{},
	}
}

func (a *StatsAggregator) Invalidate() {
	a.mu.Lock()
	a.hasCache = false
	a.cachedAt = time.Time{}
	a.cached = AggregatedStats{}
	a.cachedJS = nil
	a.mu.Unlock()
}

func (a *StatsAggregator) meter(key string) *RateMeter {
	if a.meters == nil {
		a.meters = map[string]*RateMeter{}
	}
	m := a.meters[key]
	if m == nil {
		m = &RateMeter{}
		a.meters[key] = m
	}
	return m
}

func (a *StatsAggregator) Update(layers []Layer) AggregatedStats {
	stats, _, _ := a.snapshot(layers, false)
	return stats
}

func (a *StatsAggregator) UpdateJSON(layers []Layer) (AggregatedStats, []byte, error) {
	return a.snapshot(layers, true)
}

func (a *StatsAggregator) snapshot(layers []Layer, wantJSON bool) (AggregatedStats, []byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.now()
	if a.hasCache && now.Sub(a.cachedAt) < a.ttl {
		if wantJSON && a.cachedJS == nil {
			data, err := json.Marshal(a.cached)
			if err != nil {
				return a.cached, nil, err
			}
			a.cachedJS = data
		}
		return a.cached, a.cachedJS, nil
	}

	out := a.aggregate(layers, now)
	a.cached = out
	a.cachedAt = now
	a.hasCache = true
	a.cachedJS = nil
	if wantJSON {
		data, err := json.Marshal(out)
		if err != nil {
			return out, nil, err
		}
		a.cachedJS = data
		return out, data, nil
	}
	return out, nil, nil
}

func (a *StatsAggregator) aggregate(layers []Layer, now time.Time) AggregatedStats {
	var appSent, appRecv, wireSent, wireRecv int64

	for _, l := range layers {
		switch l.Kind {
		case "mux":
			appSent = l.Snapshot.UsefulSent
			appRecv = l.Snapshot.UsefulRecv
		case "pipe":
			wireSent += l.Snapshot.NetworkSent
			wireRecv += l.Snapshot.NetworkRecv
		}
	}

	overall := Snapshot{
		NetworkSent: wireSent,
		NetworkRecv: wireRecv,
		UsefulSent:  appSent,
		UsefulRecv:  appRecv,
		Timestamp:   now,
	}

	subGroups := map[string][]Layer{}
	for _, l := range layers {
		if l.Kind != "pipe" {
			continue
		}
		subGroups[l.Name] = append(subGroups[l.Name], l)
	}

	var subs []*SubflowStats
	for key, ls := range subGroups {
		sf := &SubflowStats{Name: key}
		for _, l := range ls {
			sf.WireSent += l.Snapshot.NetworkSent
			sf.WireRecv += l.Snapshot.NetworkRecv
			if sf.Type == "" {
				sf.Type = l.Type
			}
		}
		subs = append(subs, sf)
	}

	totalWireBoth := wireSent + wireRecv

	out := AggregatedStats{Overall: overall}
	out.Rates = a.meter("").Update(overall)

	for _, s := range subs {
		if wireSent > 0 {
			s.UsefulSent = int64(float64(appSent) * float64(s.WireSent) / float64(wireSent))
		}
		if wireRecv > 0 {
			s.UsefulRecv = int64(float64(appRecv) * float64(s.WireRecv) / float64(wireRecv))
		}

		tu := s.UsefulSent + s.UsefulRecv
		tw := s.WireSent + s.WireRecv
		if tw > 0 {
			s.Efficiency = float64(tu) / float64(tw)
		}
		if s.WireSent > 0 {
			s.TxEfficiency = float64(s.UsefulSent) / float64(s.WireSent)
		}
		if s.WireRecv > 0 {
			s.RxEfficiency = float64(s.UsefulRecv) / float64(s.WireRecv)
		}
		if totalWireBoth > 0 {
			s.Share = float64(s.WireSent+s.WireRecv) / float64(totalWireBoth)
		}
		s.Rates = a.meter(s.Name).Update(Snapshot{
			NetworkSent: s.WireSent,
			NetworkRecv: s.WireRecv,
			UsefulSent:  s.UsefulSent,
			UsefulRecv:  s.UsefulRecv,
			Timestamp:   now,
		})
		out.Subflows = append(out.Subflows, *s)
	}

	sort.Slice(out.Subflows, func(i, j int) bool {
		return out.Subflows[i].Name < out.Subflows[j].Name
	})
	return out
}
