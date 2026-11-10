package bond

import (
	"math/rand/v2"
	"sync/atomic"
	"time"
)

type subflow struct {
	name string
	tube tubeLike

	rttNs      atomic.Int64
	inflight   atomic.Int64
	deadUntil  atomic.Int64
	errorCount atomic.Int64
}

type scheduler struct {
	subflows []*subflow
	rr       atomic.Uint64
	rng      *rand.Rand
}

func newScheduler(names []string, tubes []tubeLike) *scheduler {
	s := &scheduler{
		rng: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 42)),
	}
	for i := range tubes {
		s.subflows = append(s.subflows, &subflow{
			name: names[i],
			tube: tubes[i],
		})
	}
	return s
}

func (s *scheduler) Pick() *subflow {
	now := time.Now().UnixNano()

	alive := make([]*subflow, 0, len(s.subflows))
	for _, sf := range s.subflows {
		if sf.deadUntil.Load() > now {
			continue
		}
		alive = append(alive, sf)
	}
	if len(alive) == 0 {
		return s.subflows[s.rng.IntN(len(s.subflows))]
	}
	if len(alive) == 1 {
		return alive[0]
	}

	idx := s.rr.Add(1) - 1
	return alive[idx%uint64(len(alive))]
}

func (s *scheduler) MarkDead(sf *subflow, _ error) {
	sf.errorCount.Add(1)
	n := sf.errorCount.Load()
	backoff := time.Second << (n - 1)
	if backoff > 10*time.Second {
		backoff = 10 * time.Second
	}
	sf.deadUntil.Store(time.Now().Add(backoff).UnixNano())
}

func (s *scheduler) MarkAlive(sf *subflow) {
	sf.errorCount.Store(0)
	sf.deadUntil.Store(0)
}

func (s *scheduler) AddInflight(sf *subflow, n int64) { sf.inflight.Add(n) }
func (s *scheduler) SubInflight(sf *subflow, n int64) { sf.inflight.Add(-n) }
func (s *scheduler) UpdateRTT(sf *subflow, rtt time.Duration) {
	old := sf.rttNs.Load()
	if old == 0 {
		sf.rttNs.Store(int64(rtt))
	} else {
		sf.rttNs.Store((old*7 + int64(rtt)*3) / 10)
	}
}
