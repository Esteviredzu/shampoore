package bond

import (
	"sync/atomic"
	"time"
)

const (
	queueCap = 32
)

type subflow struct {
	name string
	tube tubeLike

	queue      chan queuedChunk
	workerDone chan struct{}

	deadUntil  atomic.Int64
	errorCount atomic.Int64
	sentBytes  atomic.Int64
	inflight   atomic.Int32

	confirmed atomic.Bool
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

func (s *scheduler) pickControl() *subflow {
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
	Name      string
	QueueLen  int
	QueueCap  int
	SentBytes int64
	Dead      bool
	Pending   int

	Confirmed bool
}

func (s *scheduler) Snapshot() []SubflowSnapshot {
	now := time.Now().UnixNano()
	out := make([]SubflowSnapshot, 0, len(s.subflows))
	for _, sf := range s.subflows {
		out = append(out, SubflowSnapshot{
			Name:      sf.name,
			QueueLen:  len(sf.queue),
			QueueCap:  cap(sf.queue),
			SentBytes: sf.sentBytes.Load(),
			Dead:      sf.deadUntil.Load() > now,
			Pending:   int(sf.inflight.Load()),
			Confirmed: sf.confirmed.Load(),
		})
	}
	return out
}
