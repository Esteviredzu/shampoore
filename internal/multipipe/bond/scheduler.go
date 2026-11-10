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
}

type scheduler struct {
	subflows []*subflow
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

func (s *scheduler) pickNonFull(exclude *subflow) *subflow {
	now := time.Now().UnixNano()
	var best *subflow
	bestDepth := int(^uint(0) >> 1)
	for _, sf := range s.subflows {
		if sf == exclude {
			continue
		}
		if sf.deadUntil.Load() > now {
			continue
		}
		depth := len(sf.queue)
		if depth >= cap(sf.queue) {
			continue
		}
		if depth < bestDepth {
			best = sf
			bestDepth = depth
		}
	}
	return best
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
		})
	}
	return out
}
