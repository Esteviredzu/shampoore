package userlimit

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	DefaultBurst = 256 * 1024

	ActiveWindow = 30 * time.Second
)

type Option func(*Registry)

func WithBurst(bytes int) Option {
	return func(r *Registry) {
		if bytes > 0 {
			r.burst = bytes
		}
	}
}

type entry struct {
	limiter  *rate.Limiter
	bps      atomic.Int64
	lastSeen atomic.Int64
}

type Registry struct {
	mu      sync.Mutex
	entries map[string]*entry
	burst   int
}

func New(opts ...Option) *Registry {
	r := &Registry{
		entries: make(map[string]*entry),
		burst:   DefaultBurst,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

func (r *Registry) SetLimit(userID string, bytesPerSec int64) {
	if r == nil {
		return
	}
	id := strings.TrimSpace(userID)
	if id == "" {
		return
	}
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	e := r.entries[id]
	if e == nil {
		e = &entry{}
		r.entries[id] = e
	}
	burst := r.burst
	switch {
	case e.limiter == nil && bytesPerSec == 0:
		e.limiter = rate.NewLimiter(rate.Inf, burst)
	case e.limiter == nil:
		e.limiter = rate.NewLimiter(rate.Limit(bytesPerSec), burst)
	case bytesPerSec == 0:
		e.limiter.SetLimit(rate.Inf)
		e.limiter.SetBurst(burst)
	default:
		e.limiter.SetLimit(rate.Limit(bytesPerSec))
		e.limiter.SetBurst(burst)
	}
	e.bps.Store(bytesPerSec)
}

func (r *Registry) ReplaceLimits(limits map[string]int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	ids := make([]string, 0, len(r.entries))
	for id := range r.entries {
		ids = append(ids, id)
	}
	r.mu.Unlock()
	for _, id := range ids {
		if _, ok := limits[id]; !ok {
			r.SetLimit(id, 0)
		}
	}
	for id, bps := range limits {
		r.SetLimit(id, bps)
	}
}

func (r *Registry) Limit(userID string) int64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entries[strings.TrimSpace(userID)]
	if e == nil {
		return 0
	}
	return e.bps.Load()
}

func (r *Registry) Wait(ctx context.Context, userID string, n int) error {
	if r == nil || n <= 0 {
		return nil
	}
	lim, bps, burst := r.snapshot(userID)
	if lim == nil || bps <= 0 {
		return nil
	}
	for n > 0 {
		chunk := n
		if chunk > burst {
			chunk = burst
		}
		if err := lim.WaitN(ctx, chunk); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

func (r *Registry) snapshot(userID string) (*rate.Limiter, int64, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := strings.TrimSpace(userID)
	e := r.entries[id]
	if e == nil || e.limiter == nil {
		return nil, 0, r.burst
	}
	return e.limiter, e.bps.Load(), r.burst
}

func (r *Registry) Touch(userID string) {
	if r == nil {
		return
	}
	id := strings.TrimSpace(userID)
	if id == "" {
		return
	}
	now := time.Now().UnixNano()
	r.mu.Lock()
	e := r.entries[id]
	if e == nil {
		e = &entry{}
		r.entries[id] = e
	}
	e.lastSeen.Store(now)
	r.mu.Unlock()
}

func (r *Registry) ActiveUsers(window time.Duration) []string {
	if r == nil {
		return nil
	}
	if window <= 0 {
		window = ActiveWindow
	}
	cutoff := time.Now().Add(-window).UnixNano()
	r.mu.Lock()
	out := make([]string, 0, len(r.entries))
	for id, e := range r.entries {
		if e.lastSeen.Load() >= cutoff {
			out = append(out, id)
		}
	}
	r.mu.Unlock()
	sort.Strings(out)
	return out
}
