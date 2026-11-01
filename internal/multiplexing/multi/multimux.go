// Package multi implements stream-level bonding: every stream opened via
// this multiplexer is pinned to exactly one subflow, so throughput adds
// up across subflows instead of suffering head-of-line blocking.
package multi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/multiplexing"
	"github.com/Esteviredzu/shampoore-core/internal/multiplexing/tunnel"
)

type subMux struct {
	name   string
	mux    *tunnel.TunnelMultiplexer
	weight int64
	total  atomic.Int64 // total streams ever opened on this subflow
}

type MultiStreamMux struct {
	subs   []*subMux
	logger logging.ContextLogger

	incoming chan multiplexing.Stream

	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	stopped bool
	mu      sync.Mutex
	wg      sync.WaitGroup
}

// New creates a MultiStreamMux from the subflows returned by the multi
// transport descriptor. Each subflow gets its own TunnelMultiplexer built
// on top of its own transport.Transport.
func New(subs []multiplexing.SubflowTransport, logger logging.ContextLogger, cfg tunnel.Config) (*MultiStreamMux, error) {
	if len(subs) == 0 {
		return nil, errors.New("multi-mux: no subflows configured")
	}

	m := &MultiStreamMux{
		logger:   logger,
		incoming: make(chan multiplexing.Stream, 128),
	}

	for _, s := range subs {
		if s.Transport == nil {
			return nil, fmt.Errorf("multi-mux: subflow %q has nil transport", s.Name)
		}
		w := s.Weight
		if w <= 0 {
			w = 1
		}
		tm := tunnel.New(s.Transport, logger, cfg)
		m.subs = append(m.subs, &subMux{
			name:   s.Name,
			mux:    tm,
			weight: int64(w),
		})
	}
	return m, nil
}

func (m *MultiStreamMux) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	// Start all sub-muxes. On failure, roll back already started ones.
	for i, s := range m.subs {
		if err := s.mux.Start(ctx); err != nil {
			for j := i - 1; j >= 0; j-- {
				_ = m.subs[j].mux.Stop(context.Background())
			}
			return fmt.Errorf("multi-mux: start %s: %w", s.name, err)
		}
	}

	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.started = true
	m.mu.Unlock()

	for _, s := range m.subs {
		m.wg.Add(1)
		go m.acceptFrom(s)
	}

	m.logger.Info("multi-mux: started with ", len(m.subs), " subflows")
	return nil
}

func (m *MultiStreamMux) Stop(ctx context.Context) error {
	m.mu.Lock()
	if !m.started || m.stopped {
		m.mu.Unlock()
		return nil
	}
	m.stopped = true
	m.mu.Unlock()

	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()

	for i := len(m.subs) - 1; i >= 0; i-- {
		_ = m.subs[i].mux.Stop(ctx)
	}
	return nil
}

// OpenStream picks one subflow by weighted round-robin and opens a stream
// on it. All subsequent data for this stream travels only through that
// subflow — this is what makes throughput additive.
func (m *MultiStreamMux) OpenStream(ctx context.Context, metadata map[string]any) (multiplexing.Stream, error) {
	m.mu.Lock()
	if !m.started || m.stopped {
		m.mu.Unlock()
		return nil, errors.New("multi-mux: not running")
	}
	m.mu.Unlock()

	s := m.pick()
	if s == nil {
		return nil, errors.New("multi-mux: no available subflows")
	}

	stream, err := s.mux.OpenStream(ctx, metadata)
	if err != nil {
		m.logger.Warn("multi-mux: open on ", s.name, " failed: ", err)
		return nil, err
	}
	return stream, nil
}

func (m *MultiStreamMux) AcceptStream(ctx context.Context) (multiplexing.Stream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.ctx.Done():
		return nil, io.EOF
	case s, ok := <-m.incoming:
		if !ok {
			return nil, io.EOF
		}
		return s, nil
	}
}

func (m *MultiStreamMux) acceptFrom(s *subMux) {
	defer m.wg.Done()
	for {
		stream, err := s.mux.AcceptStream(m.ctx)
		if err != nil {
			if m.ctx.Err() == nil {
				m.logger.Warn("multi-mux: accept from ", s.name, " stopped: ", err)
			}
			return
		}
		select {
		case m.incoming <- stream:
		case <-m.ctx.Done():
			_ = stream.Close()
			return
		}
	}
}

// pick returns the least-loaded subflow by total streams ever assigned,
// weighted by its Weight. A subflow with Weight=2 gets chosen twice as
// often as a subflow with Weight=1.
func (m *MultiStreamMux) pick() *subMux {
	var best *subMux
	bestScore := int64(1) << 62
	for _, s := range m.subs {
		score := s.total.Load() * 1024 / s.weight
		if score < bestScore {
			best = s
			bestScore = score
		}
	}
	if best != nil {
		best.total.Add(1)
	}
	return best
}