package relay

import (
	"math/rand/v2"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/Esteviredzu/shampoore/internal/wsshape"
)

type shapeState struct {
	mu     sync.RWMutex
	config wsshape.Config
	source string
	err    error
}

func (s *Server) SetShape(cfg wsshape.Config) {
	s.shape.mu.Lock()
	s.shape.config = wsshape.Normalize(cfg)
	s.shape.source = "explicit"
	s.shape.err = nil
	s.shape.mu.Unlock()
}

func (s *Server) Shape() wsshape.Config {
	if s == nil {
		return wsshape.Default()
	}
	s.shape.mu.RLock()
	cfg := s.shape.config
	s.shape.mu.RUnlock()
	if cfg.Version == 0 {
		return wsshape.Default()
	}
	return cfg
}

func (s *Server) ShapeSource() string {
	s.shape.mu.RLock()
	defer s.shape.mu.RUnlock()
	return s.shape.source
}

func (s *Server) ShapeError() error {
	s.shape.mu.RLock()
	defer s.shape.mu.RUnlock()
	return s.shape.err
}

func (s *Server) reloadShape() {
	dir := s.DecoyDir()
	cfg, found, err := wsshape.LoadDir(dir)

	s.shape.mu.Lock()
	defer s.shape.mu.Unlock()
	s.shape.err = err
	switch {
	case err != nil:
		s.shape.config = wsshape.Default()
		s.shape.source = "default (invalid " + wsshape.FileName + ")"
	case found:
		s.shape.config = cfg
		s.shape.source = filepath.Join(dir, wsshape.FileName)
	default:
		s.shape.config = wsshape.Default()
		s.shape.source = "default"
	}
}

func (s *Server) pingEvery() time.Duration {
	if s.pingInterval > 0 {
		return s.pingInterval
	}
	if d := durationMs(s.Shape().Pulse.PingIntervalMs); d > 0 {
		return d
	}
	return defaultPingInterval
}

func (s *Server) pongWaitDuration() time.Duration {
	if s.pongWait > 0 {
		return s.pongWait
	}
	if d := durationMs(s.Shape().Pulse.ReadIdleMs); d > 0 {
		return d
	}
	return defaultPongWait
}

func (s *Server) nextTickDelay() time.Duration {
	if s.tickDelay != nil {
		return s.tickDelay()
	}
	decoy := s.Shape().Decoy
	base := durationMs(decoy.TickIntervalMs)
	if base <= 0 {
		base = minTickDelay
	}
	jitter := durationMs(decoy.TickJitterMs)
	if jitter <= 0 {
		return base
	}
	return base + time.Duration(rand.Int64N(int64(jitter)+1))
}

func decoyLimiter(r wsshape.Rate) *rate.Limiter {
	if r.MaxRateKBps <= 0 {
		return nil
	}
	burst := r.BurstBytes
	if burst <= 0 {
		burst = 64 << 10
	}
	return rate.NewLimiter(rate.Limit(r.MaxRateKBps*1024), burst)
}

func durationMs(ms int) time.Duration {
	return time.Duration(ms) * time.Millisecond
}
