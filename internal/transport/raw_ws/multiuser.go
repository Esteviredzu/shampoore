package raw_ws

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
)

func roomForUser(room, userID string) string {
	room = strings.TrimSpace(room)
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return room
	}
	return room + "-" + userID
}

func buildUserRegistry(specs []auth.UserSpec) (*auth.UserRegistry, error) {
	registry := auth.NewRegistry()
	for _, spec := range specs {
		if err := registry.Upsert(spec); err != nil {
			return nil, fmt.Errorf("raw_ws: user %q: %w", spec.UserID, err)
		}
	}
	return registry, nil
}

type multiSession struct {
	userID string
	cancel context.CancelFunc
}

type MultiServer struct {
	cfg      Config
	logger   logging.ContextLogger
	registry *auth.UserRegistry
	muxCfg   tunnel.Config

	syncMu sync.Mutex

	mu          sync.Mutex
	handler     app.SessionHandler
	sessions    map[string]*multiSession
	ctx         context.Context
	cancel      context.CancelFunc
	started     bool
	unsubscribe func()
	wg          sync.WaitGroup
}

func newMultiServer(
	cfg Config,
	logger logging.ContextLogger,
	registry *auth.UserRegistry,
	eff app.Values,
) *MultiServer {
	return &MultiServer{
		cfg:      cfg,
		logger:   logger,
		registry: registry,
		muxCfg: tunnel.Config{
			FlushThreshold: eff.Int("mux_flush_threshold", 12000),
			FlushInterval:  time.Duration(eff.Int("mux_flush_ms", 20)) * time.Millisecond,
			Registry:       registry,
		},
		sessions: make(map[string]*multiSession),
	}
}

func (m *MultiServer) SetSessionHandler(handler app.SessionHandler) {
	m.mu.Lock()
	m.handler = handler
	m.mu.Unlock()
}

func (m *MultiServer) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return nil
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.started = true
	m.mu.Unlock()

	if m.registry != nil {
		unsub := m.registry.Subscribe(m.syncUsers)
		m.mu.Lock()
		m.unsubscribe = unsub
		m.mu.Unlock()
	}
	m.syncUsers()
	return nil
}

func (m *MultiServer) Stop(ctx context.Context) error {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return nil
	}
	m.started = false
	cancel := m.cancel
	unsub := m.unsubscribe
	m.cancel = nil
	m.unsubscribe = nil
	sessions := make([]*multiSession, 0, len(m.sessions))
	for id, session := range m.sessions {
		sessions = append(sessions, session)
		delete(m.sessions, id)
	}
	m.mu.Unlock()

	if unsub != nil {
		unsub()
	}
	if cancel != nil {
		cancel()
	}
	for _, session := range sessions {
		session.cancel()
	}

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (m *MultiServer) syncUsers() {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	ctx := m.ctx
	active := make(map[string]struct{})
	if m.registry != nil {
		for _, spec := range m.registry.ActiveUsers() {
			active[spec.UserID] = struct{}{}
		}
	}

	var toStart []string
	for userID := range active {
		if _, ok := m.sessions[userID]; !ok {
			toStart = append(toStart, userID)
		}
	}
	var toStop []*multiSession
	for userID, session := range m.sessions {
		if _, ok := active[userID]; !ok {
			toStop = append(toStop, session)
			delete(m.sessions, userID)
		}
	}
	m.mu.Unlock()

	for _, session := range toStop {
		m.logger.Info("raw_ws: stopping user session ", session.userID)
		session.cancel()
	}
	for _, userID := range toStart {
		m.startSession(ctx, userID)
	}
}

func (m *MultiServer) startSession(ctx context.Context, userID string) {
	if ctx == nil {
		return
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	session := &multiSession{userID: userID, cancel: cancel}

	m.mu.Lock()
	if !m.started || m.sessions[userID] != nil {
		m.mu.Unlock()
		cancel()
		return
	}
	handler := m.handler
	m.sessions[userID] = session
	m.wg.Add(1)
	m.mu.Unlock()

	m.logger.Info("raw_ws: starting user session ", userID, " (room=",
		roomForUser(m.cfg.Room, userID), ")")

	go func() {
		defer m.wg.Done()
		defer func() {
			m.mu.Lock()
			if m.sessions[userID] == session {
				delete(m.sessions, userID)
			}
			m.mu.Unlock()
			cancel()
		}()

		if sessionCtx.Err() != nil {
			return
		}

		cfg := m.cfg
		cfg.UserID = userID
		cfg.Multiuser = false
		tr := newTransport(cfg, m.logger)
		if tr == nil {
			return
		}

		muxCfg := m.muxCfg
		muxCfg.UserID = ""
		muxCfg.ExitNodeID = ""
		muxCfg.Registry = m.registry
		mux := tunnel.New(tr, m.logger, muxCfg)

		if handler == nil {
			handler = func(ctx context.Context, _ multiplexing.StreamMultiplexer) error {
				<-ctx.Done()
				return ctx.Err()
			}
		}

		if err := mux.Start(sessionCtx); err != nil {
			if sessionCtx.Err() == nil {
				m.logger.Warn("raw_ws: user ", userID, " mux start: ", err)
			}
			return
		}
		if err := handler(sessionCtx, mux); err != nil && sessionCtx.Err() == nil {
			m.logger.Warn("raw_ws: user ", userID, " session: ", err)
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := mux.Stop(stopCtx); err != nil {
			m.logger.Debug("raw_ws: user ", userID, " mux stop: ", err)
		}
		stopCancel()
	}()
}
