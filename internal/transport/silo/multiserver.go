package silo

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
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type multiSession struct {
	userID string
	cancel context.CancelFunc

	mu  sync.Mutex
	mux multiplexing.StreamMultiplexer
}

func (s *multiSession) setMux(m multiplexing.StreamMultiplexer) {
	s.mu.Lock()
	s.mux = m
	s.mu.Unlock()
}

func (s *multiSession) currentMux() multiplexing.StreamMultiplexer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mux
}

const sessionRetryDelayDefault = time.Second

var sessionRetryDelay = sessionRetryDelayDefault

type MultiServer struct {
	cfg      Config
	logger   logging.ContextLogger
	registry *auth.UserRegistry
	store    objectStore
	password string
	muxCfg   tunnel.Config

	exitNodeID string

	syncMu sync.Mutex

	mu          sync.Mutex
	handler     app.SessionHandler
	transportIn app.SessionTransportSink
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
	password string,
	store objectStore,
	eff app.Values,
) *MultiServer {
	return &MultiServer{
		cfg:      cfg,
		logger:   logger,
		registry: registry,
		store:    store,
		password: password,
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

func (m *MultiServer) SetTransportSink(sink app.SessionTransportSink) {
	m.mu.Lock()
	m.transportIn = sink
	m.mu.Unlock()
}

func (m *MultiServer) SetExitNodeID(id string) {
	m.mu.Lock()
	m.exitNodeID = strings.TrimSpace(id)
	m.mu.Unlock()
}

func (m *MultiServer) transportSink() app.SessionTransportSink {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.transportIn
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
		m.logger.Info("silo: stopping user session ", session.userID)
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
	if !m.started {
		m.mu.Unlock()
		cancel()
		return
	}
	stale := m.sessions[userID]
	m.sessions[userID] = session
	m.wg.Add(1)
	m.mu.Unlock()

	if stale != nil && stale != session {
		m.logger.Info("silo: replacing stale user session ", userID)
		stale.cancel()
	}

	m.logger.Info("silo: starting user session ", userID, " (prefix=", UserPrefix(m.cfg.TunnelID, userID), ")")

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

		waitRetry := func() bool {
			select {
			case <-sessionCtx.Done():
				return false
			case <-time.After(sessionRetryDelay):
				return true
			}
		}

		for {
			if sessionCtx.Err() != nil {
				return
			}

			m.mu.Lock()
			handler := m.handler
			exitNodeID := m.exitNodeID
			m.mu.Unlock()

			cfg := m.cfg
			cfg.UserID = userID
			cfg.Multiuser = false
			if cfg.TunnelID == "" {
				cfg.TunnelID = "default"
			}

			pipe, err := newPipeWithStore(&cfg, "server", m.logger, m.store)
			if err != nil {
				m.logger.Warn("silo: user ", userID, " pipe: ", err)
				if !waitRetry() {
					return
				}
				continue
			}
			secured, err := wrapEncryption(pipe, m.password, "server", m.logger)
			if err != nil {
				m.logger.Warn("silo: user ", userID, " encryption: ", err)
				if !waitRetry() {
					return
				}
				continue
			}

			tr := newTransport(secured)
			if sink := m.transportSink(); sink != nil {
				release, aggregated := sink(userID, tr)
				if release == nil {

					m.logger.Debug("silo: user ", userID, " tube was not accepted by the aggregator")
					if !waitRetry() {
						return
					}
					continue
				}
				select {
				case <-sessionCtx.Done():
				case <-aggregated:
				}
				release()
				if sessionCtx.Err() != nil {
					return
				}
				if !waitRetry() {
					return
				}
				continue
			}

			muxCfg := m.muxCfg

			muxCfg.UserID = userID
			muxCfg.ExitNodeID = exitNodeID
			muxCfg.Registry = m.registry

			muxCfg.OnRemoteEpochChange = func(uint32, uint32) {
				pipe.ResetReadHead()
				go func() {
					purgeCtx, purgeCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer purgeCancel()
					pipe.PurgeWritePrefix(purgeCtx)
				}()
			}
			mux := tunnel.New(tr, m.logger, muxCfg)

			if handler == nil {
				handler = func(ctx context.Context, _ multiplexing.StreamMultiplexer) error {
					<-ctx.Done()
					return ctx.Err()
				}
			}

			if err := mux.Start(sessionCtx); err != nil {
				if sessionCtx.Err() == nil {
					m.logger.Warn("silo: user ", userID, " mux start: ", err)
				}
				if !waitRetry() {
					return
				}
				continue
			}
			session.setMux(mux)
			if err := handler(sessionCtx, mux); err != nil && sessionCtx.Err() == nil {
				m.logger.Warn("silo: user ", userID, " session: ", err)
			}
			session.setMux(nil)
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := mux.Stop(stopCtx); err != nil {
				m.logger.Debug("silo: user ", userID, " mux stop: ", err)
			}
			stopCancel()
			if !waitRetry() {
				return
			}
		}
	}()
}

func (m *MultiServer) WalkLayers() []transport.Layer {
	m.mu.Lock()
	sessions := make([]*multiSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.mu.Unlock()

	var appSent, appRecv, wireSent, wireRecv int64
	pipes := make([]transport.Layer, 0, len(sessions))
	for _, session := range sessions {
		mux := session.currentMux()
		tel, ok := mux.(transport.Telemetry)
		if !ok {
			continue
		}
		for _, layer := range tel.WalkLayers() {
			switch layer.Kind {
			case "mux":
				appSent += layer.Snapshot.UsefulSent
				appRecv += layer.Snapshot.UsefulRecv
				wireSent += layer.Snapshot.NetworkSent
				wireRecv += layer.Snapshot.NetworkRecv
			case "pipe":
				if layer.Name == "" {
					layer.Name = session.userID
				}
				pipes = append(pipes, layer)
			}
		}
	}

	out := make([]transport.Layer, 0, len(pipes)+1)
	out = append(out, transport.Layer{
		Kind: "mux", Type: "silo",
		Snapshot: transport.Snapshot{
			UsefulSent:  appSent,
			UsefulRecv:  appRecv,
			NetworkSent: wireSent,
			NetworkRecv: wireRecv,
			Timestamp:   time.Now(),
		},
	})
	out = append(out, pipes...)
	return out
}

func (m *MultiServer) sessionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

func buildUserRegistry(specs []auth.UserSpec) (*auth.UserRegistry, error) {
	registry := auth.NewRegistry()
	for _, spec := range specs {
		if err := registry.Upsert(spec); err != nil {
			return nil, fmt.Errorf("silo: user %q: %w", spec.UserID, err)
		}
	}
	return registry, nil
}

var _ app.SessionServer = (*MultiServer)(nil)
var _ app.AggregableSession = (*MultiServer)(nil)
var _ transport.Telemetry = (*MultiServer)(nil)
