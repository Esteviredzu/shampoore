package jellyfin

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const exitSessionRetryDelay = time.Second

type exitSession struct {
	userID string
	cancel context.CancelFunc

	mu  sync.Mutex
	mux multiplexing.StreamMultiplexer
}

func (s *exitSession) setMux(mux multiplexing.StreamMultiplexer) {
	s.mu.Lock()
	s.mux = mux
	s.mu.Unlock()
}

func (s *exitSession) currentMux() multiplexing.StreamMultiplexer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mux
}

type MultiServer struct {
	cfg        Config
	logger     logging.ContextLogger
	registry   *auth.UserRegistry
	password   string
	muxCfg     tunnel.Config
	httpClient *http.Client

	exitNodeID string
	syncMu     sync.Mutex

	mu          sync.Mutex
	handler     app.SessionHandler
	transportIn app.SessionTransportSink
	sessions    map[string]*exitSession
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
	muxCfg tunnel.Config,
) *MultiServer {
	if logger == nil {
		logger = logging.New("jellyfin", false)
	}
	return &MultiServer{
		cfg:      cfg,
		logger:   logger,
		registry: registry,
		password: strings.TrimSpace(password),
		muxCfg:   muxCfg,
		sessions: make(map[string]*exitSession),
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
	if ctx == nil {
		ctx = context.Background()
	}
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
	sessions := make([]*exitSession, 0, len(m.sessions))
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

	if ctx == nil {
		m.wg.Wait()
		return nil
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
	var toStop []*exitSession
	for userID, session := range m.sessions {
		if _, ok := active[userID]; !ok {
			toStop = append(toStop, session)
			delete(m.sessions, userID)
		}
	}
	m.mu.Unlock()

	for _, session := range toStop {
		m.logger.Info("jellyfin: stopping exit session ", session.userID)
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
	session := &exitSession{userID: userID, cancel: cancel}

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
		m.logger.Info("jellyfin: replacing stale exit session ", userID)
		stale.cancel()
	}

	m.logger.Info("jellyfin: starting exit session ", userID, " (url=", m.cfg.URL, ")")

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
			case <-time.After(exitSessionRetryDelay):
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
			httpClient := m.httpClient
			m.mu.Unlock()

			cfg := m.cfg
			cfg.UserID = userID
			cfg.Multiuser = false
			cfg.Users = nil
			if cfg.Role == "" {
				cfg.Role = "exit"
			}

			client, err := newClient(cfg, exitNodeID, httpClient, m.logger)
			if err != nil {
				m.logger.Warn("jellyfin: exit ", userID, " client: ", err)
				if !waitRetry() {
					return
				}
				continue
			}
			if err := client.Start(sessionCtx); err != nil {
				m.logger.Warn("jellyfin: exit ", userID, " start: ", err)
				_ = client.Stop(context.Background())
				if !waitRetry() {
					return
				}
				continue
			}

			var tr transport.Transport = client
			if m.password != "" {
				secured, err := security.WrapTransport(client, m.password, security.RoleServer, m.logger)
				if err != nil {
					m.logger.Warn("jellyfin: exit ", userID, " encryption: ", err)
					_ = client.Stop(context.Background())
					if !waitRetry() {
						return
					}
					continue
				}
				tr = secured
			}
			if sink := m.transportSink(); sink != nil {
				release, aggregated := sink(userID, tr)
				if release == nil {
					m.logger.Debug("jellyfin: exit ", userID, " tube was not accepted by the aggregator")
					_ = client.Stop(context.Background())
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
				_ = client.Stop(context.Background())
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
			mux := tunnel.New(tr, m.logger, muxCfg)

			h := handler
			if h == nil {
				h = func(ctx context.Context, _ multiplexing.StreamMultiplexer) error {
					<-ctx.Done()
					return ctx.Err()
				}
			}
			if err := mux.Start(sessionCtx); err != nil {
				if sessionCtx.Err() == nil {
					m.logger.Warn("jellyfin: exit ", userID, " mux start: ", err)
				}
				_ = client.Stop(context.Background())
				if !waitRetry() {
					return
				}
				continue
			}
			session.setMux(mux)
			if err := h(sessionCtx, mux); err != nil && sessionCtx.Err() == nil {
				m.logger.Warn("jellyfin: exit ", userID, " session: ", err)
			}
			session.setMux(nil)
			stopCtx, stopCancel := context.WithTimeout(context.Background(), sessionStopTimeout)
			if err := mux.Stop(stopCtx); err != nil {
				m.logger.Debug("jellyfin: exit ", userID, " mux stop: ", err)
			}
			stopCancel()
			_ = client.Stop(context.Background())
			if !waitRetry() {
				return
			}
		}
	}()
}

func (m *MultiServer) WalkLayers() []transport.Layer {
	m.mu.Lock()
	sessions := make([]*exitSession, 0, len(m.sessions))
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
				layer.Type = "jellyfin"
				pipes = append(pipes, layer)
			}
		}
	}

	out := make([]transport.Layer, 0, len(pipes)+1)
	out = append(out, transport.Layer{
		Kind: "mux", Type: "jellyfin",
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

var _ app.SessionServer = (*MultiServer)(nil)
var _ app.AggregableSession = (*MultiServer)(nil)
var _ transport.Telemetry = (*MultiServer)(nil)
