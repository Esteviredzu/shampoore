package jellyfin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
	"github.com/Esteviredzu/shampoore/internal/transport/httpstream"
)

const (
	defaultFlushThreshold = 12000
	defaultFlushInterval  = 20 * time.Millisecond
)

type Server struct {
	cfg      Config
	logger   logging.ContextLogger
	registry *auth.UserRegistry
	password string
	muxCfg   tunnel.Config

	mu          sync.Mutex
	engine      *httpstream.Handler
	handler     app.SessionHandler
	transportIn app.SessionTransportSink
	exitNodeID  string
	httpSrv     *http.Server
	listener    net.Listener
	ctx         context.Context
	cancel      context.CancelFunc
	started     bool
	wg          sync.WaitGroup
}

func newServer(cfg Config, logger logging.ContextLogger, registry *auth.UserRegistry, password string) (*Server, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validateForRole("server"); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = logging.New("jellyfin", false)
	}
	return &Server{
		cfg:      cfg,
		logger:   logger,
		registry: registry,
		password: strings.TrimSpace(password),
		muxCfg: tunnel.Config{
			FlushThreshold: defaultFlushThreshold,
			FlushInterval:  defaultFlushInterval,
			Registry:       registry,
		},
	}, nil
}

func (s *Server) Handler() http.Handler {
	engine, err := s.ensureEngine()
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "jellyfin: not configured", http.StatusInternalServerError)
		})
	}
	return engine
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) SessionCount() int {
	engine, err := s.ensureEngine()
	if err != nil {
		return 0
	}
	return engine.SessionCount()
}

func (s *Server) ensureEngine() (*httpstream.Handler, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.engine != nil {
		return s.engine, nil
	}
	profile := profileFor(s.cfg.UTLSProfile)
	cfg := httpstream.ServerConfig{
		Secret:           s.cfg.Secret,
		Namespace:        s.cfg.Namespace,
		PinnedUserID:     s.cfg.UserID,
		PinnedExitNodeID: s.exitNodeID,
		RequireUser:      s.cfg.Multiuser,
		FallbackHandler:  newFallbackHandler(s.cfg.UpstreamURL, s.logger),
		Logger:           s.logger,
	}
	if s.registry != nil {
		registry := s.registry
		cfg.Authorize = func(_ context.Context, info httpstream.SessionInfo) error {
			return registry.Authorize(info.UserID, info.ExitNodeID)
		}
	}
	engine := httpstream.NewHandler(profile, cfg, s.handleSession)
	if err := engine.Err(); err != nil {
		return nil, fmt.Errorf("jellyfin: %w", err)
	}
	s.engine = engine
	return engine, nil
}

func (s *Server) SetSessionHandler(handler app.SessionHandler) {
	s.mu.Lock()
	s.handler = handler
	s.mu.Unlock()
}

func (s *Server) SetTransportSink(sink app.SessionTransportSink) {
	s.mu.Lock()
	s.transportIn = sink
	s.mu.Unlock()
}

func (s *Server) SetExitNodeID(id string) {
	s.mu.Lock()
	s.exitNodeID = strings.TrimSpace(id)
	s.mu.Unlock()
}

func (s *Server) sessionHandler() app.SessionHandler {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handler
}

func (s *Server) transportSink() app.SessionTransportSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transportIn
}

func (s *Server) Start(ctx context.Context) error {
	engine, err := s.ensureEngine()
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.started = true
	listen := s.cfg.Listen
	s.mu.Unlock()

	if listen == "" {
		return nil
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		s.mu.Lock()
		s.started = false
		if s.cancel != nil {
			s.cancel()
			s.cancel = nil
		}
		s.mu.Unlock()
		return fmt.Errorf("jellyfin: listen %s: %w", listen, err)
	}
	httpSrv := &http.Server{
		Handler:           engine,
		ReadHeaderTimeout: 10 * time.Second,
	}
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		_ = listener.Close()
		return nil
	}
	s.listener = listener
	s.httpSrv = httpSrv
	s.wg.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.wg.Done()
		if serveErr := httpSrv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			s.logger.Warn("jellyfin: http server stopped: ", serveErr)
		}
	}()
	s.logger.Info("jellyfin: HTTP listener on ", listener.Addr().String())
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	httpSrv := s.httpSrv
	engine := s.engine
	cancel := s.cancel
	s.httpSrv = nil
	s.listener = nil
	s.cancel = nil
	s.started = false
	s.mu.Unlock()

	if httpSrv != nil {
		if ctx == nil {
			var stopCancel context.CancelFunc
			ctx, stopCancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
		}
		if err := httpSrv.Shutdown(ctx); err != nil {
			s.logger.Debug("jellyfin: http shutdown: ", err)
		}
	}
	if cancel != nil {
		cancel()
	}
	if engine != nil {
		if err := engine.Close(); err != nil {
			s.logger.Debug("jellyfin: engine close: ", err)
		}
	}
	s.wg.Wait()
	return nil
}

func (s *Server) WalkLayers() []transport.Layer {
	engine, err := s.ensureEngine()
	if err != nil {
		return nil
	}
	return renameLayers(engine.WalkLayers())
}

type jellyfinTransport struct {
	transport.Transport
}

func (t jellyfinTransport) WalkLayers() []transport.Layer {
	if tel, ok := t.Transport.(transport.Telemetry); ok {
		return renameLayers(tel.WalkLayers())
	}
	return nil
}

var _ app.SessionServer = (*Server)(nil)
var _ app.AggregableSession = (*Server)(nil)
var _ transport.Telemetry = (*Server)(nil)
