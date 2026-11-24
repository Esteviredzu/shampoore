package jellyfin

import (
	"context"
	"fmt"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
	"github.com/Esteviredzu/shampoore/internal/transport/httpstream"
)

const sessionStopTimeout = 5 * time.Second

func buildUserRegistry(specs []auth.UserSpec) (*auth.UserRegistry, error) {
	registry := auth.NewRegistry()
	for _, spec := range specs {
		if err := registry.Upsert(spec); err != nil {
			return nil, fmt.Errorf("jellyfin: user %q: %w", spec.UserID, err)
		}
	}
	return registry, nil
}

func (s *Server) effectiveRegistry() *auth.UserRegistry {
	if s.registry == nil || !s.registry.Active() {
		return nil
	}
	return s.registry
}

func (s *Server) handleSession(ctx context.Context, sess *httpstream.Session) error {
	info := sess.Info()
	s.logger.Info("jellyfin: session started (namespace=", info.Namespace,
		" user=", info.UserID, " sid=", shortSession(info.SessionID), ")")

	var tr transport.Transport = jellyfinTransport{Transport: sess}
	if s.password != "" {
		secured, err := security.WrapTransport(tr, s.password, security.RoleServer, s.logger)
		if err != nil {
			return fmt.Errorf("jellyfin: wrap session: %w", err)
		}
		tr = secured
	}

	if sink := s.transportSink(); sink != nil {
		release, done := sink(info.UserID, tr)
		if release == nil {
			s.logger.Debug("jellyfin: session for ", info.UserID,
				" was not accepted by the aggregator")
			return nil
		}
		select {
		case <-ctx.Done():
		case <-done:
		}
		release()
		return nil
	}

	cfg := s.muxCfg
	cfg.UserID = info.UserID
	cfg.ExitNodeID = info.ExitNodeID
	if cfg.ExitNodeID == "" {
		s.mu.Lock()
		cfg.ExitNodeID = s.exitNodeID
		s.mu.Unlock()
	}
	cfg.Registry = s.effectiveRegistry()

	mux := tunnel.New(tr, s.logger, cfg)
	if err := mux.Start(ctx); err != nil {
		return fmt.Errorf("jellyfin: mux start: %w", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), sessionStopTimeout)
		defer cancel()
		if err := mux.Stop(stopCtx); err != nil {
			s.logger.Debug("jellyfin: mux stop: ", err)
		}
	}()

	handler := s.sessionHandler()
	if handler == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	return handler(ctx, mux)
}

func shortSession(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

var _ app.SessionServer = (*Server)(nil)
