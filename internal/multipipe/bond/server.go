package bond

import (
	"context"
	"fmt"
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

const aggregateStopTimeout = 2 * time.Second

var aggregateRendezvousTimeout = 15 * time.Second

type Server struct {
	names    []string
	logger   logging.ContextLogger
	muxCfg   tunnel.Config
	registry *auth.UserRegistry
	children []app.SessionServer

	password   string
	exitNodeID string

	mu      sync.Mutex
	handler app.SessionHandler
	started bool
	ctx     context.Context
	cancel  context.CancelFunc
	users   map[string]*aggregate
	wg      sync.WaitGroup
}

type aggregate struct {
	userID string

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	tubes   map[string]transport.Transport
	raw     map[string]transport.Transport
	started bool
	mux     multiplexing.StreamMultiplexer
	conn    *connManager

	once    sync.Once
	timeout *time.Timer
}

func NewServer(
	names []string,
	children []app.SessionServer,
	logger logging.ContextLogger,
	cfg tunnel.Config,
	registry *auth.UserRegistry,
) *Server {
	cfg.Registry = registry
	return &Server{
		names:    append([]string(nil), names...),
		logger:   logger,
		muxCfg:   cfg,
		registry: registry,
		children: children,
		users:    make(map[string]*aggregate),
	}
}

func (s *Server) SetSessionHandler(handler app.SessionHandler) {
	s.mu.Lock()
	s.handler = handler
	s.mu.Unlock()
}

func (s *Server) SetEncryptionPassword(password string) {
	s.mu.Lock()
	s.password = password
	s.mu.Unlock()
}

func (s *Server) SetExitNodeID(id string) {
	s.mu.Lock()
	s.exitNodeID = id
	s.mu.Unlock()
}

func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.started = true
	s.mu.Unlock()

	for i, child := range s.children {
		if err := child.Start(ctx); err != nil {
			for j := i - 1; j >= 0; j-- {
				_ = s.children[j].Stop(context.Background())
			}
			s.mu.Lock()
			s.started = false
			s.mu.Unlock()
			return fmt.Errorf("bond: start child session %d: %w", i, err)
		}
	}
	s.logger.Info("bond: exit aggregator started, tubes=", len(s.names),
		" children=", len(s.children))
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = false
	cancel := s.cancel
	aggs := make([]*aggregate, 0, len(s.users))
	for _, agg := range s.users {
		aggs = append(aggs, agg)
	}
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, agg := range aggs {
		s.teardown(agg, "server stopped")
	}
	for i := len(s.children) - 1; i >= 0; i-- {
		_ = s.children[i].Stop(ctx)
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) Sink(tube string) app.SessionTransportSink {
	return func(userID string, tr transport.Transport) (func(), <-chan struct{}) {
		return s.register(tube, userID, tr)
	}
}

func (s *Server) register(tube, userID string, tr transport.Transport) (func(), <-chan struct{}) {
	s.mu.Lock()
	if !s.started || s.ctx == nil || s.ctx.Err() != nil {
		s.mu.Unlock()
		stopCtx, cancel := context.WithTimeout(context.Background(), aggregateStopTimeout)
		_ = tr.Stop(stopCtx)
		cancel()
		return nil, nil
	}

	agg := s.users[userID]
	if agg == nil {
		ctx, cancel := context.WithCancel(s.ctx)
		agg = &aggregate{
			userID: userID,
			ctx:    ctx,
			cancel: cancel,
			done:   make(chan struct{}),
			tubes:  make(map[string]transport.Transport),
			raw:    make(map[string]transport.Transport),
		}
		s.users[userID] = agg
	}
	if agg.timeout == nil && !agg.started {
		agg.timeout = time.AfterFunc(aggregateRendezvousTimeout, func() {
			s.teardown(agg, "rendezvous timeout")
		})
	}
	previousRaw := agg.raw[tube]
	replaced := previousRaw != nil && previousRaw != tr
	agg.tubes[tube] = tr
	agg.raw[tube] = tr
	ready := !replaced && len(agg.tubes) >= len(s.names)
	started := agg.started
	s.mu.Unlock()

	release := func() { s.teardown(agg, "tube released") }

	if replaced {
		stopCtx, cancel := context.WithTimeout(context.Background(), aggregateStopTimeout)
		_ = previousRaw.Stop(stopCtx)
		cancel()
		if started {

			s.teardown(agg, "tube replaced")
			return func() {}, agg.done
		}
		return release, agg.done
	}
	if ready {
		s.startAggregate(agg)
	}

	return release, agg.done
}

func (s *Server) startAggregate(agg *aggregate) {
	s.mu.Lock()
	if agg.started || s.users[agg.userID] != agg {
		s.mu.Unlock()
		return
	}
	if len(agg.tubes) < len(s.names) {
		s.mu.Unlock()
		return
	}
	agg.started = true
	if agg.timeout != nil {
		agg.timeout.Stop()
	}
	names := make([]string, len(s.names))
	tubes := make([]tubeLike, len(s.names))
	transports := make([]transport.Transport, len(s.names))
	var ensureErr error
	for i, name := range s.names {
		tr := agg.tubes[name]
		if tr == nil {
			agg.started = false
			s.mu.Unlock()
			return
		}
		wrapped, err := security.EnsureSecureTransport(tr, s.password, security.RoleServer, s.logger)
		if err != nil {
			ensureErr = fmt.Errorf("secure tube %s: %w", name, err)
			agg.started = false
			break
		}
		agg.tubes[name] = wrapped
		names[i] = name
		transports[i] = wrapped
		tubes[i] = &transportTube{t: wrapped}
	}
	handler := s.handler
	exitNodeID := s.exitNodeID
	s.mu.Unlock()
	if ensureErr != nil {
		s.teardown(agg, ensureErr.Error())
		return
	}

	for i, tr := range transports {
		if err := tr.Start(agg.ctx); err != nil {
			s.teardown(agg, fmt.Sprintf("start tube %s: %v", names[i], err))
			return
		}
	}

	session := newConnManager(agg.ctx, names, tubes, s.logger)
	ct := &connTransport{session: session, maxPayload: session.MaxPayload(), numTubes: len(tubes)}
	cfg := s.muxCfg
	cfg.Registry = s.registry

	cfg.UserID = agg.userID
	cfg.ExitNodeID = exitNodeID
	cfg.OnSessionError = func(err error) {

		go s.teardown(agg, "session error: "+err.Error())
	}
	mux := tunnel.New(ct, s.logger, cfg)
	if err := mux.Start(agg.ctx); err != nil {
		_ = session.Close()
		s.teardown(agg, "tunnel start: "+err.Error())
		return
	}

	s.mu.Lock()
	if s.users[agg.userID] != agg {
		s.mu.Unlock()
		_ = mux.Stop(context.Background())
		_ = session.Close()
		return
	}
	agg.mux = mux
	agg.conn = session
	s.mu.Unlock()

	if handler == nil {
		handler = func(ctx context.Context, _ multiplexing.StreamMultiplexer) error {
			<-ctx.Done()
			return ctx.Err()
		}
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := handler(agg.ctx, mux); err != nil && agg.ctx.Err() == nil {
			s.logger.Warn("bond: user ", agg.userID, " handler: ", err)
		}
		s.teardown(agg, "handler finished")
	}()

	s.logger.Info("bond: aggregated session ready for user ", agg.userID,
		" tubes=", len(names))
}

func (s *Server) teardown(agg *aggregate, reason string) {
	if agg == nil {
		return
	}
	agg.once.Do(func() {
		s.mu.Lock()
		timer := agg.timeout
		agg.timeout = nil
		if s.users[agg.userID] == agg {
			delete(s.users, agg.userID)
		}
		mux := agg.mux
		conn := agg.conn
		agg.mux = nil
		agg.conn = nil
		tubes := make([]transport.Transport, 0, len(agg.tubes))
		for _, tr := range agg.tubes {
			tubes = append(tubes, tr)
		}
		agg.tubes = make(map[string]transport.Transport)
		agg.raw = make(map[string]transport.Transport)
		s.mu.Unlock()

		if timer != nil {
			timer.Stop()
		}

		close(agg.done)

		if mux != nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), aggregateStopTimeout)
			_ = mux.Stop(stopCtx)
			cancel()
		}
		if conn != nil {
			_ = conn.Close()
		}
		agg.cancel()
		for _, tr := range tubes {
			stopCtx, cancel := context.WithTimeout(context.Background(), aggregateStopTimeout)
			_ = tr.Stop(stopCtx)
			cancel()
		}

		s.logger.Info("bond: aggregated session for user ", agg.userID,
			" closed (", reason, ")")
	})
}

func (s *Server) WalkLayers() []transport.Layer {
	s.mu.Lock()
	aggs := make([]*aggregate, 0, len(s.users))
	for _, agg := range s.users {
		aggs = append(aggs, agg)
	}
	type tubeSnapshot struct {
		name string
		tr   transport.Transport
	}
	muxes := make([]multiplexing.StreamMultiplexer, 0, len(aggs))
	tubes := make([]tubeSnapshot, 0, len(aggs)*len(s.names))
	for _, agg := range aggs {
		if agg.mux != nil {
			muxes = append(muxes, agg.mux)
		}
		for name, tr := range agg.tubes {
			tubes = append(tubes, tubeSnapshot{name: name, tr: tr})
		}
	}
	s.mu.Unlock()

	var appSent, appRecv, wireSent, wireRecv int64
	for _, mux := range muxes {
		tel, ok := mux.(transport.Telemetry)
		if !ok {
			continue
		}
		for _, l := range tel.WalkLayers() {
			if l.Kind != "mux" {
				continue
			}
			appSent += l.Snapshot.UsefulSent
			appRecv += l.Snapshot.UsefulRecv
			wireSent += l.Snapshot.NetworkSent
			wireRecv += l.Snapshot.NetworkRecv
			break
		}
	}

	layers := make([]transport.Layer, 0, len(tubes)+1)
	layers = append(layers, transport.Layer{
		Kind: "mux", Type: "bond",
		Snapshot: transport.Snapshot{
			UsefulSent:  appSent,
			UsefulRecv:  appRecv,
			NetworkSent: wireSent,
			NetworkRecv: wireRecv,
			Timestamp:   time.Now(),
		},
	})
	for _, t := range tubes {
		tel, ok := t.tr.(transport.Telemetry)
		if !ok {
			continue
		}
		for _, l := range tel.WalkLayers() {
			if l.Kind != "pipe" {
				continue
			}
			if l.Name == "" {
				l.Name = t.name
			}
			layers = append(layers, l)
		}
	}
	return layers
}

var _ app.SessionServer = (*Server)(nil)
var _ transport.Telemetry = (*Server)(nil)
