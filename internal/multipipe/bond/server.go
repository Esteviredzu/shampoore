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
	live    int
	mux     multiplexing.StreamMultiplexer
	conn    *connManager

	once    sync.Once
	timeout *time.Timer
}

const (
	tubeStartRetryBase = time.Second
	tubeStartRetryMax  = 10 * time.Second
)

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
	previousRaw := agg.raw[tube]
	replaced := previousRaw != nil && previousRaw != tr
	agg.tubes[tube] = tr
	agg.raw[tube] = tr
	started := agg.started
	doneCh := agg.done
	s.mu.Unlock()

	release := func() {
		s.mu.Lock()
		current := s.users[userID]
		matches := current == agg && agg.raw[tube] == tr
		empty := false
		var cm *connManager
		if matches {
			delete(agg.tubes, tube)
			delete(agg.raw, tube)
			empty = len(agg.tubes) == 0 || s.ctx == nil || s.ctx.Err() != nil
			cm = agg.conn
		}
		s.mu.Unlock()
		if !matches {
			return
		}
		if cm != nil {
			cm.dropTube(tube)
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), aggregateStopTimeout)
		_ = tr.Stop(stopCtx)
		cancel()
		if empty {
			go s.teardown(agg, "all tubes released")
		}
	}

	if replaced {
		stopCtx, cancel := context.WithTimeout(context.Background(), aggregateStopTimeout)
		_ = previousRaw.Stop(stopCtx)
		cancel()
	}

	if !started {

		s.startAggregate(agg)
		return release, doneCh
	}
	if replaced {
		go s.attachTube(agg, tube, tr)
		return release, doneCh
	}
	go s.attachTube(agg, tube, tr)
	return release, doneCh
}

func (s *Server) attachTube(agg *aggregate, tube string, raw transport.Transport) {
	if agg == nil || raw == nil {
		return
	}
	s.mu.Lock()
	password := s.password
	logger := s.logger
	aggCtx := agg.ctx
	s.mu.Unlock()

	wrapped, err := wrapBondTube(raw, password, security.RoleServer, logger)
	if err != nil {
		logger.Warn("bond: secure tube ", tube, ": ", err)
		return
	}

	backoff := tubeStartRetryBase
	for {
		if aggCtx.Err() != nil {
			stopTransport(wrapped)
			return
		}
		if err := wrapped.Start(aggCtx); err == nil {
			break
		} else {
			logger.Warn("bond: start tube ", tube, ": ", err)
		}
		select {
		case <-aggCtx.Done():
			stopTransport(wrapped)
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > tubeStartRetryMax {
			backoff = tubeStartRetryMax
		}
	}

	deadline := time.Now().Add(aggregateRendezvousTimeout)
	for {
		s.mu.Lock()
		live := s.users[agg.userID] == agg && agg.started
		cm := agg.conn
		if live && cm != nil {
			agg.tubes[tube] = wrapped
		}
		s.mu.Unlock()
		if !live {
			stopTransport(wrapped)
			return
		}
		if cm != nil {
			cm.addTube(tube, &transportTube{t: wrapped})
			s.noteLiveTube(agg)
			return
		}
		if time.Now().After(deadline) {
			logger.Warn("bond: aggregate for ", agg.userID, " never became ready for tube ", tube)
			stopTransport(wrapped)
			return
		}
		select {
		case <-aggCtx.Done():
			stopTransport(wrapped)
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s *Server) noteLiveTube(agg *aggregate) {
	s.mu.Lock()
	if agg.live == 0 && agg.timeout != nil {
		agg.timeout.Stop()
		agg.timeout = nil
	}
	agg.live++
	s.mu.Unlock()
}

func stopTransport(tr transport.Transport) {
	if tr == nil {
		return
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), aggregateStopTimeout)
	_ = tr.Stop(stopCtx)
	cancel()
}

func (s *Server) startAggregate(agg *aggregate) {
	s.mu.Lock()
	if agg.started || s.users[agg.userID] != agg {
		s.mu.Unlock()
		return
	}
	agg.started = true
	handler := s.handler
	exitNodeID := s.exitNodeID
	aggCtx := agg.ctx
	pending := make(map[string]transport.Transport, len(agg.raw))
	for name, tr := range agg.raw {
		pending[name] = tr
	}
	s.mu.Unlock()

	hintTubes := make([]tubeLike, 0, len(pending))
	for _, tr := range pending {
		if tr != nil {
			hintTubes = append(hintTubes, &transportTube{t: tr})
		}
	}
	session := newConnManager(aggCtx, nil, nil, s.logger)
	if hint := aggregateMaxPayload(hintTubes); hint > 0 && hint < session.payload {
		session.payload = hint
	}
	ct := &connTransport{session: session, maxPayload: session.MaxPayload()}

	s.mu.Lock()
	if s.users[agg.userID] != agg {
		s.mu.Unlock()
		_ = session.Close()
		return
	}
	agg.conn = session
	for name, tr := range agg.raw {
		if _, ok := pending[name]; !ok {
			pending[name] = tr
		}
	}
	launched := make(map[string]struct{}, len(pending))
	for name := range pending {
		launched[name] = struct{}{}
	}
	s.mu.Unlock()

	for name, tr := range pending {
		go s.attachTube(agg, name, tr)
	}

	cfg := s.muxCfg
	cfg.Registry = s.registry

	cfg.UserID = agg.userID
	cfg.ExitNodeID = exitNodeID
	cfg.OnSessionError = func(err error) {

		go s.teardown(agg, "session error: "+err.Error())
	}
	mux := tunnel.New(ct, s.logger, cfg)
	if err := mux.Start(aggCtx); err != nil {
		_ = session.Close()
		s.teardown(agg, "tunnel start: "+err.Error())
		return
	}

	s.mu.Lock()
	if s.users[agg.userID] != agg {
		s.mu.Unlock()
		stopCtx, cancelStop := context.WithTimeout(context.Background(), aggregateStopTimeout)
		_ = mux.Stop(stopCtx)
		cancelStop()
		_ = session.Close()
		return
	}
	agg.mux = mux

	extra := make(map[string]transport.Transport, len(agg.raw))
	for name, tr := range agg.raw {
		if _, ok := launched[name]; !ok {
			extra[name] = tr
		}
	}
	if agg.timeout == nil {
		agg.timeout = time.AfterFunc(aggregateRendezvousTimeout, func() {
			s.mu.Lock()
			live := agg.live > 0
			s.mu.Unlock()
			if !live {
				s.teardown(agg, "rendezvous timeout")
			}
		})
	}
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

	for name, tr := range extra {
		select {
		case <-aggCtx.Done():
			return
		default:
		}
		go s.attachTube(agg, name, tr)
	}

	s.logger.Info("bond: aggregated session ready for user ", agg.userID,
		" pending tubes=", len(pending)+len(extra))
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
		live := agg.live
		agg.mux = nil
		agg.conn = nil
		tubes := make([]transport.Transport, 0, len(agg.tubes)+len(agg.raw))
		for _, tr := range agg.tubes {
			tubes = append(tubes, tr)
		}
		for name, tr := range agg.raw {
			if _, dup := agg.tubes[name]; !dup {
				tubes = append(tubes, tr)
			}
		}
		agg.tubes = make(map[string]transport.Transport)
		agg.raw = make(map[string]transport.Transport)
		s.mu.Unlock()

		if timer != nil {
			timer.Stop()
		}

		close(agg.done)

		if live == 0 && conn != nil {
			_ = conn.Close()
			conn = nil
		}

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
