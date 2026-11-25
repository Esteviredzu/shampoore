package livekit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	lkauth "github.com/livekit/protocol/auth"
	livekitpb "github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
)

const (
	roomDeleteTimeout  = 2 * time.Second
	sessionStopTimeout = 2 * time.Second
)

type roomEvents struct {
	DataTopic           string
	OnDataPacket        func(payload []byte, sender string)
	OnParticipantJoined func(identity string)
	OnParticipantLeft   func(identity string)
	OnDisconnected      func()
}

type roomSession interface {
	LocalParticipant() *lksdk.LocalParticipant
	RemoteParticipantCount() int
	Disconnect()
}

type roomDialFunc func(ctx context.Context, url, token string, events roomEvents) (roomSession, error)

type lkRoomSession struct {
	room *lksdk.Room
}

func (r *lkRoomSession) LocalParticipant() *lksdk.LocalParticipant {
	return r.room.LocalParticipant
}

func (r *lkRoomSession) RemoteParticipantCount() int {
	return len(r.room.GetRemoteParticipants())
}

func (r *lkRoomSession) Disconnect() {
	r.room.Disconnect()
}

func BuildUserRegistry(specs []auth.UserSpec) (*auth.UserRegistry, error) {
	registry := auth.NewRegistry()
	for _, spec := range specs {
		if err := registry.Upsert(spec); err != nil {
			return nil, fmt.Errorf("livekit: user %q: %w", spec.UserID, err)
		}
	}
	return registry, nil
}

func makeTokenFor(apiKey, apiSecret, room, identity string, ttl time.Duration) (string, error) {
	at := lkauth.NewAccessToken(apiKey, apiSecret)
	grant := &lkauth.VideoGrant{RoomJoin: true, Room: room}
	at.SetVideoGrant(grant).SetIdentity(identity).SetValidFor(ttl)
	return at.ToJWT()
}

type MultiServer struct {
	cfg      Config
	logger   logging.ContextLogger
	registry *auth.UserRegistry
	muxCfg   tunnel.Config
	password string

	exitNodeID string

	serverTTL time.Duration

	connectRoom roomDialFunc
	deleteRoom  func(ctx context.Context, room string) error

	mu           sync.Mutex
	handler      app.SessionHandler
	transportIn  app.SessionTransportSink
	sessions     map[string]*activeSession
	starting     map[string]struct{}
	ctx          context.Context
	cancel       context.CancelFunc
	started      bool
	unsubscribe  func()
	syncInterval time.Duration
	syncCh       chan struct{}
	wg           sync.WaitGroup
	syncMu       sync.Mutex
}

func NewMultiServer(
	cfg Config,
	logger logging.ContextLogger,
	registry *auth.UserRegistry,
	muxCfg tunnel.Config,
	password string,
) *MultiServer {
	cfg.applyDefaults()
	muxCfg.Registry = registry
	m := &MultiServer{
		cfg:          cfg,
		logger:       logger,
		registry:     registry,
		muxCfg:       muxCfg,
		password:     password,
		serverTTL:    cfg.serverTokenTTL(),
		sessions:     make(map[string]*activeSession),
		starting:     make(map[string]struct{}),
		syncInterval: defaultSyncInterval,
		syncCh:       make(chan struct{}, 1),
	}
	m.connectRoom = connectLiveKitRoom
	m.deleteRoom = m.deleteRemoteRoom
	return m
}

func (m *MultiServer) ActiveSessions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
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

func connectLiveKitRoom(ctx context.Context, url, token string, events roomEvents) (roomSession, error) {
	cb := &lksdk.RoomCallback{
		ParticipantCallback: lksdk.ParticipantCallback{
			OnDataPacket: func(data lksdk.DataPacket, params lksdk.DataReceiveParams) {
				if events.OnDataPacket == nil {
					return
				}
				pkt, ok := data.(*lksdk.UserDataPacket)
				if !ok {
					return
				}
				if events.DataTopic != "" && pkt.Topic != events.DataTopic {
					return
				}
				events.OnDataPacket(pkt.Payload, params.SenderIdentity)
			},
		},
		OnParticipantConnected: func(rp *lksdk.RemoteParticipant) {
			if events.OnParticipantJoined != nil && rp != nil {
				events.OnParticipantJoined(rp.Identity())
			}
		},
		OnParticipantDisconnected: func(rp *lksdk.RemoteParticipant) {
			if events.OnParticipantLeft != nil {
				identity := ""
				if rp != nil {
					identity = rp.Identity()
				}
				events.OnParticipantLeft(identity)
			}
		},
		OnDisconnected: func() {
			if events.OnDisconnected != nil {
				events.OnDisconnected()
			}
		},
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	room, err := lksdk.ConnectToRoomWithToken(
		url,
		token,
		cb,
		lksdk.WithConnectTimeout(connectTimeout),
	)
	if err != nil {
		return nil, err
	}
	return &lkRoomSession{room: room}, nil
}

func (m *MultiServer) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return nil
	}
	if strings.TrimSpace(m.cfg.SignalURL) == "" || m.cfg.APIKey == "" || m.cfg.APISecret == "" {
		m.mu.Unlock()
		return errors.New("livekit: signal_url, api_key and api_secret are required for multiuser mode")
	}
	serveCtx, cancel := context.WithCancel(ctx)
	m.ctx = serveCtx
	m.cancel = cancel
	m.started = true
	m.mu.Unlock()

	if m.registry != nil {
		unsub := m.registry.Subscribe(m.syncUsers)
		m.mu.Lock()
		if !m.started {
			m.mu.Unlock()
			unsub()
			cancel()
			return nil
		}
		m.unsubscribe = unsub
		m.mu.Unlock()
	}
	m.wg.Add(1)
	go m.reconcileLoop(serveCtx)
	m.logger.Info("livekit: multiuser subscription server ready (room_prefix=", m.cfg.RoomPrefix, ")")
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
	sessions := make([]*activeSession, 0, len(m.sessions))
	for room, session := range m.sessions {
		sessions = append(sessions, session)
		delete(m.sessions, room)
	}
	m.cancel = nil
	m.unsubscribe = nil
	m.mu.Unlock()

	if unsub != nil {
		unsub()
	}
	if cancel != nil {
		cancel()
	}
	for _, session := range sessions {
		session.close("server stopped")
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

func (m *MultiServer) reconcileLoop(ctx context.Context) {
	defer m.wg.Done()
	interval := m.syncInterval
	if interval <= 0 {
		interval = defaultSyncInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.syncCh:
			m.syncUsers()
		case <-ticker.C:
			m.syncUsers()
		}
	}
}

func (m *MultiServer) requestSync() {
	if m.syncCh == nil {
		return
	}
	select {
	case m.syncCh <- struct{}{}:
	default:
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
	existing := make(map[string]*activeSession, len(m.sessions))
	for _, session := range m.sessions {
		existing[session.userID] = session
	}
	var toStart []string
	for userID := range active {
		if _, ok := existing[userID]; ok {
			continue
		}
		if _, ok := m.starting[RoomForUser(m.cfg.RoomPrefix, userID)]; ok {
			continue
		}
		toStart = append(toStart, userID)
	}
	var toStop []*activeSession
	for userID, session := range existing {
		if _, ok := active[userID]; ok {
			continue
		}
		toStop = append(toStop, session)
		delete(m.sessions, session.room)
	}
	m.mu.Unlock()

	for _, session := range toStop {
		m.logger.Info("livekit: stopping user session ", session.userID, " (room=", session.room, ")")
		session.close("user inactive")
	}
	for _, userID := range toStart {
		m.startUserSession(ctx, userID)
	}
}

func (m *MultiServer) startUserSession(ctx context.Context, userID string) {
	if ctx == nil {
		return
	}
	room := RoomForUser(m.cfg.RoomPrefix, userID)

	m.mu.Lock()
	if !m.started || ctx.Err() != nil {
		m.mu.Unlock()
		return
	}
	if _, ok := m.sessions[room]; ok {
		m.mu.Unlock()
		return
	}
	if _, ok := m.starting[room]; ok {
		m.mu.Unlock()
		return
	}
	exitNodeID := m.exitNodeID
	m.starting[room] = struct{}{}
	m.wg.Add(1)
	m.mu.Unlock()

	m.logger.Info("livekit: starting user session ", userID, " (room=", room, ")")
	go func() {
		defer m.wg.Done()
		defer func() {
			m.mu.Lock()
			delete(m.starting, room)
			m.mu.Unlock()
		}()
		if _, err := m.openSession(ctx, room, userID, exitNodeID); err != nil && ctx.Err() == nil {
			m.logger.Warn("livekit: user ", userID, " session: ", err)
		}
	}()
}

func (m *MultiServer) openSession(
	parent context.Context,
	room, userID, exitNodeID string,
) (*activeSession, error) {
	serverIdentity := "server-" + shortID()
	serverToken, err := makeTokenFor(m.cfg.APIKey, m.cfg.APISecret, room, serverIdentity, m.serverTTL)
	if err != nil {
		return nil, fmt.Errorf("livekit: server token: %w", err)
	}

	sessionCtx, cancel := context.WithCancel(parent)
	session := &activeSession{
		server:     m,
		room:       room,
		userID:     userID,
		exitNodeID: exitNodeID,
		identity:   serverIdentity,
		ctx:        sessionCtx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	session.touch()

	events := roomEvents{
		DataTopic: m.cfg.DataTopic,
		OnDataPacket: func(payload []byte, sender string) {
			session.touch()
			session.pushIncoming(payload, sender)
		},
		OnParticipantJoined: func(identity string) {
			session.touch()
			session.setRemote(identity)
			m.logger.Info("livekit: client ", identity, " joined room ", room)
		},
		OnParticipantLeft: func(identity string) {
			m.logger.Info("livekit: client ", identity, " left room ", room)
			session.touch()
			session.peerLeft()
		},
		OnDisconnected: func() {
			session.closeKeepRoom("server room disconnected")
		},
	}

	roomConn, err := m.connectRoom(sessionCtx, m.cfg.SignalURL, serverToken, events)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("livekit: connect server participant: %w", err)
	}
	if sessionCtx.Err() != nil {
		roomConn.Disconnect()
		cancel()
		return nil, context.Canceled
	}

	pipe := newPipe(roomConn.LocalParticipant(), m.cfg.DataTopic, userID)

	pipe.SetAdoptFirstPacket(true)
	pipe.SetDeadHandler(func() {
		session.closeKeepRoom("heartbeat timeout")
	})
	secured, err := security.WrapPipeWithRole(pipe, m.password, "server", m.logger)
	if err != nil {
		_ = pipe.Stop(context.Background())
		roomConn.Disconnect()
		cancel()
		return nil, fmt.Errorf("livekit: wrap encryption: %w", err)
	}
	muxCfg := m.muxCfg

	muxCfg.UserID = userID
	muxCfg.ExitNodeID = exitNodeID
	muxCfg.Registry = m.registry

	tr := NewTransport(secured)
	var (
		mux            *tunnel.TunnelMultiplexer
		release        func()
		aggregatedDone <-chan struct{}
	)
	if sink := m.transportSink(); sink != nil {
		release, aggregatedDone = sink(userID, tr)
	}
	aggregated := release != nil
	if !aggregated {
		mux = tunnel.New(tr, m.logger, muxCfg)
	}

	stopOwned := func() {
		if mux != nil {
			_ = mux.Stop(context.Background())
		}
		if release != nil {
			release()
		}
	}

	session.mu.Lock()
	session.roomConn = roomConn
	session.pipe = pipe
	session.mux = mux
	session.aggregated = aggregated
	session.release = release
	session.aggregatedDone = aggregatedDone
	session.lastSent = pipe.BytesSent()
	session.lastRecv = pipe.BytesRecv()
	closed := session.closed
	session.mu.Unlock()
	if closed {
		stopOwned()
		_ = pipe.Stop(context.Background())
		roomConn.Disconnect()
		return nil, context.Canceled
	}

	if mux != nil {
		if err := mux.Start(sessionCtx); err != nil {
			session.close("mux start failed")
			return nil, fmt.Errorf("livekit: start mux: %w", err)
		}
	}

	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		stopOwned()
		_ = pipe.Stop(context.Background())
		roomConn.Disconnect()
		return nil, context.Canceled
	}
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		session.mu.Unlock()
		session.close("server stopped")
		return nil, errors.New("livekit: session server is not running")
	}
	if _, exists := m.sessions[room]; exists {
		m.mu.Unlock()
		session.mu.Unlock()
		stopOwned()
		_ = pipe.Stop(context.Background())
		roomConn.Disconnect()
		cancel()
		return nil, fmt.Errorf("livekit: room %q is already active", room)
	}
	handler := m.handler
	m.sessions[room] = session
	m.wg.Add(1)
	m.mu.Unlock()
	session.mu.Unlock()

	go m.runSessionHandler(session, handler)

	m.logger.Info("livekit: session ", room, " ready for user ", userID,
		" exit=", exitNodeID, " aggregated=", aggregated)
	return session, nil
}

func (m *MultiServer) runSessionHandler(session *activeSession, handler app.SessionHandler) {
	defer m.wg.Done()
	session.mu.Lock()
	mux := session.mux
	aggregated := session.aggregated
	aggregatedDone := session.aggregatedDone
	session.mu.Unlock()
	if aggregated {
		select {
		case <-session.ctx.Done():
		case <-aggregatedDone:
		}
		session.close("aggregated session finished")
		return
	}
	if handler == nil || mux == nil {
		<-session.ctx.Done()
		return
	}
	err := handler(session.ctx, mux)
	if err != nil && session.ctx.Err() == nil {
		m.logger.Warn("livekit: session ", session.room, " handler: ", err)
	}
	session.closeKeepRoom("handler finished")
}

func (m *MultiServer) removeSession(room string, session *activeSession) {
	m.mu.Lock()
	if current, ok := m.sessions[room]; ok && current == session {
		delete(m.sessions, room)
	}
	m.mu.Unlock()
}

func (m *MultiServer) deleteRemoteRoom(ctx context.Context, room string) error {
	if m.cfg.SignalURL == "" || m.cfg.APIKey == "" || m.cfg.APISecret == "" {
		return nil
	}
	client := lksdk.NewRoomServiceClient(m.cfg.SignalURL, m.cfg.APIKey, m.cfg.APISecret)
	_, err := client.DeleteRoom(ctx, &livekitpb.DeleteRoomRequest{Room: room})
	return err
}

func shortID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
}

func RoomForUser(prefix, userID string) string {
	prefix = sanitizeRoomPart(prefix)
	if prefix == "" {
		prefix = defaultRoomPrefix
	}
	user := sanitizeRoomPart(strings.ToLower(strings.TrimSpace(userID)))
	if user == "" {
		user = "user"
	}
	return prefix + "-" + user
}

func sanitizeRoomPart(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "-_")
	if len(out) > 48 {
		out = out[:48]
	}
	return out
}

type activeSession struct {
	server     *MultiServer
	room       string
	userID     string
	exitNodeID string
	identity   string

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu             sync.Mutex
	closed         bool
	roomConn       roomSession
	pipe           *Pipe
	mux            *tunnel.TunnelMultiplexer
	aggregated     bool
	release        func()
	aggregatedDone <-chan struct{}
	releaseOnce    sync.Once
	lastSent       int64
	lastRecv       int64
	lastActivity   atomic.Int64
}

func (s *activeSession) touch() {
	s.lastActivity.Store(time.Now().UnixNano())
}

func (s *activeSession) pushIncoming(payload []byte, sender string) {
	s.mu.Lock()
	pipe := s.pipe
	s.mu.Unlock()
	if pipe != nil {
		pipe.PushIncoming(payload, sender)
	}
}

func (s *activeSession) setRemote(identity string) {
	s.mu.Lock()
	pipe := s.pipe
	s.mu.Unlock()
	if pipe != nil {

		pipe.PeerJoined(identity)
	}
}

func (s *activeSession) peerLeft() {
	s.mu.Lock()
	pipe := s.pipe
	s.mu.Unlock()
	if pipe != nil {
		pipe.PeerLeft()
	}
}

func (s *activeSession) close(reason string) {
	s.closeWith(reason, true)
}

func (s *activeSession) closeKeepRoom(reason string) {
	s.closeWith(reason, false)
}

func (s *activeSession) closeWith(reason string, deleteRoom bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	mux := s.mux
	pipe := s.pipe
	roomConn := s.roomConn
	release := s.release
	s.mux = nil
	s.pipe = nil
	s.roomConn = nil
	s.release = nil
	s.mu.Unlock()

	s.server.logger.Info("livekit: closing session ", s.room, " (", reason, ")")
	s.cancel()

	if release != nil {
		s.releaseOnce.Do(release)
	}
	if mux != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), sessionStopTimeout)
		if err := mux.Stop(stopCtx); err != nil {
			s.server.logger.Debug("livekit: stop session mux: ", err)
		}
		stopCancel()
	}
	if pipe != nil {
		if err := pipe.Stop(context.Background()); err != nil {
			s.server.logger.Debug("livekit: stop session pipe: ", err)
		}
	}
	if roomConn != nil {
		roomConn.Disconnect()
	}
	if deleteRoom && s.server.deleteRoom != nil {
		delCtx, delCancel := context.WithTimeout(context.Background(), roomDeleteTimeout)
		if err := s.server.deleteRoom(delCtx, s.room); err != nil {
			s.server.logger.Debug("livekit: delete room ", s.room, ": ", err)
		}
		delCancel()
	}
	s.server.removeSession(s.room, s)
	close(s.done)

	s.server.requestSync()
}

var _ app.SessionServer = (*MultiServer)(nil)
var _ multiplexing.StreamMultiplexer = (*tunnel.TunnelMultiplexer)(nil)
