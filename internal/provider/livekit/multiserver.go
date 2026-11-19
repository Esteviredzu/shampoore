package livekit

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
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
	sessionRequestBodyLimit = 64 << 10
	roomDeleteTimeout       = 2 * time.Second
	sessionStopTimeout      = 2 * time.Second
)

type sessionRequest struct {
	UserID     string `json:"user_id"`
	ExitNodeID string `json:"exit_node_id"`
}

type sessionResponse struct {
	URL   string `json:"url"`
	Token string `json:"token"`
	Room  string `json:"room"`
}

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

// BuildUserRegistry builds a registry from the users block of a transport
// configuration.
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

// MultiServer is an on-demand LiveKit session server. It exposes a small HTTP
// endpoint that authorizes a user and returns a fresh room + client token.
type MultiServer struct {
	cfg      Config
	logger   logging.ContextLogger
	registry *auth.UserRegistry
	muxCfg   tunnel.Config
	password string

	listenAddr  string
	sessionPath string
	idleTimeout time.Duration
	clientTTL   time.Duration
	serverTTL   time.Duration

	connectRoom roomDialFunc
	deleteRoom  func(ctx context.Context, room string) error

	mu       sync.Mutex
	handler  app.SessionHandler
	sessions map[string]*activeSession
	ctx      context.Context
	cancel   context.CancelFunc
	started  bool
	httpSrv  *http.Server
	ln       net.Listener
	addr     string
	wg       sync.WaitGroup
}

// NewMultiServer creates an on-demand LiveKit session server.
func NewMultiServer(
	cfg Config,
	logger logging.ContextLogger,
	registry *auth.UserRegistry,
	muxCfg tunnel.Config,
	password string,
) *MultiServer {
	cfg.applyDefaults()
	muxCfg.Registry = registry
	listenAddr, sessionPath, err := parseTokenListen(cfg.TokenListen)
	if err != nil {
		// The builder validates the config first; keep a safe fallback so a
		// directly constructed server still fails cleanly in Start.
		listenAddr = ""
		sessionPath = "/session"
	}
	m := &MultiServer{
		cfg:         cfg,
		logger:      logger,
		registry:    registry,
		muxCfg:      muxCfg,
		password:    password,
		listenAddr:  listenAddr,
		sessionPath: sessionPath,
		idleTimeout: cfg.idleTimeout(),
		clientTTL:   cfg.clientTokenTTL(),
		serverTTL:   cfg.serverTokenTTL(),
		sessions:    make(map[string]*activeSession),
	}
	m.connectRoom = connectLiveKitRoom
	m.deleteRoom = m.deleteRemoteRoom
	if strings.TrimSpace(cfg.TokenSecret) == "" {
		logger.Warn("livekit: multiuser session endpoint has no token_secret; it is unprotected")
	}
	return m
}

// Addr returns the bound HTTP listener address (after Start).
func (m *MultiServer) Addr() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.addr
}

// ActiveSessions returns the number of currently active on-demand rooms.
func (m *MultiServer) ActiveSessions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// SessionPath returns the HTTP path of the session endpoint.
func (m *MultiServer) SessionPath() string {
	return m.sessionPath
}

func (m *MultiServer) SetSessionHandler(handler app.SessionHandler) {
	m.mu.Lock()
	m.handler = handler
	m.mu.Unlock()
}

// Handler returns the HTTP handler for the session endpoint. It is useful for
// embedding the server behind another listener (and for tests).
func (m *MultiServer) Handler() http.Handler {
	mux := http.NewServeMux()
	path := m.sessionPath
	if path == "" {
		path = "/session"
	}
	mux.HandleFunc(path, m.handleSession)
	return mux
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
	if m.listenAddr == "" {
		m.mu.Unlock()
		return errors.New("livekit: token_listen is required")
	}
	serveCtx, cancel := context.WithCancel(ctx)
	ln, err := net.Listen("tcp", m.listenAddr)
	if err != nil {
		cancel()
		m.mu.Unlock()
		return fmt.Errorf("livekit: listen %s: %w", m.listenAddr, err)
	}
	srv := &http.Server{
		Handler:           m.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	m.ctx = serveCtx
	m.cancel = cancel
	m.started = true
	m.httpSrv = srv
	m.ln = ln
	m.addr = ln.Addr().String()
	m.mu.Unlock()

	m.logger.Info("livekit: on-demand session endpoint listening on http://", m.addr, m.sessionPath)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			m.logger.Warn("livekit: session endpoint stopped: ", err)
		}
	}()
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
	srv := m.httpSrv
	sessions := make([]*activeSession, 0, len(m.sessions))
	for room, session := range m.sessions {
		sessions = append(sessions, session)
		delete(m.sessions, room)
	}
	m.cancel = nil
	m.httpSrv = nil
	m.ln = nil
	m.addr = ""
	m.mu.Unlock()

	if srv != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), sessionStopTimeout)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
		}
		shutdownCancel()
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

func (m *MultiServer) handleSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !m.authorizedRequest(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	body := http.MaxBytesReader(w, r.Body, sessionRequestBodyLimit)
	defer body.Close()
	var req sessionRequest
	dec := json.NewDecoder(body)
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	userID, err := auth.NormalizeUserID(req.UserID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	exitNodeID := strings.TrimSpace(req.ExitNodeID)
	if m.registry != nil {
		if err := m.registry.Authorize(userID, exitNodeID); err != nil {
			m.logger.Warn("livekit: session denied for ", userID, ": ", err)
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
	}

	m.mu.Lock()
	started := m.started
	parent := m.ctx
	m.mu.Unlock()
	if !started || parent == nil {
		http.Error(w, "session server is not running", http.StatusServiceUnavailable)
		return
	}

	resp, err := m.createSession(parent, userID, exitNodeID)
	if err != nil {
		m.logger.Warn("livekit: create session for ", userID, ": ", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *MultiServer) authorizedRequest(r *http.Request) bool {
	expected := strings.TrimSpace(m.cfg.TokenSecret)
	if expected == "" {
		return true
	}
	got := strings.TrimSpace(r.Header.Get("X-Shampoore-Secret"))
	if got == "" {
		authz := strings.TrimSpace(r.Header.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(authz), "bearer ") {
			got = strings.TrimSpace(authz[len("bearer "):])
		}
	}
	if got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

func (m *MultiServer) createSession(parent context.Context, userID, exitNodeID string) (sessionResponse, error) {
	room, err := m.newRoomName(userID)
	if err != nil {
		return sessionResponse{}, err
	}
	serverIdentity := "server-" + shortID()
	clientToken, err := makeTokenFor(m.cfg.APIKey, m.cfg.APISecret, room, userID, m.clientTTL)
	if err != nil {
		return sessionResponse{}, fmt.Errorf("livekit: client token: %w", err)
	}
	serverToken, err := makeTokenFor(m.cfg.APIKey, m.cfg.APISecret, room, serverIdentity, m.serverTTL)
	if err != nil {
		return sessionResponse{}, fmt.Errorf("livekit: server token: %w", err)
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
			session.mu.Lock()
			roomConn := session.roomConn
			session.mu.Unlock()
			if roomConn != nil && roomConn.RemoteParticipantCount() > 0 {
				session.touch()
				return
			}
			session.close("client left")
		},
		OnDisconnected: func() {
			session.close("server room disconnected")
		},
	}

	roomConn, err := m.connectRoom(sessionCtx, m.cfg.SignalURL, serverToken, events)
	if err != nil {
		cancel()
		return sessionResponse{}, fmt.Errorf("livekit: connect server participant: %w", err)
	}
	if sessionCtx.Err() != nil {
		roomConn.Disconnect()
		cancel()
		return sessionResponse{}, context.Canceled
	}

	pipe := newPipe(roomConn.LocalParticipant(), m.cfg.DataTopic, userID)
	secured, err := security.WrapPipeWithRole(pipe, m.password, "server", m.logger)
	if err != nil {
		_ = pipe.Stop(context.Background())
		roomConn.Disconnect()
		cancel()
		return sessionResponse{}, fmt.Errorf("livekit: wrap encryption: %w", err)
	}
	mux := tunnel.New(NewTransport(secured), m.logger, m.muxCfg)

	session.mu.Lock()
	session.roomConn = roomConn
	session.pipe = pipe
	session.mux = mux
	session.lastSent = pipe.BytesSent()
	session.lastRecv = pipe.BytesRecv()
	closed := session.closed
	session.mu.Unlock()
	if closed {
		_ = mux.Stop(context.Background())
		_ = pipe.Stop(context.Background())
		roomConn.Disconnect()
		return sessionResponse{}, context.Canceled
	}

	if err := mux.Start(sessionCtx); err != nil {
		session.close("mux start failed")
		return sessionResponse{}, fmt.Errorf("livekit: start mux: %w", err)
	}

	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		_ = mux.Stop(context.Background())
		_ = pipe.Stop(context.Background())
		roomConn.Disconnect()
		return sessionResponse{}, context.Canceled
	}
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		session.mu.Unlock()
		session.close("server stopped")
		return sessionResponse{}, errors.New("livekit: session server is not running")
	}
	handler := m.handler
	m.sessions[room] = session
	m.wg.Add(2)
	m.mu.Unlock()
	session.mu.Unlock()

	go m.runSessionHandler(session, handler)
	go m.monitorSession(session)

	m.logger.Info("livekit: session ", room, " ready for user ", userID,
		" exit=", exitNodeID)
	return sessionResponse{URL: m.cfg.SignalURL, Token: clientToken, Room: room}, nil
}

func (m *MultiServer) runSessionHandler(session *activeSession, handler app.SessionHandler) {
	defer m.wg.Done()
	session.mu.Lock()
	mux := session.mux
	session.mu.Unlock()
	if handler == nil || mux == nil {
		<-session.ctx.Done()
		return
	}
	err := handler(session.ctx, mux)
	if err != nil && session.ctx.Err() == nil {
		m.logger.Warn("livekit: session ", session.room, " handler: ", err)
	}
	session.close("handler finished")
}

func (m *MultiServer) monitorSession(session *activeSession) {
	defer m.wg.Done()
	interval := m.idleTimeout / 4
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	session.mu.Lock()
	lastSent := session.lastSent
	lastRecv := session.lastRecv
	session.mu.Unlock()

	for {
		select {
		case <-session.ctx.Done():
			return
		case <-ticker.C:
			session.mu.Lock()
			pipe := session.pipe
			session.mu.Unlock()
			if pipe != nil {
				sent := pipe.BytesSent()
				recv := pipe.BytesRecv()
				if sent != lastSent || recv != lastRecv {
					lastSent, lastRecv = sent, recv
					session.touch()
				}
			}
			last := session.lastActivity.Load()
			if time.Since(time.Unix(0, last)) >= m.idleTimeout {
				m.logger.Info("livekit: session ", session.room, " idle timeout")
				session.close("idle timeout")
				return
			}
		}
	}
}

func (m *MultiServer) newRoomName(userID string) (string, error) {
	prefix := sanitizeRoomPart(m.cfg.RoomPrefix)
	if prefix == "" {
		prefix = defaultRoomPrefix
	}
	user := sanitizeRoomPart(userID)
	if user == "" {
		user = "user"
	}
	for i := 0; i < 8; i++ {
		room := prefix + "-" + user + "-" + shortID()
		m.mu.Lock()
		_, exists := m.sessions[room]
		m.mu.Unlock()
		if !exists {
			return room, nil
		}
	}
	return "", errors.New("livekit: could not allocate a unique room name")
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

	mu           sync.Mutex
	closed       bool
	roomConn     roomSession
	pipe         *Pipe
	mux          *tunnel.TunnelMultiplexer
	lastSent     int64
	lastRecv     int64
	lastActivity atomic.Int64
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
		pipe.SetRemoteID(identity)
	}
}

func (s *activeSession) close(reason string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	mux := s.mux
	pipe := s.pipe
	roomConn := s.roomConn
	s.mux = nil
	s.pipe = nil
	s.roomConn = nil
	s.mu.Unlock()

	s.server.logger.Info("livekit: closing session ", s.room, " (", reason, ")")
	s.cancel()

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
	if s.server.deleteRoom != nil {
		delCtx, delCancel := context.WithTimeout(context.Background(), roomDeleteTimeout)
		if err := s.server.deleteRoom(delCtx, s.room); err != nil {
			s.server.logger.Debug("livekit: delete room ", s.room, ": ", err)
		}
		delCancel()
	}
	s.server.removeSession(s.room, s)
	close(s.done)
}

var _ app.SessionServer = (*MultiServer)(nil)
var _ multiplexing.StreamMultiplexer = (*tunnel.TunnelMultiplexer)(nil)
