package relay

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	DefaultMaxMessageSize = 16 << 20
	defaultPingInterval   = 30 * time.Second
	defaultWriteTimeout   = 10 * time.Second

	streamPath    = "/ws/stream"
	minTickDelay  = 300 * time.Millisecond
	maxTickJitter = 300 * time.Millisecond
	telemetryRead = 16 << 10
)

type Server struct {
	password       string
	maxMessageSize int64
	pingInterval   time.Duration
	roomUpgrader   websocket.Upgrader
	decoyUpgrader  websocket.Upgrader

	feed      *Feed
	now       func() time.Time
	tickDelay func() time.Duration

	decoyMu  sync.Mutex
	decoyDir string

	mu    sync.Mutex
	rooms map[string]map[*client]struct{}
}

type client struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func New(password string) *Server {
	s := &Server{
		password:       strings.TrimSpace(password),
		maxMessageSize: DefaultMaxMessageSize,
		pingInterval:   defaultPingInterval,
		feed:           NewFeed(FeedOptions{}),
		now:            time.Now,
		tickDelay:      defaultTickDelay,
		rooms:          make(map[string]map[*client]struct{}),
	}
	base := websocket.Upgrader{
		CheckOrigin:     func(*http.Request) bool { return true },
		ReadBufferSize:  32 << 10,
		WriteBufferSize: 32 << 10,
	}
	s.roomUpgrader = base
	s.roomUpgrader.Subprotocols = []string{SubprotocolTelemetry}

	s.decoyUpgrader = base
	return s
}

func (s *Server) SetFeed(f *Feed) {
	if f == nil {
		return
	}
	s.feed = f
}

func (s *Server) Feed() *Feed { return s.feed }

func defaultTickDelay() time.Duration {
	return minTickDelay + time.Duration(rand.Int64N(int64(maxTickJitter)+1))
}

func (s *Server) SetMaxMessageSize(n int64) {
	if n > 0 {
		s.maxMessageSize = n
	}
}

func (s *Server) authorized(r *http.Request, room string) bool {
	if s.password == "" {
		return true
	}
	if VerifyAuthToken(room, s.password, authTokenFromRequest(r), s.now()) {
		return true
	}
	got := r.Header.Get("X-Room-Password")
	if got == "" {
		got = r.URL.Query().Get("password")
	}
	if got == "" || len(got) != len(s.password) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.password)) == 1
}

func roomFromPath(path string) string {
	const prefix = "/ws/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	room := strings.TrimPrefix(path, prefix)
	if room == "" || strings.Contains(room, "/") {
		return ""
	}
	return room
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == streamPath {
		s.serveStream(w, r)
		return
	}
	if room := roomFromPath(r.URL.Path); room != "" {
		s.serveRoom(w, r, room)
		return
	}
	s.serveDecoy(w, r)
}

func (s *Server) serveRoom(w http.ResponseWriter, r *http.Request, room string) {
	if !s.authorized(r, room) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := s.roomUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.serveTunnel(room, conn)
}

func (s *Server) serveStream(w http.ResponseWriter, r *http.Request) {
	room := strings.TrimSpace(r.URL.Query().Get("room"))
	if room != "" && !strings.Contains(room, "/") && s.authorized(r, room) {
		conn, err := s.roomUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		s.serveTunnel(room, conn)
		return
	}
	s.serveTelemetry(w, r)
}

func (s *Server) serveTunnel(room string, conn *websocket.Conn) {
	conn.SetReadLimit(s.maxMessageSize)

	c := &client{conn: conn}
	s.add(room, c)
	defer func() {
		s.remove(room, c)
		_ = conn.Close()
	}()

	stopPing := make(chan struct{})
	defer close(stopPing)
	go s.pingLoop(c, stopPing)

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if msgType != websocket.BinaryMessage && msgType != websocket.TextMessage {
			continue
		}
		s.broadcast(room, c, msgType, data)
	}
}

func (s *Server) serveTelemetry(w http.ResponseWriter, r *http.Request) {
	header := http.Header{"Sec-Websocket-Protocol": []string{SubprotocolTelemetry}}
	conn, err := s.decoyUpgrader.Upgrade(w, r, header)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	conn.SetReadLimit(telemetryRead)
	c := &client{conn: conn}

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	stopPing := make(chan struct{})
	defer close(stopPing)
	go s.pingLoop(c, stopPing)

	ctx := r.Context()
	for {
		timer := time.NewTimer(s.tickDelay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-closed:
			timer.Stop()
			return
		case <-timer.C:
		}

		data, err := json.Marshal(s.feed.Tick())
		if err != nil {
			return
		}
		c.mu.Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
		err = conn.WriteMessage(websocket.TextMessage, data)
		c.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (s *Server) pingLoop(c *client, stop <-chan struct{}) {
	ticker := time.NewTicker(s.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			c.mu.Lock()
			_ = c.conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
			err := c.conn.WriteMessage(websocket.PingMessage, nil)
			c.mu.Unlock()
			if err != nil {
				_ = c.conn.Close()
				return
			}
		}
	}
}

func (s *Server) add(room string, c *client) {
	s.mu.Lock()
	if s.rooms[room] == nil {
		s.rooms[room] = make(map[*client]struct{})
	}
	s.rooms[room][c] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) remove(room string, c *client) {
	s.mu.Lock()
	if conns := s.rooms[room]; conns != nil {
		delete(conns, c)
		if len(conns) == 0 {
			delete(s.rooms, room)
		}
	}
	s.mu.Unlock()
}

func (s *Server) broadcast(room string, from *client, msgType int, data []byte) {
	s.mu.Lock()
	targets := make([]*client, 0, len(s.rooms[room]))
	for c := range s.rooms[room] {
		if c != from {
			targets = append(targets, c)
		}
	}
	s.mu.Unlock()

	for _, c := range targets {
		c.mu.Lock()
		_ = c.conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
		err := c.conn.WriteMessage(msgType, data)
		c.mu.Unlock()
		if err != nil {
			_ = c.conn.Close()
		}
	}
}

func (s *Server) RoomSize(room string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rooms[room])
}

func (s *Server) CloseAll() {
	s.mu.Lock()
	var all []*client
	for _, conns := range s.rooms {
		for c := range conns {
			all = append(all, c)
		}
	}
	s.rooms = make(map[string]map[*client]struct{})
	s.mu.Unlock()
	for _, c := range all {
		_ = c.conn.Close()
	}
}

func (s *Server) Run(ctx context.Context, addr string) error {
	if s.feed != nil {
		s.feed.Start(ctx)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.CloseAll()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
