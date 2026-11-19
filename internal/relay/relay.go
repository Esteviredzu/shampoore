package relay

import (
	"context"
	"crypto/subtle"
	"errors"
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
)

// Server is a lightweight password protected WebSocket room relay.
type Server struct {
	password       string
	maxMessageSize int64
	pingInterval   time.Duration
	upgrader       websocket.Upgrader

	mu    sync.Mutex
	rooms map[string]map[*client]struct{}
}

type client struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

// New creates a relay server. An empty password disables authentication.
func New(password string) *Server {
	return &Server{
		password:       strings.TrimSpace(password),
		maxMessageSize: DefaultMaxMessageSize,
		pingInterval:   defaultPingInterval,
		upgrader: websocket.Upgrader{
			CheckOrigin:     func(*http.Request) bool { return true },
			ReadBufferSize:  32 << 10,
			WriteBufferSize: 32 << 10,
		},
		rooms: make(map[string]map[*client]struct{}),
	}
}

// SetMaxMessageSize changes the per-client read limit.
func (s *Server) SetMaxMessageSize(n int64) {
	if n > 0 {
		s.maxMessageSize = n
	}
}

func (s *Server) authorized(r *http.Request) bool {
	if s.password == "" {
		return true
	}
	got := r.Header.Get("X-Room-Password")
	if got == "" {
		got = r.URL.Query().Get("password")
	}
	if len(got) != len(s.password) {
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
	roomName := roomFromPath(r.URL.Path)
	if roomName == "" {
		http.NotFound(w, r)
		return
	}
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(s.maxMessageSize)

	c := &client{conn: conn}
	s.add(roomName, c)
	defer func() {
		s.remove(roomName, c)
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
		s.broadcast(roomName, c, msgType, data)
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

// RoomSize reports how many clients are currently in a room (for tests/metrics).
func (s *Server) RoomSize(room string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rooms[room])
}

// CloseAll closes every connected client.
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

// Run starts an HTTP server until ctx is cancelled.
func (s *Server) Run(ctx context.Context, addr string) error {
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
