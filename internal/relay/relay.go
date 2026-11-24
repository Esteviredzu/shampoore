package relay

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Esteviredzu/shampoore/internal/wsshape"
)

const (
	DefaultMaxMessageSize = 16 << 20
	defaultPingInterval   = 30 * time.Second
	defaultPongWait       = 45 * time.Second
	defaultWriteTimeout   = 10 * time.Second

	streamPath    = "/ws/stream"
	minTickDelay  = 300 * time.Millisecond
	maxTickJitter = 300 * time.Millisecond
	telemetryRead = 16 << 10

	decoyRateWait = 250 * time.Millisecond

	clientQueueCap = 256
)

type Server struct {
	password       string
	maxMessageSize int64
	pingInterval   time.Duration
	pongWait       time.Duration
	roomUpgrader   websocket.Upgrader
	decoyUpgrader  websocket.Upgrader

	feed      *Feed
	now       func() time.Time
	tickDelay func() time.Duration
	shape     shapeState

	decoyMu  sync.Mutex
	decoyDir string

	mu    sync.Mutex
	rooms map[string]map[*client]struct{}
}

type clientMessage struct {
	typ  int
	data []byte
}

type client struct {
	conn *websocket.Conn
	send chan clientMessage
	done chan struct{}

	closeOnce sync.Once
}

func newClient(conn *websocket.Conn) *client {
	return &client{
		conn: conn,
		send: make(chan clientMessage, clientQueueCap),
		done: make(chan struct{}),
	}
}

func (c *client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

func (c *client) enqueue(msg clientMessage) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.send <- msg:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

func New(password string) *Server {
	s := &Server{
		password:       strings.TrimSpace(password),
		maxMessageSize: DefaultMaxMessageSize,
		feed:           NewFeed(FeedOptions{}),
		now:            time.Now,
		rooms:          make(map[string]map[*client]struct{}),
	}
	s.shape.config = wsshape.Default()
	s.shape.source = "default"
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

	wait := s.pongWaitDuration()
	if wait <= 0 {
		wait = defaultPongWait
	}
	_ = conn.SetReadDeadline(time.Now().Add(wait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wait))
	})

	c := newClient(conn)
	s.add(room, c)
	defer func() {
		s.remove(room, c)
		c.close()
	}()

	go s.writeLoop(c)

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

	shape := s.Shape()
	readLimit := int64(shape.Frame.MaxMessageBytes)
	if readLimit <= 0 {
		readLimit = telemetryRead
	}
	conn.SetReadLimit(readLimit)
	c := newClient(conn)
	go s.writeLoop(c)
	defer func() { c.close() }()

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

	limiter := decoyLimiter(shape.Rate)
	ctx := r.Context()
	for {
		timer := time.NewTimer(s.nextTickDelay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-closed:
			timer.Stop()
			return
		case <-timer.C:
		}

		shape = s.Shape()
		payload, err := json.Marshal(s.feed.Tick())
		if err != nil {
			return
		}
		frame := wsshape.PadPayload(payload, shape.Padding)
		if maxFrame := shape.Frame.MaxFrameBytes; maxFrame > 0 && len(frame) > maxFrame {

			if len(payload) > maxFrame {
				continue
			}
			frame = payload
		}
		if limiter != nil {
			waitCtx, cancel := context.WithTimeout(ctx, decoyRateWait)
			err := limiter.WaitN(waitCtx, len(frame))
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				continue
			}
		}
		if !c.enqueue(clientMessage{typ: websocket.TextMessage, data: frame}) {
			return
		}
	}
}

func (s *Server) writeLoop(c *client) {
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
			if err := c.conn.WriteMessage(msg.typ, msg.data); err != nil {
				c.close()
				return
			}
		}
	}
}

func (s *Server) pingLoop(c *client, stop <-chan struct{}) {
	ticker := time.NewTicker(s.pingEvery())
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-c.done:
			return
		case <-ticker.C:
			if !c.enqueue(clientMessage{typ: websocket.PingMessage}) {
				c.close()
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

	msg := clientMessage{typ: msgType, data: data}
	for _, c := range targets {

		if !c.enqueue(msg) {
			c.close()
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
		c.close()
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
