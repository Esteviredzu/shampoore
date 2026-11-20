package raw_ws

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
)

type relayConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

type relayRoom struct {
	conns map[*relayConn]struct{}
}

type testRelay struct {
	mu     sync.Mutex
	rooms  map[string]*relayRoom
	accept chan string
}

func newTestRelay() *testRelay {
	return &testRelay{rooms: make(map[string]*relayRoom), accept: make(chan string, 64)}
}

func testRoomFromRequest(req *http.Request) string {
	if req.URL.Path == "/ws/stream" {
		return strings.TrimSpace(req.URL.Query().Get("room"))
	}
	room := strings.TrimPrefix(req.URL.Path, "/ws/")
	if room == "" || room == req.URL.Path {
		return ""
	}
	return room
}

func (r *testRelay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	room := testRoomFromRequest(req)
	if room == "" {
		http.NotFound(w, req)
		return
	}
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := up.Upgrade(w, req, nil)
	if err != nil {
		return
	}
	rc := &relayConn{conn: conn}

	r.mu.Lock()
	state := r.rooms[room]
	if state == nil {
		state = &relayRoom{conns: make(map[*relayConn]struct{})}
		r.rooms[room] = state
	}
	state.conns[rc] = struct{}{}
	r.mu.Unlock()

	select {
	case r.accept <- room:
	default:
	}

	defer func() {
		r.mu.Lock()
		if state := r.rooms[room]; state != nil {
			delete(state.conns, rc)
		}
		r.mu.Unlock()
		_ = conn.Close()
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		r.broadcast(room, rc, data)
	}
}

func (r *testRelay) broadcast(room string, from *relayConn, data []byte) {
	r.mu.Lock()
	state := r.rooms[room]
	var targets []*relayConn
	if state != nil {
		for conn := range state.conns {
			if conn != from {
				targets = append(targets, conn)
			}
		}
	}
	r.mu.Unlock()

	for _, target := range targets {
		target.mu.Lock()
		_ = target.conn.WriteMessage(websocket.BinaryMessage, data)
		target.mu.Unlock()
	}
}

func (r *testRelay) roomSize(room string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.rooms[room]
	if state == nil {
		return 0
	}
	return len(state.conns)
}

func waitUntil(t *testing.T, timeout time.Duration, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func testUserRegistry(t *testing.T, ids ...string) *auth.UserRegistry {
	t.Helper()
	registry := auth.NewRegistry()
	for _, id := range ids {
		if err := registry.Upsert(auth.UserSpec{UserID: id, Enabled: true}); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}
	return registry
}

func echoHandler(ctx context.Context, mux multiplexing.StreamMultiplexer) error {
	for {
		stream, err := mux.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go func(stream multiplexing.Stream) {
			defer stream.Close()
			_, _ = io.Copy(stream, stream)
		}(stream)
	}
}

func multiuserClient(t *testing.T, relayURL, baseRoom, userID string) *tunnel.TunnelMultiplexer {
	t.Helper()
	cfg := Config{
		ServerURL:       relayURL,
		Room:            baseRoom,
		Password:        "secret",
		AllowInsecureWS: true,
		UserID:          userID,
	}
	tr := newTransport(cfg, testLogger{})
	tr.reconnectDelay = 20 * time.Millisecond
	muxCfg := tunnel.Config{
		FlushThreshold: 12000,
		FlushInterval:  5 * time.Millisecond,
		UserID:         userID,
		ExitNodeID:     "exit-1",
	}
	return tunnel.New(tr, testLogger{}, muxCfg)
}

func TestMultiServerServesTwoUsersOnOneRelay(t *testing.T) {
	relay := newTestRelay()
	srv := httptest.NewServer(relay)
	defer srv.Close()

	registry := testUserRegistry(t, "alice", "bob")
	cfg := Config{
		ServerURL:       srv.URL,
		Room:            "base",
		Password:        "secret",
		AllowInsecureWS: true,
		Multiuser:       true,
	}
	server := newMultiServer(cfg, testLogger{}, registry, app.Values{
		"mux_flush_ms":        5,
		"mux_flush_threshold": 12000,
	})
	server.SetSessionHandler(echoHandler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := server.Stop(stopCtx); err != nil {
			t.Errorf("server stop: %v", err)
		}
	}()

	waitUntil(t, 5*time.Second, "alice server room", func() bool {
		return relay.roomSize("base-alice") >= 1
	})
	waitUntil(t, 5*time.Second, "bob server room", func() bool {
		return relay.roomSize("base-bob") >= 1
	})

	alice := multiuserClient(t, srv.URL, "base", "alice")
	bob := multiuserClient(t, srv.URL, "base", "bob")
	if err := alice.Start(ctx); err != nil {
		t.Fatalf("alice start: %v", err)
	}
	if err := bob.Start(ctx); err != nil {
		t.Fatalf("bob start: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = alice.Stop(stopCtx)
		_ = bob.Stop(stopCtx)
	}()

	waitUntil(t, 5*time.Second, "both users connected", func() bool {
		return relay.roomSize("base-alice") >= 2 && relay.roomSize("base-bob") >= 2
	})

	type result struct {
		name string
		data []byte
		err  error
	}
	results := make(chan result, 2)
	for _, tc := range []struct {
		name string
		mux  *tunnel.TunnelMultiplexer
		want []byte
	}{
		{name: "alice", mux: alice, want: []byte("payload-from-alice")},
		{name: "bob", mux: bob, want: []byte("payload-from-bob!!")},
	} {
		tc := tc
		go func() {
			stream, err := tc.mux.OpenStream(ctx, map[string]any{"host": "echo", "port": float64(80)})
			if err != nil {
				results <- result{name: tc.name, err: err}
				return
			}
			defer stream.Close()
			if _, err := stream.Write(tc.want); err != nil {
				results <- result{name: tc.name, err: err}
				return
			}
			buf := make([]byte, len(tc.want))
			if _, err := io.ReadFull(stream, buf); err != nil {
				results <- result{name: tc.name, err: err}
				return
			}
			results <- result{name: tc.name, data: buf}
		}()
	}

	got := make(map[string][]byte)
	for i := 0; i < 2; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("%s round trip: %v", res.name, res.err)
			}
			got[res.name] = res.data
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for user round trips")
		}
	}
	if string(got["alice"]) != "payload-from-alice" {
		t.Fatalf("unexpected alice payload: %q", got["alice"])
	}
	if string(got["bob"]) != "payload-from-bob!!" {
		t.Fatalf("unexpected bob payload: %q", got["bob"])
	}

	for _, id := range []string{"alice", "bob"} {
		info, ok := registry.Get(id)
		if !ok {
			t.Fatalf("user %s missing", id)
		}
		if info.Usage.UploadBytes == 0 || info.Usage.DownloadBytes == 0 {
			t.Fatalf("traffic was not accounted for %s: %+v", id, info.Usage)
		}
	}

	registry.Remove("alice")
	waitUntil(t, 5*time.Second, "alice session stop", func() bool {
		return relay.roomSize("base-alice") == 1
	})

	if err := registry.Upsert(auth.UserSpec{UserID: "bob", Enabled: false}); err != nil {
		t.Fatalf("disable bob: %v", err)
	}
	waitUntil(t, 5*time.Second, "bob session stop after disabling", func() bool {
		return relay.roomSize("base-bob") == 1
	})
}

func TestRawWSConfigUserRoomJSON(t *testing.T) {
	var cfg Config
	if err := json.Unmarshal([]byte(`{"server_url":"wss://example.com","room":"base","password":"pw","user_id":"alice"}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.UserID != "alice" {
		t.Fatalf("user id not decoded: %+v", cfg)
	}
	if roomForUser(cfg.Room, cfg.UserID) != "base-alice" {
		t.Fatalf("unexpected room: %q", roomForUser(cfg.Room, cfg.UserID))
	}
}
