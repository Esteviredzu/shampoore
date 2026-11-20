package raw_ws

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Esteviredzu/shampoore/internal/relay"
)

func TestWarmupFetchesDecoyAssets(t *testing.T) {
	type hit struct {
		path string
		ua   string
	}
	var mu sync.Mutex
	var hits []hit

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits = append(hits, hit{path: r.URL.Path, ua: r.Header.Get("User-Agent")})
		mu.Unlock()
		_, _ = io.WriteString(w, "<html>ok</html>")
	}))
	defer srv.Close()

	tr := newTestTransport(srv.URL, false)
	tr.warmup(context.Background(), srv.URL+"/ws/stream?room=room-1")

	mu.Lock()
	got := append([]hit(nil), hits...)
	mu.Unlock()

	if len(got) < 2 || len(got) > 3 {
		t.Fatalf("warmup requested %d assets, want 2..3: %+v", len(got), got)
	}
	for i, h := range got {
		if h.path != warmupAssetPaths[i] {
			t.Fatalf("asset %d = %q, want %q", i, h.path, warmupAssetPaths[i])
		}
		if !strings.Contains(h.ua, "Chrome/") {
			t.Fatalf("asset %d User-Agent = %q", i, h.ua)
		}
	}
}

func TestWarmupStopsOnCancelledContext(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()

	tr := newTestTransport(srv.URL, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tr.warmup(ctx, srv.URL+"/ws/stream?room=room-1")

	mu.Lock()
	defer mu.Unlock()
	if requests != 0 {
		t.Fatalf("warmup issued %d requests with a cancelled context", requests)
	}
}

func TestMaskingDelayBounds(t *testing.T) {
	tr := newTestTransport("ws://example.test", false)
	for i := 0; i < 200; i++ {
		if d := tr.nextWarmupPause(); d < defaultWarmupMinDelay || d > defaultWarmupMaxDelay {
			t.Fatalf("warmup pause %s outside 60..250ms", d)
		}
		if d := tr.nextRotationDelay(); d < defaultRotationMin || d > defaultRotationMax {
			t.Fatalf("rotation delay %s outside 15..35m", d)
		}
		if d := tr.nextGrace(); d < defaultRotateGraceMin || d > defaultRotateGraceMax {
			t.Fatalf("grace %s outside 2..3s", d)
		}
	}
}

func TestDialSendsHMACSubprotocol(t *testing.T) {
	protocols := make(chan []string, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !websocket.IsWebSocketUpgrade(r) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		select {
		case protocols <- websocket.Subprotocols(r):
		default:
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	tr := newTestTransport(srv.URL, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess, err := tr.dialSession(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer sess.close()

	var got []string
	select {
	case got = <-protocols:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not observe the upgrade")
	}
	if len(got) != 2 || got[0] != relay.SubprotocolTelemetry {
		t.Fatalf("protocols = %v", got)
	}
	if !relay.VerifyAuthToken("room-1", "secret", got[1], time.Now()) {
		t.Fatalf("HMAC token %q does not verify", got[1])
	}
}

func TestConnectionRotationIsMakeBeforeBreak(t *testing.T) {
	var mu sync.Mutex
	accepted, open, maxOpen := 0, 0, 0
	messages := make(chan string, 32)

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !websocket.IsWebSocketUpgrade(r) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		accepted++
		open++
		if open > maxOpen {
			maxOpen = open
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			open--
			mu.Unlock()
			_ = conn.Close()
		}()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			select {
			case messages <- string(data):
			default:
			}
		}
	}))
	defer srv.Close()

	tr := newTestTransport(srv.URL, false)
	tr.rotationMin = 120 * time.Millisecond
	tr.rotationMax = 150 * time.Millisecond
	tr.graceMin = 80 * time.Millisecond
	tr.graceMax = 100 * time.Millisecond

	cancel := startTransport(t, tr)
	defer cancel()
	defer stopWithTimeout(t, tr)

	waitUntil(t, 8*time.Second, "first rotation", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return accepted >= 2
	})
	waitUntil(t, 3*time.Second, "two concurrently open sessions", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return maxOpen >= 2
	})

	if err := tr.Send(context.Background(), []byte("after-rotate")); err != nil {
		t.Fatalf("send after rotation: %v", err)
	}
	select {
	case msg := <-messages:
		if msg != "after-rotate" {
			t.Fatalf("server received %q", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not receive the frame after rotation")
	}

	waitUntil(t, 3*time.Second, "old connection retired", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return open < accepted
	})
	if err := tr.Send(context.Background(), []byte("still-alive")); err != nil {
		t.Fatalf("transport stopped after rotation: %v", err)
	}
}

func TestBuildStreamURL(t *testing.T) {
	cases := []struct{ base, room, want string }{
		{"wss://example.test", "room-1", "wss://example.test/ws/stream?room=room-1"},
		{"https://example.test", "room-1", "wss://example.test/ws/stream?room=room-1"},
		{"ws://example.test", "room 1", "ws://example.test/ws/stream?room=room+1"},
		{"wss://example.test/ws", "room-1", "wss://example.test/ws/stream?room=room-1"},
		{"wss://example.test/prefix", "room-1", "wss://example.test/prefix/ws/stream?room=room-1"},
	}
	for _, c := range cases {
		got, err := buildStreamURL(c.base, c.room)
		if err != nil {
			t.Fatalf("buildStreamURL(%q): %v", c.base, err)
		}
		if got != c.want {
			t.Fatalf("buildStreamURL(%q, %q) = %q, want %q", c.base, c.room, got, c.want)
		}
	}
}

func TestHTTPBaseURL(t *testing.T) {
	cases := map[string]string{
		"wss://example.test/ws/stream?room=x":   "https://example.test",
		"ws://example.test:8787/ws/room-1":      "http://example.test:8787",
		"https://example.test/prefix/ws/room-1": "https://example.test/prefix",
	}
	for in, want := range cases {
		got, err := httpBaseURL(in)
		if err != nil {
			t.Fatalf("httpBaseURL(%q): %v", in, err)
		}
		if got != want {
			t.Fatalf("httpBaseURL(%q) = %q, want %q", in, got, want)
		}
	}
}
