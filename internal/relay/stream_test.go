package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func streamURL(srv *httptest.Server, room string) string {
	base := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/stream"
	if room != "" {
		return base + "?room=" + room
	}
	return base
}

func dialWithProtocols(t *testing.T, url string, protocols []string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	d := websocket.Dialer{
		Subprotocols:     protocols,
		HandshakeTimeout: 5 * time.Second,
	}
	conn, resp, err := d.Dial(url, nil)
	if err == nil {
		t.Cleanup(func() { _ = conn.Close() })
	}
	return conn, resp, err
}

func TestStreamHMACJoinsTunnelRoom(t *testing.T) {
	srv := httptest.NewServer(New("secret"))
	defer srv.Close()

	room := "room-1"
	protocols := AuthProtocols(room, "secret", time.Now())
	a, resp, err := dialWithProtocols(t, streamURL(srv, room), protocols)
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != SubprotocolTelemetry {
		t.Fatalf("subprotocol = %q", got)
	}
	b, resp, err := dialWithProtocols(t, streamURL(srv, room), protocols)
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != SubprotocolTelemetry {
		t.Fatalf("subprotocol = %q", got)
	}

	if err := a.WriteMessage(websocket.BinaryMessage, []byte("tunnel-frame")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.SetReadDeadline(time.Now().Add(3 * time.Second))
	mt, data, err := b.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if mt != websocket.BinaryMessage || string(data) != "tunnel-frame" {
		t.Fatalf("message = %d %q", mt, data)
	}
}

func TestStreamLegacyPathAcceptsHMACToken(t *testing.T) {
	srv := httptest.NewServer(New("secret"))
	defer srv.Close()

	room := "legacy-room"
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/" + room
	conn, resp, err := dialWithProtocols(t, url, AuthProtocols(room, "secret", time.Now()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != SubprotocolTelemetry {
		t.Fatalf("subprotocol = %q", got)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestStreamLegacyPathRejectsBadPassword(t *testing.T) {
	srv := httptest.NewServer(New("secret"))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/room-1"
	if _, resp, err := dialWithProtocols(t, url, nil); err == nil {
		t.Fatal("unauthenticated legacy dial succeeded")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %v", resp)
	}
}

func TestStreamDecoyTelemetryForStrangers(t *testing.T) {
	s := New("secret")
	feed := NewFeed(FeedOptions{
		Offline: true,
		Now:     func() time.Time { return fixedNow() },
	})
	feed.Refresh(context.Background())
	s.SetFeed(feed)
	s.tickDelay = func() time.Duration { return 5 * time.Millisecond }

	srv := httptest.NewServer(s)
	defer srv.Close()

	conn, resp, err := dialWithProtocols(t, streamURL(srv, "guarded-room"), nil)
	if err != nil {
		t.Fatalf("decoy dial: %v", err)
	}
	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != SubprotocolTelemetry {
		t.Fatalf("decoy subprotocol = %q", got)
	}

	ticks := readTicks(t, conn, 2)
	for _, tick := range ticks {
		if tick.Type != "tick" {
			t.Fatalf("tick type = %q", tick.Type)
		}
		if len(tick.Quotes) == 0 {
			t.Fatalf("tick has no quotes: %+v", tick)
		}
		for _, q := range tick.Quotes {
			if q.Price <= 0 || q.Symbol == "" {
				t.Fatalf("invalid quote %+v", q)
			}
		}
		if len(tick.News) != 1 || tick.News[0].Title == "" {
			t.Fatalf("tick has no headline: %+v", tick)
		}
	}
}

func TestStreamWrongOrStaleTokenGetsDecoyFeed(t *testing.T) {
	s := New("secret")
	feed := NewFeed(FeedOptions{Offline: true})
	feed.Refresh(context.Background())
	s.SetFeed(feed)
	s.tickDelay = func() time.Duration { return 5 * time.Millisecond }

	srv := httptest.NewServer(s)
	defer srv.Close()

	cases := map[string][]string{
		"garbage": {SubprotocolTelemetry, "v1.1.aa.deadbeef"},
		"stale":   AuthProtocols("guarded-room", "secret", time.Now().Add(-10*time.Minute)),
		"wrong":   AuthProtocols("another-room", "secret", time.Now()),
	}
	for name, protocols := range cases {
		t.Run(name, func(t *testing.T) {
			conn, resp, err := dialWithProtocols(t, streamURL(srv, "guarded-room"), protocols)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != SubprotocolTelemetry {
				t.Fatalf("subprotocol = %q", got)
			}
			ticks := readTicks(t, conn, 1)
			if ticks[0].Type != "tick" {
				t.Fatalf("payload = %+v", ticks[0])
			}
		})
	}
}

func readTicks(t *testing.T, conn *websocket.Conn, want int) []Tick {
	t.Helper()
	var ticks []Tick
	deadline := time.Now().Add(3 * time.Second)
	for len(ticks) < want {
		_ = conn.SetReadDeadline(deadline)
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read tick: %v", err)
		}
		var tick Tick
		if err := json.Unmarshal(data, &tick); err != nil {
			t.Fatalf("decode tick %q: %v", data, err)
		}
		ticks = append(ticks, tick)
	}
	return ticks
}
