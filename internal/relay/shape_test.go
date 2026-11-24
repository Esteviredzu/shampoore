package relay

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Esteviredzu/shampoore/internal/wsshape"
)

func newTestServer(t *testing.T, s *Server) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	return srv
}

func offlineServer(t *testing.T, shape string) *Server {
	t.Helper()
	dir := writeDecoySite(t, map[string]string{
		"index.html":    "<html>decoy</html>",
		"ws-shape.json": shape,
	})
	s := New("secret")
	feed := NewFeed(FeedOptions{Offline: true, Now: func() time.Time { return fixedNow() }})
	feed.Refresh(context.Background())
	s.SetFeed(feed)
	s.SetDecoyDir(dir)
	return s
}

func TestSetDecoyDirLoadsShape(t *testing.T) {
	s := offlineServer(t, `{
		"version": 1,
		"batch": {"window_ms": 1},
		"padding": {"min_total_bytes": 4096, "max_extra_bytes": 64, "small_extra_bytes": 8},
		"pulse": {"ping_interval_ms": 1500, "read_idle_ms": 2500},
		"decoy": {"tick_interval_ms": 5, "tick_jitter_ms": 0},
		"rate": {"max_rate_kbps": 64, "burst_bytes": 8192}
	}`)
	shape := s.Shape()
	if shape.Batch.WindowMs != 1 || shape.Batch.MaxBytes != wsshape.Default().Batch.MaxBytes {
		t.Fatalf("batch = %+v", shape.Batch)
	}
	if shape.Padding.MinTotalBytes != 4096 || !shape.Padding.On() {
		t.Fatalf("padding = %+v", shape.Padding)
	}
	if shape.Pulse.PingIntervalMs != 1500 || shape.Pulse.ReadIdleMs != 2500 {
		t.Fatalf("pulse = %+v", shape.Pulse)
	}
	if shape.Rate.MaxRateKBps != 64 || shape.Rate.BurstBytes != 8192 {
		t.Fatalf("rate = %+v", shape.Rate)
	}
	if got := s.ShapeSource(); got == "" || got == "default" {
		t.Fatalf("shape source = %q", got)
	}
	if err := s.ShapeError(); err != nil {
		t.Fatalf("shape error = %v", err)
	}
}

func TestShapeDefaultsWithoutProfile(t *testing.T) {
	dir := writeDecoySite(t, map[string]string{"index.html": "<html>decoy</html>"})
	s := New("")
	s.SetDecoyDir(dir)
	if got, want := s.Shape().Pulse.PingIntervalMs, wsshape.Default().Pulse.PingIntervalMs; got != want {
		t.Fatalf("ping interval = %d, want %d", got, want)
	}
	if s.ShapeSource() != "default" {
		t.Fatalf("shape source = %q", s.ShapeSource())
	}
}

func TestInvalidShapeFallsBackToDefaults(t *testing.T) {
	s := offlineServer(t, `{broken`)
	if err := s.ShapeError(); err == nil {
		t.Fatal("invalid profile was not reported")
	}
	if got, want := s.Shape().Batch.MaxBytes, wsshape.Default().Batch.MaxBytes; got != want {
		t.Fatalf("batch max = %d, want %d", got, want)
	}
	if s.ShapeSource() == "" {
		t.Fatal("shape source is empty")
	}
}

func TestDecoyTickFramesMatchShape(t *testing.T) {
	s := offlineServer(t, `{
		"padding": {"min_total_bytes": 4096, "max_extra_bytes": 64, "small_extra_bytes": 8},
		"decoy": {"tick_interval_ms": 5, "tick_jitter_ms": 0}
	}`)
	s.tickDelay = func() time.Duration { return 5 * time.Millisecond }
	srv := newTestServer(t, s)

	conn, _, err := dialWithProtocols(t, streamURL(srv, "guarded-room"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	for i := 0; i < 3; i++ {
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if len(data) < 4096 || len(data) > 4159 {
			t.Fatalf("frame %d = %d bytes, outside the profile target 4096..4159", i, len(data))
		}
		var tick Tick
		if err := json.Unmarshal(data, &tick); err != nil {
			t.Fatalf("frame %d is not valid JSON: %v", i, err)
		}
		if tick.Type != "tick" || len(tick.Quotes) == 0 {
			t.Fatalf("frame %d payload = %+v", i, tick)
		}
	}
}

func TestDecoyFeedRespectsRateLimit(t *testing.T) {
	s := offlineServer(t, `{
		"padding": {"min_total_bytes": 512, "max_extra_bytes": 64, "small_extra_bytes": 8},
		"decoy": {"tick_interval_ms": 1, "tick_jitter_ms": 0},
		"rate": {"max_rate_kbps": 8, "burst_bytes": 1024}
	}`)
	s.tickDelay = func() time.Duration { return time.Millisecond }
	srv := newTestServer(t, s)

	conn, _, err := dialWithProtocols(t, streamURL(srv, "guarded-room"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	deadline := time.Now().Add(400 * time.Millisecond)
	total := 0
	frames := 0
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(deadline)
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		total += len(data)
		frames++
	}
	if frames == 0 {
		t.Fatal("decoy feed sent nothing")
	}
	if total > 8*1024 {
		t.Fatalf("received %d bytes in 400ms at 8 KB/s: limiter is not applied", total)
	}
	if total < 512 {
		t.Fatalf("received only %d bytes; the feed looks broken", total)
	}
}

func TestDecoyReadLimitUsesShape(t *testing.T) {
	s := offlineServer(t, `{"frame": {"max_message_bytes": 1024}}`)
	s.tickDelay = func() time.Duration { return 10 * time.Millisecond }
	srv := newTestServer(t, s)

	conn, _, err := dialWithProtocols(t, streamURL(srv, "guarded-room"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, make([]byte, 4096)); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}
