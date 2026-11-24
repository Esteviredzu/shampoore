package raw_ws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/wsshape"
)

func TestShapeDefaultsMatchLegacyConstants(t *testing.T) {
	d := wsshape.Default()
	checks := []struct {
		name string
		ok   bool
	}{
		{"frame header", frameHeaderSize == d.Frame.HeaderBytes},
		{"frame version", int(frameVersion) == d.Frame.FrameVersion},
		{"max frame", maxFrameSize == d.Frame.MaxFrameBytes},
		{"max message", maxWSPayload == d.Frame.MaxMessageBytes},
		{"batch max", maxBatchPayload == d.Batch.MaxBytes},
		{"batch window", batchBaseDelay == durationMs(d.Batch.WindowMs)},
		{"batch jitter", batchJitterMax == durationMs(d.Batch.JitterMs)},
		{"queue cap", sendQueueCap == d.Batch.QueueCapacity},
		{"ack timeout", sendAckTimeout == durationMs(d.Batch.AckTimeoutMs)},
		{"pad min", minPadTotal == d.Padding.MinTotalBytes},
		{"pad extra", maxPadExtra == d.Padding.MaxExtraBytes},
		{"pad small", smallPadExtra == d.Padding.SmallExtraBytes},
		{"write timeout", writeTimeout == durationMs(d.Pulse.WriteTimeoutMs)},
		{"ping interval", pingInterval == durationMs(d.Pulse.PingIntervalMs)},
		{"ping deadline", pingDeadline == durationMs(d.Pulse.PingTimeoutMs)},
		{"read idle", readIdleLimit == durationMs(d.Pulse.ReadIdleMs)},
		{"reconnect", defaultReconnectDelay == durationMs(d.Lifecycle.ReconnectBaseMs)},
		{"reconnect max", reconnectMaxDelay == durationMs(d.Lifecycle.ReconnectMaxMs)},
		{"rotate idle", rotateIdleWindow == durationMs(d.Lifecycle.RotateIdleMs)},
		{"warmup min", defaultWarmupMinDelay == durationMs(d.Lifecycle.WarmupMinMs)},
		{"warmup max", defaultWarmupMaxDelay == durationMs(d.Lifecycle.WarmupMaxMs)},
		{"rotation min", defaultRotationMin == durationMs(d.Lifecycle.RotationMinMs)},
		{"rotation max", defaultRotationMax == durationMs(d.Lifecycle.RotationMaxMs)},
	}
	for _, c := range checks {
		if !c.ok {
			t.Errorf("%s does not match wsshape.Default()", c.name)
		}
	}
}

func TestTransportAppliesShapeOverrides(t *testing.T) {
	cfg := testConfig("ws://example.test", true)
	cfg.Shape = wsshape.Config{
		Batch: wsshape.Batch{WindowMs: 1, JitterMs: 2, MaxBytes: 4096, QueueCapacity: 8},
		Pulse: wsshape.Pulse{PingIntervalMs: 1500, PingTimeoutMs: 700, ReadIdleMs: 2500},
		Frame: wsshape.Frame{MaxFrameBytes: 2048},
		Rate:  wsshape.Rate{MaxRateKBps: 64, BurstBytes: 8192},
		Decoy: wsshape.Decoy{TickIntervalMs: 42},
		Lifecycle: wsshape.Lifecycle{
			WarmupMinMs: 10, WarmupMaxMs: 20,
			RotationMinMs: 30000, RotationMaxMs: 60000,
			ReconnectBaseMs: 500, ReconnectMaxMs: 4000, RotateIdleMs: 300,
		},
	}
	tr := newTransport(cfg, testLogger{})
	defer func() { _ = tr.Stop(context.Background()) }()

	if got := tr.pingEvery(); got != 1500*time.Millisecond {
		t.Fatalf("pingEvery = %s", got)
	}
	if got := tr.pingTimeout(); got != 700*time.Millisecond {
		t.Fatalf("pingTimeout = %s", got)
	}
	if got := tr.pingWait(); got != 2500*time.Millisecond {
		t.Fatalf("pingWait = %s", got)
	}
	if got := tr.MaxMessageSize(); got != 2048 {
		t.Fatalf("MaxMessageSize = %d", got)
	}
	if got := tr.queueCapacity(); got != 8 {
		t.Fatalf("queueCapacity = %d", got)
	}
	if got := tr.batchLimit(); got != 4096 {
		t.Fatalf("batchLimit = %d", got)
	}
	if tr.lim == nil || tr.burst != 8192 {
		t.Fatalf("rate limiter not installed: lim=%v burst=%d", tr.lim, tr.burst)
	}
	for i := 0; i < 100; i++ {
		d := tr.nextFlushDelay()
		if d < time.Millisecond || d >= 3*time.Millisecond {
			t.Fatalf("nextFlushDelay = %s outside 1..3ms", d)
		}
	}
	if d := tr.nextWarmupPause(); d < 10*time.Millisecond || d > 20*time.Millisecond {
		t.Fatalf("nextWarmupPause = %s", d)
	}
	if d := tr.nextRotationDelay(); d < 30*time.Second || d > time.Minute {
		t.Fatalf("nextRotationDelay = %s", d)
	}
}

func TestBuildAppliesAdvancedShapeValues(t *testing.T) {
	s := &app.Settings{
		Type:      "raw_ws",
		Transport: json.RawMessage(`{"server_url":"wss://127.0.0.1:1","room":"r","password":"p"}`),
		Advanced: app.Values{
			"shape_max_rate_kbps":       "64",
			"shape_batch_window_ms":     "2",
			"shape_ping_interval_ms":    "1500",
			"shape_pad_min_total_bytes": "128",
		},
	}
	res, err := build(context.Background(), s, "client", testLogger{}, app.Values{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	tr, ok := res.Transport.(*wsTransport)
	if !ok {
		t.Fatalf("transport type = %T", res.Transport)
	}
	defer func() { _ = tr.Stop(context.Background()) }()
	if tr.shape.Rate.MaxRateKBps != 64 || tr.lim == nil {
		t.Fatalf("rate override not applied: %+v", tr.shape.Rate)
	}
	if tr.shape.Batch.WindowMs != 2 {
		t.Fatalf("batch window = %d", tr.shape.Batch.WindowMs)
	}
	if got := tr.pingEvery(); got != 1500*time.Millisecond {
		t.Fatalf("pingEvery = %s", got)
	}
	if tr.shape.Padding.MinTotalBytes != 128 || !tr.shape.Padding.On() {
		t.Fatalf("padding = %+v", tr.shape.Padding)
	}
}

func waitActive(t *testing.T, tr *wsTransport) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		tr.connMu.Lock()
		active := tr.active
		tr.connMu.Unlock()
		if active != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("transport did not connect")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSendHonorsMaxRate(t *testing.T) {
	var total atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			total.Add(int64(len(data)))
		}
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL, false)
	cfg.Shape = wsshape.Config{
		Frame: wsshape.Frame{MaxFrameBytes: 1024},
		Rate:  wsshape.Rate{MaxRateKBps: 32, BurstBytes: 4096},
	}
	tr := newTransport(cfg, testLogger{})
	tr.reconnectDelay = 20 * time.Millisecond
	cancel := startTransport(t, tr)
	defer cancel()
	defer stopWithTimeout(t, tr)

	waitActive(t, tr)

	payload := make([]byte, 12*1024)
	start := time.Now()
	if err := tr.Send(context.Background(), payload); err != nil {
		t.Fatalf("send: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond {
		t.Fatalf("12 KiB at 32 KiB/s finished in %s: the limiter is not applied", elapsed)
	}
	deadline := time.Now().Add(2 * time.Second)
	for total.Load() < int64(len(payload)) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := total.Load(); got != int64(len(payload)) {
		t.Fatalf("server received %d bytes, want %d", got, len(payload))
	}
}

func TestShapeURLCodecRoundTrip(t *testing.T) {
	q := url.Values{}
	q.Set("server_url", "wss://example.test")
	q.Set("room", "room-1")
	q.Set("password", "secret")
	q.Set("batch", "1")
	q.Set("padding", "1")
	q.Set("shape_batch_window_ms", "3")
	q.Set("shape_pad_min_total_bytes", "96")
	q.Set("shape_max_rate_kbps", "128")

	raw, err := decodeURL(q)
	if err != nil {
		t.Fatalf("decodeURL: %v", err)
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.Shape.Batch.WindowMs != 3 || cfg.Shape.Padding.MinTotalBytes != 96 || !cfg.Shape.Padding.On() {
		t.Fatalf("decoded shape = %+v", cfg.Shape)
	}
	if cfg.Shape.Rate.MaxRateKBps != 128 {
		t.Fatalf("decoded rate = %+v", cfg.Shape.Rate)
	}

	out := url.Values{}
	if err := encodeURL(raw, out); err != nil {
		t.Fatalf("encodeURL: %v", err)
	}
	for key, want := range map[string]string{
		"shape_batch_window_ms":     "3",
		"shape_pad_min_total_bytes": "96",
		"shape_max_rate_kbps":       "128",
	} {
		if got := out.Get(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
}

func TestEncodedFrameUsesShapePadding(t *testing.T) {
	pad := wsshape.Padding{MinTotalBytes: 512, MaxExtraBytes: 64, SmallExtraBytes: 8}
	pad.SetEnabled(true)

	small := []byte("hi")
	for i := 0; i < 200; i++ {
		frame := encodeFrame([][]byte{small}, false, true, pad)
		target := len(frame) - frameHeaderSize
		if target < 512 || target > 575 {
			t.Fatalf("small frame target %d outside 512..575", target)
		}
	}
	large := make([]byte, 1024)
	for i := 0; i < 200; i++ {
		frame := encodeFrame([][]byte{large}, false, true, pad)
		extra := len(frame) - frameHeaderSize - len(large)
		if extra < 0 || extra > 7 {
			t.Fatalf("large frame pad %d outside 0..7", extra)
		}
	}
	if got := encodeFrame([][]byte{small}, false, false, pad); len(got) != frameHeaderSize+len(small) {
		t.Fatalf("padding disabled still grew the frame to %d", len(got))
	}
}
