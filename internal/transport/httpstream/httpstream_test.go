package httpstream

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	testSecret    = "httpstream-test-secret"
	testNamespace = "route-1"
)

type testLogger struct{}

func (testLogger) Trace(...any)                         {}
func (testLogger) Debug(...any)                         {}
func (testLogger) Info(...any)                          {}
func (testLogger) Warn(...any)                          {}
func (testLogger) Error(...any)                         {}
func (testLogger) Fatal(...any)                         {}
func (testLogger) Panic(...any)                         {}
func (testLogger) TraceContext(context.Context, ...any) {}
func (testLogger) DebugContext(context.Context, ...any) {}
func (testLogger) InfoContext(context.Context, ...any)  {}
func (testLogger) WarnContext(context.Context, ...any)  {}
func (testLogger) ErrorContext(context.Context, ...any) {}
func (testLogger) FatalContext(context.Context, ...any) {}
func (testLogger) PanicContext(context.Context, ...any) {}

func testProfile(mode DownlinkMode) MimicProfile {
	profile := MimicProfile{
		DownlinkMode: mode,
		DownlinkSpecs: []RequestSpec{{
			Method:  http.MethodGet,
			Paths:   []string{"/media/{id}", "/stream/{id}?cursor={cursor}"},
			Headers: map[string]string{"Accept": "application/octet-stream", "Referer": "https://media.test/library"},
		}},
		UplinkSpecs: []RequestSpec{{
			Method:      http.MethodPost,
			Paths:       []string{"/api/progress", "/api/session/{seq}"},
			ContentType: "application/octet-stream",
			Headers:     map[string]string{"Accept": "*/*"},
		}},
		SessionPlacement:    SessionPlacement{Mode: PlaceHeader, Name: "X-Session"},
		ImmediateOnUplink:   true,
		MaxStreamDuration:   10 * time.Second,
		MaxStreamBytes:      8 << 10,
		TargetChunkMin:      1 << 10,
		TargetChunkMax:      4 << 10,
		IdlePauseMin:        time.Millisecond,
		IdlePauseMax:        2 * time.Millisecond,
		PollHold:            20 * time.Millisecond,
		Padding:             Padding{MinPad: 0, MaxPad: 16},
		UplinkBatchWindow:   time.Millisecond,
		MaxUplinkBatchBytes: 32 << 10,
		UTLSProfile:         "chrome",
	}
	if mode == DownlinkChunkedPoll {
		profile.MaxStreamBytes = 1 << 20
	}
	return profile
}

func testServerConfig(fallback http.Handler) ServerConfig {
	return ServerConfig{
		Secret:              testSecret,
		Namespace:           testNamespace,
		Logger:              testLogger{},
		FallbackHandler:     fallback,
		TokenTTL:            10 * time.Minute,
		TokenLeeway:         time.Minute,
		SessionTTL:          10 * time.Second,
		MaxMessageSize:      32 << 10,
		MaxUplinkBodyBytes:  2 << 20,
		ReorderTimeout:      500 * time.Millisecond,
		ReorderCapacity:     1024,
		ReorderMaxBytes:     8 << 20,
		DownlinkReplayBytes: 512 << 10,
		PollHold:            20 * time.Millisecond,
		KeepAliveInterval:   100 * time.Millisecond,
	}
}

func newH2Server(t *testing.T, handler http.Handler) (*httptest.Server, *http.Client, *atomic.Bool, *atomic.Int64) {
	t.Helper()
	var sawH2 atomic.Bool
	var gets atomic.Int64
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 {
			sawH2.Store(true)
		}
		if r.Method == http.MethodGet {
			gets.Add(1)
		}
		handler.ServeHTTP(w, r)
	})
	srv := httptest.NewUnstartedServer(wrapped)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	client := srv.Client()
	if tr, ok := client.Transport.(*http.Transport); ok {
		tr.ForceAttemptHTTP2 = true
	} else {
		client = &http.Client{Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		}}
	}
	return srv, client, &sawH2, &gets
}

func newTestClient(t *testing.T, srv *httptest.Server, httpClient *http.Client, profile MimicProfile) *Client {
	t.Helper()
	cfg := ClientConfig{
		BaseURL:          srv.URL,
		Secret:           testSecret,
		Namespace:        testNamespace,
		UserID:           "alice",
		ExitNodeID:       "42",
		HTTPClient:       httpClient,
		Logger:           testLogger{},
		MaxMessageSize:   32 << 10,
		AckInterval:      50 * time.Millisecond,
		ReadIdleTimeout:  5 * time.Second,
		ReconnectDelay:   10 * time.Millisecond,
		ReconnectMax:     50 * time.Millisecond,
		MaxUplinkRetries: 1,
	}
	client, err := NewClient(profile, cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client
}

func echoSession(ctx context.Context, sess *Session) error {
	for {
		data, err := sess.Receive(ctx)
		if err != nil {
			return err
		}
		if err := sess.Send(ctx, data); err != nil {
			return err
		}
	}
}

func makePayload(size, chunk int) [][]byte {
	var out [][]byte
	for off := 0; off < size; off += chunk {
		end := off + chunk
		if end > size {
			end = size
		}
		buf := make([]byte, end-off)
		for i := range buf {
			buf[i] = byte((off + i) % 251)
		}
		out = append(out, buf)
	}
	return out
}

func startClient(t *testing.T, client *Client, ctx context.Context) {
	t.Helper()
	if err := client.Start(ctx); err != nil {
		t.Fatalf("client start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Stop(stopCtx); err != nil {
			t.Errorf("client stop: %v", err)
		}
	})
}

func runEchoTransfer(t *testing.T, client *Client, ctx context.Context, chunks [][]byte) {
	t.Helper()
	sendErr := make(chan error, 1)
	go func() {
		for _, chunk := range chunks {
			if err := client.Send(ctx, chunk); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()

	expected := bytes.Join(chunks, nil)
	received := 0
	for received < len(expected) {
		select {
		case err := <-sendErr:
			if err != nil {
				t.Fatalf("send: %v", err)
			}
			sendErr = nil
		default:
		}
		recvCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		data, err := client.Receive(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("receive after %d/%d bytes: %v", received, len(expected), err)
		}
		end := received + len(data)
		if end > len(expected) || !bytes.Equal(data, expected[received:end]) {
			t.Fatalf("payload mismatch at offset %d (len=%d)", received, len(data))
		}
		received = end
	}
	if received != len(expected) {
		t.Fatalf("received %d bytes, want %d", received, len(expected))
	}
}

func TestStreamEchoWithRotation(t *testing.T) {
	profile := testProfile(DownlinkStream)
	handler := NewHandler(profile, testServerConfig(nil), echoSession)
	if err := handler.Err(); err != nil {
		t.Fatalf("handler: %v", err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	srv, httpClient, sawH2, gets := newH2Server(t, handler)
	client := newTestClient(t, srv, httpClient, profile)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	startClient(t, client, ctx)

	chunks := makePayload(96<<10, 2<<10)
	runEchoTransfer(t, client, ctx, chunks)

	if !sawH2.Load() {
		t.Fatal("engine requests were not served over HTTP/2")
	}
	if gets.Load() < 2 {
		t.Fatalf("downlink GET count = %d, want at least 2 rotations", gets.Load())
	}
}

func TestChunkedPollEcho(t *testing.T) {
	profile := testProfile(DownlinkChunkedPoll)
	handler := NewHandler(profile, testServerConfig(nil), echoSession)
	if err := handler.Err(); err != nil {
		t.Fatalf("handler: %v", err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	srv, httpClient, sawH2, gets := newH2Server(t, handler)
	client := newTestClient(t, srv, httpClient, profile)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	startClient(t, client, ctx)

	chunks := makePayload(64<<10, 2<<10)
	runEchoTransfer(t, client, ctx, chunks)

	if !sawH2.Load() {
		t.Fatal("engine requests were not served over HTTP/2")
	}
	if gets.Load() < 3 {
		t.Fatalf("downlink GET count = %d, want several polls", gets.Load())
	}
}

func TestReorderingBufferOrderTimeoutAndOverflow(t *testing.T) {
	buf := newReorderingBuffer(200*time.Millisecond, 8, 1<<20)
	if ready, err := buf.Push(1, 2, []byte("two")); err != nil || len(ready) != 0 {
		t.Fatalf("push seq=2: ready=%v err=%v", ready, err)
	}
	ready, err := buf.Push(1, 1, []byte("one"))
	if err != nil {
		t.Fatalf("push seq=1: %v", err)
	}
	if len(ready) != 2 || string(ready[0]) != "one" || string(ready[1]) != "two" {
		t.Fatalf("ready = %q", ready)
	}
	if buf.Delivered() != 2 {
		t.Fatalf("delivered = %d, want 2", buf.Delivered())
	}
	if ready, err := buf.Push(1, 1, []byte("one")); err != nil || len(ready) != 0 {
		t.Fatalf("duplicate should be ignored: ready=%v err=%v", ready, err)
	}

	timeout := newReorderingBuffer(20*time.Millisecond, 8, 1<<20)
	if _, err := timeout.Push(1, 2, []byte("late")); err != nil {
		t.Fatalf("push gap: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := timeout.CheckDeadline(time.Now()); !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("expected reorder timeout reset, got %v", err)
	}

	overflow := newReorderingBuffer(time.Second, 2, 1<<20)
	if _, err := overflow.Push(1, 3, []byte("3")); err != nil {
		t.Fatal(err)
	}
	if _, err := overflow.Push(1, 4, []byte("4")); err != nil {
		t.Fatal(err)
	}
	if _, err := overflow.Push(1, 5, []byte("5")); !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("expected overflow reset, got %v", err)
	}

	epochs := newReorderingBuffer(time.Second, 8, 1<<20)
	if ready, err := epochs.Push(2, 1, []byte("new")); err != nil || len(ready) != 1 {
		t.Fatalf("new epoch: ready=%v err=%v", ready, err)
	}
	if ready, err := epochs.Push(1, 2, []byte("stale")); err != nil || len(ready) != 0 {
		t.Fatalf("stale epoch: ready=%v err=%v", ready, err)
	}
}

func TestServerReordersOutOfOrderPosts(t *testing.T) {
	profile := testProfile(DownlinkStream)
	received := make(chan string, 4)
	onSession := func(ctx context.Context, sess *Session) error {
		for {
			data, err := sess.Receive(ctx)
			if err != nil {
				return err
			}
			received <- string(data)
		}
	}
	handler := NewHandler(profile, testServerConfig(nil), onSession)
	if err := handler.Err(); err != nil {
		t.Fatalf("handler: %v", err)
	}
	t.Cleanup(func() { _ = handler.Close() })
	srv, httpClient, _, _ := newH2Server(t, handler)

	sidStr, sid, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	token, err := signSessionToken(testSecret, SessionInfo{
		SessionID: sidStr, Namespace: testNamespace, UserID: "alice",
	}, time.Now(), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	post := func(seq uint64, msg string) {
		t.Helper()
		frame := encodeFrame(frameData, sid, 1, seq, 0, [][]byte{[]byte(msg)}, 0)
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/progress", bytes.NewReader(frame))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set(profile.SessionPlacement.Name, token)
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("post seq=%d: %v", seq, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("post seq=%d status=%d body=%q", seq, resp.StatusCode, body)
		}
		if got := resp.Header.Get(defaultAckHeader); got != strconv.FormatUint(seq, 10) {
			t.Fatalf("ack header = %q, want %d", got, seq)
		}
	}

	post(2, "two")
	select {
	case got := <-received:
		t.Fatalf("seq=2 was delivered before seq=1: %q", got)
	case <-time.After(150 * time.Millisecond):
	}
	post(1, "one")

	for _, want := range []string{"one", "two"} {
		select {
		case got := <-received:
			if got != want {
				t.Fatalf("received %q, want %q", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout waiting for %q", want)
		}
	}
}

func TestFallbackHandler(t *testing.T) {
	var hits atomic.Int64
	fallback := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("X-Decoy", "yes")
		_, _ = io.WriteString(w, "decoy-page")
	})
	profile := testProfile(DownlinkStream)
	handler := NewHandler(profile, testServerConfig(fallback), echoSession)
	if err := handler.Err(); err != nil {
		t.Fatalf("handler: %v", err)
	}
	t.Cleanup(func() { _ = handler.Close() })
	srv, httpClient, _, _ := newH2Server(t, handler)

	resp, err := httpClient.Get(srv.URL + "/library/index.html")
	if err != nil {
		t.Fatalf("fallback request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "decoy-page" {
		t.Fatalf("fallback response: status=%d body=%q", resp.StatusCode, body)
	}
	if resp.Header.Get("X-Decoy") != "yes" {
		t.Fatal("fallback headers were not passed through")
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/library/other", nil)
	req.Header.Set(profile.SessionPlacement.Name, "hsv1.invalid.token")
	resp, err = httpClient.Do(req)
	if err != nil {
		t.Fatalf("invalid-token request: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "decoy-page" {
		t.Fatalf("invalid token did not fall back: status=%d body=%q", resp.StatusCode, body)
	}
	if hits.Load() < 2 {
		t.Fatalf("fallback hits = %d, want 2", hits.Load())
	}
	if handler.SessionCount() != 0 {
		t.Fatalf("fallback requests created %d sessions", handler.SessionCount())
	}
}

func TestTokenPlacementPaddingAndWrappers(t *testing.T) {
	sidStr, sid, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	info := SessionInfo{SessionID: sidStr, Namespace: testNamespace, UserID: "alice", ExitNodeID: "42"}
	token, err := signSessionToken(testSecret, info, time.Now(), 5*time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	got, err := verifySessionToken(testSecret, token, time.Now(), 10*time.Minute, time.Minute, testNamespace)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got.SessionID != info.SessionID || got.UserID != info.UserID || got.ExitNodeID != info.ExitNodeID {
		t.Fatalf("verified info = %+v", got)
	}
	if _, err := verifySessionToken(testSecret+"x", token, time.Now(), 10*time.Minute, time.Minute, testNamespace); err == nil {
		t.Fatal("expected bad signature error")
	}
	if _, err := verifySessionToken(testSecret, token, time.Now(), 10*time.Minute, time.Minute, "other-ns"); err == nil {
		t.Fatal("expected namespace mismatch")
	}
	expired, err := signSessionToken(testSecret, info, time.Now().Add(-time.Hour), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifySessionToken(testSecret, expired, time.Now(), 10*time.Minute, 0, testNamespace); err == nil {
		t.Fatal("expected expired token error")
	}

	placements := []SessionPlacement{
		{Mode: PlaceCookie, Name: "sid"},
		{Mode: PlaceBearer},
		{Mode: PlaceHeader, Name: "X-Session"},
		{Mode: PlaceQuery, Name: "sid"},
	}
	for _, placement := range placements {
		profile := testProfile(DownlinkStream)
		profile.SessionPlacement = placement
		profile = profile.Normalize()
		req, _ := http.NewRequest(http.MethodGet, "https://media.test/media/1?x=1", nil)
		if err := profile.applySessionToken(req, token); err != nil {
			t.Fatalf("placement %s: %v", placement.Mode, err)
		}
		if extracted := profile.extractSessionToken(req); extracted != token {
			t.Fatalf("placement %s extracted %q", placement.Mode, extracted)
		}
	}

	buckets := Padding{TargetBuckets: []int{128, 256, 512}}
	if got := buckets.PadLen(100); got != 28 {
		t.Fatalf("bucket pad(100) = %d, want 28", got)
	}
	if got := buckets.PadLen(200); got != 56 {
		t.Fatalf("bucket pad(200) = %d, want 56", got)
	}
	fixed := Padding{MinPad: 5, MaxPad: 5}
	if got := fixed.PadLen(100); got != 5 {
		t.Fatalf("fixed pad = %d, want 5", got)
	}

	profile := testProfile(DownlinkStream)
	profile = profile.Normalize()
	if err := profile.Validate(); err != nil {
		t.Fatalf("profile validate: %v", err)
	}

	frame := encodeFrame(frameData, sid, 1, 1, 0, [][]byte{[]byte("wrapped")}, 0)
	for _, spec := range []RequestSpec{
		{Method: http.MethodPost, Paths: []string{"/api"}, ContentType: "application/json",
			BodyTemplate: `{"data":"{{PAYLOAD_B64}}"}`, PayloadField: "data"},
		{Method: http.MethodPost, Paths: []string{"/api"}, ContentType: "application/x-www-form-urlencoded",
			BodyTemplate: `data={{PAYLOAD_B64}}`, PayloadField: "data"},
	} {
		contentType, body, err := spec.WrapUplinkBody(frame)
		if err != nil {
			t.Fatalf("wrap: %v", err)
		}
		raw, err := profile.UnwrapUplinkBody(contentType, body)
		if err != nil {
			t.Fatalf("unwrap %s: %v", contentType, err)
		}
		if !bytes.Equal(raw, frame) {
			t.Fatalf("unwrapped frame mismatch for %s", contentType)
		}
	}
}

func TestSessionIsolationByNamespace(t *testing.T) {
	handler := NewHandler(testProfile(DownlinkStream), testServerConfig(nil), echoSession)
	if err := handler.Err(); err != nil {
		t.Fatalf("handler: %v", err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	sidStr, _, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	first, err := handler.getOrCreate(SessionInfo{SessionID: sidStr, Namespace: "route-1", UserID: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := handler.getOrCreate(SessionInfo{SessionID: sidStr, Namespace: "route-2", UserID: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("sessions with different namespaces were merged")
	}
	if handler.SessionCount() != 2 {
		t.Fatalf("session count = %d, want 2", handler.SessionCount())
	}
}

func TestSecurePipeAndTunnelMultiplexer(t *testing.T) {
	const pipeSecret = "pipe-e2e-secret"
	profile := testProfile(DownlinkStream)
	payload := bytes.Repeat([]byte("secure-httpstream-"), 2048)

	onSession := func(ctx context.Context, sess *Session) error {
		secured, err := security.WrapTransport(sess, pipeSecret, security.RoleServer, testLogger{})
		if err != nil {
			return err
		}
		mux := tunnel.New(secured, testLogger{}, tunnel.Config{
			FlushThreshold: 12000,
			FlushInterval:  5 * time.Millisecond,
		})
		if err := mux.Start(ctx); err != nil {
			return err
		}
		defer func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = mux.Stop(stopCtx)
		}()

		stream, err := mux.AcceptStream(ctx)
		if err != nil {
			return err
		}
		defer stream.Close()
		_ = stream.SetDeadline(time.Now().Add(20 * time.Second))

		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(stream, buf); err != nil {
			return err
		}
		if !bytes.Equal(buf, payload) {
			return errors.New("server payload mismatch")
		}
		if _, err := stream.Write(buf); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}

	handler := NewHandler(profile, testServerConfig(nil), onSession)
	if err := handler.Err(); err != nil {
		t.Fatalf("handler: %v", err)
	}
	t.Cleanup(func() { _ = handler.Close() })
	srv, httpClient, sawH2, _ := newH2Server(t, handler)

	raw := newTestClient(t, srv, httpClient, profile)
	secured, err := security.WrapTransport(raw, pipeSecret, security.RoleClient, testLogger{})
	if err != nil {
		t.Fatalf("wrap client: %v", err)
	}
	mux := tunnel.New(secured, testLogger{}, tunnel.Config{
		FlushThreshold: 12000,
		FlushInterval:  5 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	startClient(t, raw, ctx)
	if err := mux.Start(ctx); err != nil {
		t.Fatalf("mux start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = mux.Stop(stopCtx)
	})

	stream, err := mux.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := stream.Write(payload); err != nil {
		t.Fatalf("stream write: %v", err)
	}
	echo := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, echo); err != nil {
		t.Fatalf("stream read: %v", err)
	}
	if !bytes.Equal(echo, payload) {
		t.Fatal("stream echo mismatch")
	}

	var clientWire int64
	for _, layer := range raw.WalkLayers() {
		clientWire += layer.Snapshot.NetworkSent + layer.Snapshot.NetworkRecv
	}
	if clientWire == 0 {
		t.Fatal("client telemetry counters are empty")
	}
	layers := handler.WalkLayers()
	if len(layers) < 2 || layers[0].Kind != "mux" {
		t.Fatalf("handler layers = %+v", layers)
	}
	var serverWire int64
	for _, layer := range layers {
		serverWire += layer.Snapshot.NetworkSent + layer.Snapshot.NetworkRecv
	}
	if serverWire == 0 {
		t.Fatal("server telemetry counters are empty")
	}
	if !sawH2.Load() {
		t.Fatal("secure pipe traffic was not HTTP/2")
	}
	if !strings.EqualFold(string(profile.DownlinkMode), string(DownlinkStream)) {
		t.Fatal("unexpected profile mode")
	}
}

func TestClientResetsSequencesOnServerGenerationChange(t *testing.T) {
	client, err := NewClient(testProfile(DownlinkStream), ClientConfig{
		BaseURL:    "https://media.test",
		Secret:     testSecret,
		Namespace:  testNamespace,
		UserID:     "alice",
		HTTPClient: http.DefaultClient,
		Logger:     testLogger{},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	if epoch, seq := client.nextSequence(); epoch != 1 || seq != 1 {
		t.Fatalf("initial sequence = %d/%d", epoch, seq)
	}

	first := decodedFrame{
		Kind:      frameData,
		Epoch:     100,
		Seq:       1,
		SessionID: client.sid,
		Messages:  [][]byte{[]byte("one")},
	}
	if err := client.handleDownlinkFrame(first); err != nil {
		t.Fatalf("first generation frame: %v", err)
	}
	select {
	case item := <-client.recvCh:
		if item.err != nil || string(item.data) != "one" {
			t.Fatalf("downlink item = %+v", item)
		}
	default:
		t.Fatal("downlink data was not queued")
	}
	if client.downlinkCursor.Load() != 1 || client.downlinkNext.Load() != 2 {
		t.Fatalf("cursor/next = %d/%d", client.downlinkCursor.Load(), client.downlinkNext.Load())
	}

	changed := decodedFrame{Kind: frameKeepAlive, Epoch: 200, SessionID: client.sid}
	if err := client.handleDownlinkFrame(changed); !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("generation change error = %v", err)
	}
	if client.downlinkCursor.Load() != 0 || client.downlinkNext.Load() != 1 {
		t.Fatalf("downlink state was not reset: %d/%d",
			client.downlinkCursor.Load(), client.downlinkNext.Load())
	}
	if epoch, seq := client.nextSequence(); epoch != 2 || seq != 1 {
		t.Fatalf("uplink sequence after reset = %d/%d, want 2/1", epoch, seq)
	}
}

func TestHandlerPinnedIdentity(t *testing.T) {
	cfg := testServerConfig(nil)
	cfg.PinnedUserID = "alice"
	cfg.PinnedExitNodeID = "42"
	cfg.RequireUser = true
	handler := NewHandler(testProfile(DownlinkStream), cfg, echoSession)
	if err := handler.Err(); err != nil {
		t.Fatalf("handler: %v", err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	info := SessionInfo{Namespace: testNamespace}
	if !handler.matchPinned(&info) {
		t.Fatal("pinned identity should fill missing user and exit")
	}
	if info.UserID != "alice" || info.ExitNodeID != "42" {
		t.Fatalf("pinned info = %+v", info)
	}

	mismatch := SessionInfo{Namespace: testNamespace, UserID: "bob", ExitNodeID: "42"}
	if handler.matchPinned(&mismatch) {
		t.Fatal("mismatched pinned user was accepted")
	}
}
