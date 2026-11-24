package jellyfin

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/capability"
	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multipipe/bond"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/provisioner"
	"github.com/Esteviredzu/shampoore/internal/transport"
	"github.com/Esteviredzu/shampoore/internal/transport/httpstream"
)

const (
	testSecret     = "jellyfin-test-secret"
	testPipeSecret = "jellyfin-pipe-secret"
	testNamespace  = "route-1"
	testExitNodeID = "42"
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

func testConfig() Config {
	return Config{
		URL:       "https://media.example.com",
		Secret:    testSecret,
		Namespace: testNamespace,
	}
}

func echoStreams(ctx context.Context, mux multiplexing.StreamMultiplexer) error {
	for {
		stream, err := mux.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go func(s multiplexing.Stream) {
			defer s.Close()
			_, _ = io.Copy(s, s)
		}(stream)
	}
}

func newHTTP2Front(t *testing.T, handler http.Handler) (*httptest.Server, *http.Client) {
	t.Helper()
	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	client := srv.Client()
	if tr, ok := client.Transport.(*http.Transport); ok {
		tr.ForceAttemptHTTP2 = true
	} else {
		client = &http.Client{Transport: &http.Transport{ForceAttemptHTTP2: true}}
	}
	return srv, client
}

func startMux(t *testing.T, cfg Config, exitNodeID string, httpClient *http.Client, password string) (*Client, multiplexing.StreamMultiplexer) {
	t.Helper()
	raw, err := newClient(cfg, exitNodeID, httpClient, testLogger{})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx := context.Background()
	if err := raw.Start(ctx); err != nil {
		t.Fatalf("client start: %v", err)
	}
	var tr transport.Transport = raw
	if password != "" {
		secured, err := security.WrapTransport(raw, password, security.RoleClient, testLogger{})
		if err != nil {
			t.Fatalf("wrap client: %v", err)
		}
		tr = secured
	}
	mux := tunnel.New(tr, testLogger{}, tunnel.Config{
		FlushThreshold: 12000,
		FlushInterval:  5 * time.Millisecond,
	})
	if err := mux.Start(ctx); err != nil {
		t.Fatalf("mux start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mux.Stop(stopCtx)
		_ = raw.Stop(stopCtx)
	})
	return raw, mux
}

func roundTrip(t *testing.T, ctx context.Context, mux multiplexing.StreamMultiplexer, payload []byte) {
	t.Helper()
	stream, err := mux.OpenStream(ctx, map[string]any{"host": "media.example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := stream.Write(payload); err != nil {
		t.Fatalf("stream write: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("stream read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("echo mismatch: got %d bytes, want %d", len(got), len(payload))
	}
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

type recordedRequest struct {
	Method   string
	Path     string
	RawQuery string
	Header   http.Header
	Body     []byte
}

type requestRecorder struct {
	next http.Handler

	mu       sync.Mutex
	requests []recordedRequest
}

func (r *requestRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(io.LimitReader(req.Body, 1<<20))
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	r.mu.Lock()
	r.requests = append(r.requests, recordedRequest{
		Method:   req.Method,
		Path:     req.URL.Path,
		RawQuery: req.URL.RawQuery,
		Header:   req.Header.Clone(),
		Body:     body,
	})
	r.mu.Unlock()
	r.next.ServeHTTP(w, req)
}

func (r *requestRecorder) snapshot() []recordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedRequest, len(r.requests))
	copy(out, r.requests)
	return out
}
func TestProfileForUTLSProfile(t *testing.T) {
	cases := map[string]string{
		"":          "chrome",
		"default":   "chrome",
		"CHROME":    "chrome",
		"random":    "random",
		"firefox":   "firefox",
		" ios ":     "ios",
		"edge":      "edge",
		"safari":    "safari",
		"something": "chrome",
	}
	for raw, want := range cases {
		profile := profileFor(raw)
		if profile.UTLSProfile != want {
			t.Fatalf("profileFor(%q).UTLSProfile = %q, want %q", raw, profile.UTLSProfile, want)
		}
	}
}

func TestMediaClientHeaderRandomized(t *testing.T) {
	headerA := profileFor("").UplinkSpecs[0].Headers["Authorization"]
	headerB := profileFor("").UplinkSpecs[0].Headers["Authorization"]

	if !strings.HasPrefix(headerA, `MediaBrowser Client="Jellyfin%20Web"`) {
		t.Fatalf("authorization = %q", headerA)
	}
	if !strings.Contains(headerA, `Device="Chrome"`) {
		t.Fatalf("device is not neutral: %q", headerA)
	}
	if strings.Contains(headerA, "Firefox") {
		t.Fatalf("hardcoded Firefox fingerprint leaked: %q", headerA)
	}
	if !strings.Contains(headerA, `DeviceId="`) || !strings.Contains(headerA, `Version="12.4.0"`) {
		t.Fatalf("authorization = %q", headerA)
	}
	if headerA == headerB {
		t.Fatal("DeviceId must be regenerated for every client instance")
	}
}

func TestProfileMatchesHAR(t *testing.T) {
	profile := profileFor("")
	if err := profile.Validate(); err != nil {
		t.Fatalf("profile validate: %v", err)
	}
	if profile.Name != "jellyfin" {
		t.Fatalf("name = %q", profile.Name)
	}
	if profile.UTLSProfile != "chrome" {
		t.Fatalf("utls = %q", profile.UTLSProfile)
	}
	if profile.SessionPlacement.Mode != httpstream.PlaceHeader || profile.SessionPlacement.Name != "X-Emby-Token" {
		t.Fatalf("placement = %+v", profile.SessionPlacement)
	}
	if profile.DownlinkMode != httpstream.DownlinkStream {
		t.Fatalf("downlink mode = %q", profile.DownlinkMode)
	}
	if profile.MaxStreamBytes != 8<<20 || profile.MaxStreamDuration != 3*time.Minute {
		t.Fatalf("stream limits = %d/%s", profile.MaxStreamBytes, profile.MaxStreamDuration)
	}
	if profile.UplinkBatchWindow != 15*time.Millisecond || profile.MaxUplinkBatchBytes != 32<<10 {
		t.Fatalf("uplink batch = %s/%d", profile.UplinkBatchWindow, profile.MaxUplinkBatchBytes)
	}
	wantBuckets := []int{560, 1536, 4096, 16384, 65536}
	if len(profile.Padding.TargetBuckets) != len(wantBuckets) {
		t.Fatalf("buckets = %v", profile.Padding.TargetBuckets)
	}
	for i := range wantBuckets {
		if profile.Padding.TargetBuckets[i] != wantBuckets[i] {
			t.Fatalf("buckets = %v", profile.Padding.TargetBuckets)
		}
	}
	if profile.Padding.MinPad != 64 || profile.Padding.MaxPad != 512 {
		t.Fatalf("padding = %+v", profile.Padding)
	}

	down := profile.DownlinkSpecs[0]
	if down.Method != http.MethodGet || len(down.Paths) != 1 || down.Paths[0] != "/Videos/{id}/stream.mp4" {
		t.Fatalf("downlink spec = %+v", down)
	}
	if down.Query["Static"][0] != "true" || down.Query["mediaSourceId"][0] != "{id}" ||
		down.Query["deviceId"][0] != "{rand}" || down.Query["Tag"][0] != "{cursor}" {
		t.Fatalf("downlink query = %+v", down.Query)
	}
	if down.Headers["Range"] != "bytes={seq}-" {
		t.Fatalf("range header = %q", down.Headers["Range"])
	}

	up := profile.UplinkSpecs[0]
	if up.Method != http.MethodPost || up.ContentType != "application/json" || up.PayloadField != "PlaySessionId" {
		t.Fatalf("uplink spec = %+v", up)
	}
	if !strings.Contains(up.BodyTemplate, `"PlaySessionId":"{{PAYLOAD_B64}}"`) {
		t.Fatalf("uplink body = %s", up.BodyTemplate)
	}
	if !strings.Contains(up.Headers["Authorization"], `Client="Jellyfin%20Web"`) {
		t.Fatalf("authorization = %q", up.Headers["Authorization"])
	}

	if len(profile.DecoyRequests) != 1 {
		t.Fatalf("decoys = %+v", profile.DecoyRequests)
	}
	decoy := profile.DecoyRequests[0]
	if decoy.IntervalMin != 12*time.Second || decoy.IntervalMax != 25*time.Second {
		t.Fatalf("decoy interval = %s..%s", decoy.IntervalMin, decoy.IntervalMax)
	}
}

func TestFallbackProxiesToJellyfin(t *testing.T) {
	var mu sync.Mutex
	var forwardedFor, forwardedProto string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		forwardedFor = r.Header.Get("X-Forwarded-For")
		forwardedProto = r.Header.Get("X-Forwarded-Proto")
		mu.Unlock()
		switch r.URL.Path {
		case "/System/Info/Public":
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("X-Mock-Jellyfin", "yes")
			_, _ = io.WriteString(w, `{"ProductName":"Jellyfin Server","Version":"10.9.11"}`)
		case "/web/index.html":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = io.WriteString(w, `<html><title>Jellyfin</title></html>`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	cfg := testConfig()
	cfg.UpstreamURL = upstream.URL
	srv, err := newServer(cfg, testLogger{}, nil, "")
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	front := httptest.NewServer(srv.Handler())
	defer front.Close()

	resp, err := http.Get(front.URL + "/System/Info/Public")
	if err != nil {
		t.Fatalf("fallback get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "Jellyfin Server") {
		t.Fatalf("fallback response: status=%d body=%q", resp.StatusCode, body)
	}
	if resp.Header.Get("X-Mock-Jellyfin") != "yes" {
		t.Fatal("upstream headers were not passed through")
	}

	mu.Lock()
	gotFor, gotProto := forwardedFor, forwardedProto
	mu.Unlock()
	if strings.TrimSpace(gotFor) == "" {
		t.Fatal("X-Forwarded-For was not set")
	}
	if gotProto != "http" {
		t.Fatalf("X-Forwarded-Proto = %q, want http", gotProto)
	}

	resp, err = http.Get(front.URL + "/web/index.html")
	if err != nil {
		t.Fatalf("fallback html: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "Jellyfin") {
		t.Fatalf("fallback html: status=%d body=%q", resp.StatusCode, body)
	}
}

func TestStubFallbackWithoutUpstream(t *testing.T) {
	cfg := testConfig()
	srv, err := newServer(cfg, testLogger{}, nil, "")
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()
	front := httptest.NewServer(srv.Handler())
	defer front.Close()

	for _, tc := range []struct {
		path string
		want string
	}{
		{"/System/Info/Public", `"ProductName":"Jellyfin Server"`},
		{"/System/Endpoint", `"IsLocal"`},
		{"/SyncPlay/List", `[]`},
		{"/web/index.html", "Jellyfin Server"},
	} {
		resp, err := http.Get(front.URL + tc.path)
		if err != nil {
			t.Fatalf("%s: %v", tc.path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), tc.want) {
			t.Fatalf("%s: status=%d body=%q", tc.path, resp.StatusCode, body)
		}
	}

	resp, err := http.Get(front.URL + "/unknown")
	if err != nil {
		t.Fatalf("unknown: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown status = %d", resp.StatusCode)
	}
}

func TestTunnelEndToEnd(t *testing.T) {
	var mu sync.Mutex
	var upstreamTunnelPaths []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/Videos/") || strings.HasPrefix(r.URL.Path, "/Sessions/") {
			mu.Lock()
			upstreamTunnelPaths = append(upstreamTunnelPaths, r.URL.Path)
			mu.Unlock()
		}
		switch r.URL.Path {
		case "/System/Info/Public":
			_, _ = io.WriteString(w, `{"ProductName":"Jellyfin Server"}`)
		case "/System/Endpoint", "/SyncPlay/List":
			_, _ = io.WriteString(w, `[]`)
		default:
			_, _ = io.WriteString(w, `<html>Jellyfin</html>`)
		}
	}))
	defer upstream.Close()

	cfg := testConfig()
	cfg.UpstreamURL = upstream.URL
	srv, err := newServer(cfg, testLogger{}, nil, testPipeSecret)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.SetSessionHandler(echoStreams)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	recorder := &requestRecorder{next: srv.Handler()}
	front, httpClient := newHTTP2Front(t, recorder)
	cfg.URL = front.URL

	raw, mux := startMux(t, cfg, "", httpClient, testPipeSecret)
	_ = raw
	payload := bytes.Repeat([]byte("jellyfin-stream-"), 2048)
	roundTrip(t, ctx, mux, payload)

	requests := recorder.snapshot()
	var downlink *recordedRequest
	var uplink *recordedRequest
	for i := range requests {
		req := &requests[i]
		switch {
		case req.Method == http.MethodGet && strings.HasPrefix(req.Path, "/Videos/") && strings.HasSuffix(req.Path, "/stream.mp4"):
			downlink = req
		case req.Method == http.MethodPost && (req.Path == "/Sessions/Playing/Progress" || req.Path == "/Sessions/Playing"):
			uplink = req
		}
	}
	if downlink == nil {
		t.Fatalf("no Jellyfin downlink request observed: %+v", requests)
	}
	if downlink.Header.Get("X-Emby-Token") == "" {
		t.Fatal("downlink request has no X-Emby-Token")
	}
	for _, key := range []string{"Static=true", "mediaSourceId=", "deviceId=", "Tag="} {
		if !strings.Contains(downlink.RawQuery, key) {
			t.Fatalf("downlink query %q missing %q", downlink.RawQuery, key)
		}
	}
	if !strings.HasPrefix(downlink.Header.Get("Range"), "bytes=") {
		t.Fatalf("downlink range = %q", downlink.Header.Get("Range"))
	}

	if uplink == nil {
		t.Fatalf("no Jellyfin uplink request observed: %+v", requests)
	}
	if uplink.Header.Get("X-Emby-Token") == "" {
		t.Fatal("uplink request has no X-Emby-Token")
	}
	if !strings.Contains(uplink.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("uplink content type = %q", uplink.Header.Get("Content-Type"))
	}
	if !strings.Contains(uplink.Header.Get("Authorization"), `Client="Jellyfin%20Web"`) {
		t.Fatalf("uplink authorization = %q", uplink.Header.Get("Authorization"))
	}
	var body map[string]any
	if err := json.Unmarshal(uplink.Body, &body); err != nil {
		t.Fatalf("uplink json: %v (body=%q)", err, uplink.Body)
	}
	playSession, ok := body["PlaySessionId"].(string)
	if !ok || playSession == "" {
		t.Fatalf("PlaySessionId = %#v", body["PlaySessionId"])
	}
	if _, err := base64.StdEncoding.DecodeString(playSession); err != nil {
		t.Fatalf("PlaySessionId is not a frame payload: %v", err)
	}

	mu.Lock()
	leaked := append([]string(nil), upstreamTunnelPaths...)
	mu.Unlock()
	if len(leaked) > 0 {
		t.Fatalf("tunnel requests leaked to the real Jellyfin: %v", leaked)
	}

	layers := srv.WalkLayers()
	if len(layers) == 0 || layers[0].Kind != "mux" || layers[0].Type != "jellyfin" {
		t.Fatalf("server layers = %+v", layers)
	}
}

func TestMultiuserAuthorizationQuotaAndTelemetry(t *testing.T) {
	registry := testRegistry(t, "alice", "bob")
	cfg := testConfig()
	cfg.Multiuser = true
	cfg.UserID = ""

	srv, err := newServer(cfg, testLogger{}, registry, "")
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.SetSessionHandler(echoStreams)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	front, httpClient := newHTTP2Front(t, srv.Handler())
	base := cfg
	base.URL = front.URL

	aliceCfg := base
	aliceCfg.UserID = "alice"
	_, aliceMux := startMux(t, aliceCfg, testExitNodeID, httpClient, "")

	bobCfg := base
	bobCfg.UserID = "bob"
	bobRaw, bobMux := startMux(t, bobCfg, testExitNodeID, httpClient, "")

	alicePayload := []byte("payload-from-alice")
	bobPayload := []byte("payload-from-bob!!")
	roundTrip(t, ctx, aliceMux, alicePayload)
	roundTrip(t, ctx, bobMux, bobPayload)

	waitUntil(t, 5*time.Second, "traffic accounting", func() bool {
		a, ok := registry.Get("alice")
		if !ok {
			return false
		}
		b, ok := registry.Get("bob")
		if !ok {
			return false
		}
		return a.Usage.UploadBytes+a.Usage.DownloadBytes > 0 &&
			b.Usage.UploadBytes+b.Usage.DownloadBytes > 0
	})

	found := false
	for _, layer := range srv.WalkLayers() {
		if layer.Kind == "pipe" && layer.Name == "alice" && layer.Type == "jellyfin" {
			found = true
		}
	}
	if !found {
		t.Fatalf("per-user telemetry layer is missing: %+v", srv.WalkLayers())
	}

	if err := registry.Upsert(auth.UserSpec{UserID: "bob", Enabled: true, TrafficLimitBytes: 1}); err != nil {
		t.Fatalf("limit bob: %v", err)
	}
	if err := registry.AddTraffic("bob", 4096, 0); err == nil {
		t.Fatal("expected bob to exceed the traffic limit")
	}

	sendCtx, sendCancel := context.WithTimeout(ctx, 5*time.Second)
	err = bobRaw.Send(sendCtx, []byte("blocked"))
	sendCancel()
	if err == nil {
		t.Fatal("quota-exceeded user was not blocked")
	}
	if !errorsIsTransportReset(err) {
		t.Fatalf("blocked send error = %v", err)
	}

	roundTrip(t, ctx, aliceMux, []byte("alice-still-works"))
}

func testRegistry(t *testing.T, ids ...string) *auth.UserRegistry {
	t.Helper()
	registry := auth.NewRegistry()
	for _, id := range ids {
		if err := registry.Upsert(auth.UserSpec{UserID: id, Enabled: true}); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}
	return registry
}

func errorsIsTransportReset(err error) bool {
	return err != nil && (errors.Is(err, transport.ErrTransportReset) ||
		strings.Contains(err.Error(), transport.ErrTransportReset.Error()))
}

func TestBondAggregatesJellyfin(t *testing.T) {
	registry := testRegistry(t, "alice")
	lg := logging.New("jellyfin-bond-test", false)
	cfg := testConfig()
	cfg.Multiuser = true
	cfg.UserID = ""

	srv, err := newServer(cfg, testLogger{}, registry, testPipeSecret)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.SetExitNodeID(testExitNodeID)
	srv.SetSessionHandler(echoStreams)

	aggCfg := tunnel.Config{
		FlushThreshold: 8192,
		FlushInterval:  5 * time.Millisecond,
		Registry:       registry,
	}
	aggregator := bond.NewServer([]string{"0-jellyfin"}, []app.SessionServer{srv}, lg, aggCfg, registry)
	aggregator.SetEncryptionPassword(testPipeSecret)
	aggregator.SetExitNodeID(testExitNodeID)
	aggregator.SetSessionHandler(echoStreams)
	srv.SetTransportSink(aggregator.Sink("0-jellyfin"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := aggregator.Start(ctx); err != nil {
		t.Fatalf("aggregator start: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = aggregator.Stop(stopCtx)
	}()

	front, httpClient := newHTTP2Front(t, srv.Handler())
	clientCfg := cfg
	clientCfg.URL = front.URL
	clientCfg.UserID = "alice"

	raw, err := newClient(clientCfg, testExitNodeID, httpClient, testLogger{})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := raw.Start(ctx); err != nil {
		t.Fatalf("client start: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = raw.Stop(stopCtx)
	}()

	bm, err := bond.NewMux([]transport.Transport{raw}, []string{"0-jellyfin"}, lg)
	if err != nil {
		t.Fatalf("bond mux: %v", err)
	}
	bm.SetIdentity("alice", testExitNodeID)
	bm.SetEncryptionPassword(testPipeSecret)
	if err := bm.Start(ctx); err != nil {
		t.Fatalf("bond mux start: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = bm.Stop(stopCtx)
	}()

	payload := bytes.Repeat([]byte("bond-jellyfin-"), 1024)
	roundTrip(t, ctx, bm, payload)
}

func TestRegisterDescriptorAndLinkCodec(t *testing.T) {
	descriptor, ok := app.DescriptorFor("jellyfin")
	if !ok {
		t.Fatal("jellyfin descriptor is not registered")
	}
	if descriptor.Caps.Client == nil || descriptor.Caps.Server == nil ||
		descriptor.Caps.Multiuser == nil || descriptor.Caps.Link == nil ||
		descriptor.Caps.Secrets == nil || descriptor.Caps.Relay == nil {
		t.Fatalf("capabilities = %+v", descriptor.Caps)
	}
	if descriptor.Defaults["mux_flush_ms"] == nil {
		t.Fatalf("descriptor defaults = %+v", descriptor.Defaults)
	}

	params := app.RouteParams{
		RouteID:    9,
		RouteName:  "route-9",
		UserID:     "alice",
		ExitNodeID: testExitNodeID,
		Domain:     "media.example.com",
		Secrets: map[string]string{
			"jellyfin_secret":     testSecret,
			"encryption_password": testPipeSecret,
		},
	}
	clientSettings, err := buildSubscriptionConfig(params)
	if err != nil {
		t.Fatalf("subscription config: %v", err)
	}
	var clientCfg map[string]any
	if err := json.Unmarshal(clientSettings.Transport, &clientCfg); err != nil {
		t.Fatalf("subscription transport json: %v", err)
	}
	if clientCfg["url"] != "https://media.example.com" ||
		clientCfg["secret"] != testSecret ||
		clientCfg["namespace"] != "route-9" ||
		clientCfg["user_id"] != "alice" {
		t.Fatalf("subscription transport = %+v", clientCfg)
	}
	if clientSettings.UserID != "alice" || clientSettings.ExitNodeID != testExitNodeID {
		t.Fatalf("subscription settings = %+v", clientSettings)
	}

	serverSettings, err := buildServerConfig(params)
	if err != nil {
		t.Fatalf("server config: %v", err)
	}
	var serverCfg map[string]any
	if err := json.Unmarshal(serverSettings.Transport, &serverCfg); err != nil {
		t.Fatalf("server transport json: %v", err)
	}
	if serverCfg["multiuser"] != true ||
		serverCfg["mode"] != modePair ||
		serverCfg["role"] != "exit" {
		t.Fatalf("server transport = %+v", serverCfg)
	}
	if _, ok := serverCfg["listen"]; ok {
		t.Fatalf("pair server config must not listen: %+v", serverCfg)
	}
	if _, ok := serverCfg["upstream_url"]; ok {
		t.Fatalf("pair server config must not proxy upstream: %+v", serverCfg)
	}
	users, ok := serverCfg["users"].([]any)
	if !ok || len(users) != 0 {
		t.Fatalf("server users = %#v", serverCfg["users"])
	}

	link, err := app.EncodeURL(&clientSettings)
	if err != nil {
		t.Fatalf("encode link: %v", err)
	}
	parsed, err := app.ParseURL(link)
	if err != nil {
		t.Fatalf("parse link: %v", err)
	}
	var parsedCfg map[string]any
	if err := json.Unmarshal(parsed.Transport, &parsedCfg); err != nil {
		t.Fatalf("parsed transport: %v", err)
	}
	if parsedCfg["url"] != "https://media.example.com" ||
		parsedCfg["secret"] != testSecret ||
		parsedCfg["namespace"] != "route-9" {
		t.Fatalf("parsed transport = %+v", parsedCfg)
	}
}
func TestRelayAssetRendersJellyfinCompose(t *testing.T) {
	provider := relayProvider{}
	if provider.RelayUpstream() != "jellyfin-broker:8097" {
		t.Fatalf("relay upstream = %q", provider.RelayUpstream())
	}
	if provider.SupportsDecoy() {
		t.Fatal("jellyfin itself is the decoy")
	}
	asset, err := provider.RelayAsset(capability.RelayContext{
		Domain:  "media.example.com",
		Secrets: map[string]string{"jellyfin_secret": testSecret},
	})
	if err != nil {
		t.Fatalf("relay asset: %v", err)
	}
	if asset.Name != "jellyfin-broker" || asset.Upstream != "jellyfin-broker:8097" {
		t.Fatalf("asset = %+v", asset)
	}
	compose := string(asset.Compose)
	for _, want := range []string{
		"jellyfin/jellyfin:latest",
		"JELLYFIN_PublishedServerUrl",
		"https://media.example.com",
		"./jellyfin/config:/config",
		"jellyfin-broker:",
		"shampoore-relay",
		"-jellyfin-listen",
		"-jellyfin-secret",
		testSecret,
		"http://jellyfin:8096",
	} {
		if !strings.Contains(compose, want) {
			t.Fatalf("compose missing %q:\n%s", want, compose)
		}
	}
	if strings.Contains(compose, "encryption_password") {
		t.Fatalf("relay compose must not carry the encryption password:\n%s", compose)
	}

	if _, err := provider.RelayAsset(capability.RelayContext{Domain: "media.example.com"}); err == nil {
		t.Fatal("expected missing jellyfin_secret to fail")
	}
}
func TestProvisionerRendersJellyfinRelayPlan(t *testing.T) {
	node := panelstore.Node{
		ID:      42,
		Name:    "media",
		IsRelay: true,
		GeneratedSecrets: map[string]string{
			"jellyfin_secret": testSecret,
		},
		Transports: []panelstore.Transport{{
			Type:    "jellyfin",
			Domain:  "media.example.com",
			Enabled: true,
		}},
	}
	plan, err := provisioner.RenderRelayPlan(node, provisioner.RenderOptions{})
	if err != nil {
		t.Fatalf("render relay plan: %v", err)
	}
	files := make(map[string]string, len(plan.Files))
	for _, file := range plan.Files {
		files[file.Name] = string(file.Data)
	}
	compose := files["docker-compose.yml"]
	if !strings.Contains(compose, "jellyfin/jellyfin:latest") ||
		!strings.Contains(compose, "JELLYFIN_PublishedServerUrl") ||
		!strings.Contains(compose, "jellyfin-broker:") ||
		!strings.Contains(compose, "shampoore-relay") {
		t.Fatalf("relay compose missing Jellyfin services:\n%s", compose)
	}
	caddy := files["Caddyfile"]
	if !strings.Contains(caddy, "media.example.com") ||
		!strings.Contains(caddy, "reverse_proxy jellyfin-broker:8097") {
		t.Fatalf("Caddyfile missing jellyfin broker upstream:\n%s", caddy)
	}
	if strings.Contains(caddy, "shampoore-server:8097") {
		t.Fatalf("Caddyfile still points at shampoore-server:\n%s", caddy)
	}
}
