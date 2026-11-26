package jellyfin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multipipe/bond"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

func newRelayBrokerFront(t *testing.T, upstream string) (*Broker, *httptest.Server, *http.Client) {
	t.Helper()
	broker, err := NewBroker(BrokerConfig{
		Secret:      testSecret,
		UpstreamURL: upstream,
		Logger:      testLogger{},
	})
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	front, httpClient := newHTTP2Front(t, broker.Handler())
	t.Cleanup(func() { _ = broker.Stop(context.Background()) })
	return broker, front, httpClient
}

func newExitServer(t *testing.T, url string, registry *auth.UserRegistry, httpClient *http.Client) *MultiServer {
	t.Helper()
	cfg := testConfig()
	cfg.URL = url
	cfg.Role = "exit"
	cfg.Multiuser = true
	muxCfg := tunnel.Config{
		FlushThreshold: 12000,
		FlushInterval:  5 * time.Millisecond,
		Registry:       registry,
	}
	srv := newMultiServer(cfg, testLogger{}, registry, testPipeSecret, muxCfg)
	srv.SetExitNodeID(testExitNodeID)
	srv.httpClient = httpClient
	return srv
}

func startExitServer(t *testing.T, ctx context.Context, srv *MultiServer, handler app.SessionHandler) {
	t.Helper()
	srv.SetSessionHandler(handler)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("exit server start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(stopCtx)
	})
}

func waitForPair(t *testing.T, broker *Broker, sessions int) {
	t.Helper()
	waitUntil(t, 10*time.Second, "pair sessions", func() bool {
		return broker.SessionCount() >= sessions
	})
}

func TestPairE2EClientRelayExit(t *testing.T) {
	var mu sync.Mutex
	var upstreamPaths []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		upstreamPaths = append(upstreamPaths, r.URL.Path)
		mu.Unlock()
		switch r.URL.Path {
		case "/System/Info/Public":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ProductName":"Jellyfin Server"}`)
		default:
			_, _ = io.WriteString(w, "real-jellyfin")
		}
	}))
	defer upstream.Close()

	broker, front, httpClient := newRelayBrokerFront(t, upstream.URL)
	registry := testRegistry(t, "alice")
	exitSrv := newExitServer(t, front.URL, registry, httpClient)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	startExitServer(t, ctx, exitSrv, echoStreams)

	clientCfg := testConfig()
	clientCfg.URL = front.URL
	clientCfg.UserID = "alice"
	clientCfg.Role = "client"
	_, mux := startMux(t, clientCfg, testExitNodeID, httpClient, testPipeSecret)

	waitForPair(t, broker, 2)
	payload := bytes.Repeat([]byte("pair-jellyfin-"), 2048)
	roundTrip(t, ctx, mux, payload)

	resp, err := httpClient.Get(front.URL + "/System/Info/Public")
	if err != nil {
		t.Fatalf("fallback get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "Jellyfin Server") {
		t.Fatalf("fallback response: status=%d body=%q", resp.StatusCode, body)
	}

	mu.Lock()
	leaked := append([]string(nil), upstreamPaths...)
	mu.Unlock()
	for _, path := range leaked {
		if strings.HasPrefix(path, "/Videos/") || strings.HasPrefix(path, "/Sessions/") {
			t.Fatalf("tunnel traffic leaked to the real Jellyfin: %v", leaked)
		}
	}
}

func TestPairE2EUserIsolation(t *testing.T) {
	broker, front, httpClient := newRelayBrokerFront(t, "")
	registry := testRegistry(t, "alice", "bob")
	exitSrv := newExitServer(t, front.URL, registry, httpClient)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	startExitServer(t, ctx, exitSrv, echoStreams)

	base := testConfig()
	base.URL = front.URL
	base.Role = "client"

	aliceCfg := base
	aliceCfg.UserID = "alice"
	_, aliceMux := startMux(t, aliceCfg, testExitNodeID, httpClient, testPipeSecret)

	bobCfg := base
	bobCfg.UserID = "bob"
	_, bobMux := startMux(t, bobCfg, testExitNodeID, httpClient, testPipeSecret)

	waitForPair(t, broker, 4)
	roundTrip(t, ctx, aliceMux, bytes.Repeat([]byte("alice-pair-"), 512))
	roundTrip(t, ctx, bobMux, bytes.Repeat([]byte("bob-pair-"), 512))
}

func TestPairE2EBondAggregatesExit(t *testing.T) {
	broker, front, httpClient := newRelayBrokerFront(t, "")
	registry := testRegistry(t, "alice")
	exitSrv := newExitServer(t, front.URL, registry, httpClient)

	lg := logging.New("jellyfin-pair-bond", false)
	aggCfg := tunnel.Config{
		FlushThreshold: 8192,
		FlushInterval:  5 * time.Millisecond,
		Registry:       registry,
	}
	aggregator := bond.NewServer([]string{"0-jellyfin"}, []app.SessionServer{exitSrv}, lg, aggCfg, registry)
	aggregator.SetEncryptionPassword(testPipeSecret)
	aggregator.SetExitNodeID(testExitNodeID)
	aggregator.SetSessionHandler(echoStreams)
	exitSrv.SetTransportSink(aggregator.Sink("0-jellyfin"))

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

	clientCfg := testConfig()
	clientCfg.URL = front.URL
	clientCfg.UserID = "alice"
	clientCfg.Role = "client"
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

	secured, err := security.WrapTransport(raw, testPipeSecret, security.RoleClient, testLogger{})
	if err != nil {
		t.Fatalf("wrap client: %v", err)
	}
	bm, err := bond.NewMux([]transport.Transport{secured}, []string{"0-jellyfin"}, lg)
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

	waitForPair(t, broker, 2)
	roundTrip(t, ctx, bm, bytes.Repeat([]byte("bond-pair-jellyfin-"), 1024))
}

func TestBrokerListenAndFallback(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "upstream-ok")
	}))
	defer upstream.Close()

	broker, err := NewBroker(BrokerConfig{
		Secret:      testSecret,
		UpstreamURL: upstream.URL,
		Listen:      "127.0.0.1:0",
		Logger:      testLogger{},
	})
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := broker.Start(ctx, "127.0.0.1:0"); err != nil {
		t.Fatalf("broker start: %v", err)
	}
	defer func() { _ = broker.Stop(context.Background()) }()
	addr := broker.Addr()
	if addr == "" {
		t.Fatal("broker did not report its listener address")
	}
	resp, err := http.Get("http://" + addr + "/System/Info/Public")
	if err != nil {
		t.Fatalf("fallback get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "upstream-ok") {
		t.Fatalf("fallback response: status=%d body=%q", resp.StatusCode, body)
	}
}

func TestPairE2ERepeatedClientReconnects(t *testing.T) {
	broker, front, httpClient := newRelayBrokerFront(t, "")
	registry := testRegistry(t, "alice")
	exitSrv := newExitServer(t, front.URL, registry, httpClient)
	exitSrv.muxCfg.PingInterval = 25 * time.Millisecond
	exitSrv.muxCfg.PingTimeout = 300 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	startExitServer(t, ctx, exitSrv, echoStreams)
	waitForPair(t, broker, 1)

	base := testConfig()
	base.URL = front.URL
	base.Role = "client"
	base.UserID = "alice"

	for cycle := 0; cycle < 3; cycle++ {
		exitSrv.mu.Lock()
		session := exitSrv.sessions["alice"]
		exitSrv.mu.Unlock()
		if session == nil {
			t.Fatalf("cycle %d: exit session is missing", cycle)
		}
		before := session.currentMux()

		raw, mux := startMux(t, base, testExitNodeID, httpClient, testPipeSecret)
		waitForPair(t, broker, 2)
		roundTrip(t, ctx, mux, []byte(fmt.Sprintf("jellyfin-cycle-%d", cycle)))

		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := mux.Stop(stopCtx)
		stopCancel()
		if err != nil {
			t.Fatalf("cycle %d: stop client mux: %v", cycle, err)
		}
		_ = raw.Stop(context.Background())

		waitUntil(t, 20*time.Second, "exit session rebuilt after the client drop", func() bool {
			now := session.currentMux()
			return now != nil && now != before
		})
	}
}
