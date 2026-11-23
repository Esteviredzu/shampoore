package silo

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
)

func TestSubscriptionAndServerConfigShareRoutePrefix(t *testing.T) {
	params := app.RouteParams{
		RouteID:    11,
		RouteName:  "s3",
		UserID:     "alice",
		ExitNodeID: "2",
		Domain:     "https://s3.example.com",
		Secrets: map[string]string{
			"s3_bucket":           "shared-bucket",
			"s3_access_key":       "key",
			"s3_secret_key":       "secret",
			"encryption_password": "pw",
		},
	}

	clientSettings, err := buildSubscriptionConfig(params)
	if err != nil {
		t.Fatalf("subscription config: %v", err)
	}
	serverSettings, err := buildServerConfig(params)
	if err != nil {
		t.Fatalf("server config: %v", err)
	}

	var clientCfg, serverCfg Config
	if err := json.Unmarshal(clientSettings.Transport, &clientCfg); err != nil {
		t.Fatalf("decode client transport: %v", err)
	}
	if err := json.Unmarshal(serverSettings.Transport, &serverCfg); err != nil {
		t.Fatalf("decode server transport: %v", err)
	}

	if clientCfg.TunnelID != "route-11" || serverCfg.TunnelID != "route-11" {
		t.Fatalf("tunnel ids = %q/%q, want route-11", clientCfg.TunnelID, serverCfg.TunnelID)
	}
	if clientCfg.UserID != "alice" {
		t.Fatalf("client user = %q, want alice", clientCfg.UserID)
	}
	if !serverCfg.Multiuser || serverCfg.UserID != "" {
		t.Fatalf("server cfg multiuser=%v user=%q", serverCfg.Multiuser, serverCfg.UserID)
	}

	clientWrite, clientRead := buildPrefixes(&clientCfg, "client")

	serverCfg.UserID = "alice"
	serverWrite, serverRead := buildPrefixes(&serverCfg, "server")

	if clientWrite != "route-11/users/alice/c2s/" || clientRead != "route-11/users/alice/s2c/" {
		t.Fatalf("client prefixes = %q/%q", clientWrite, clientRead)
	}
	if clientWrite != serverRead || clientRead != serverWrite {
		t.Fatalf("client/server prefixes diverge: client %q/%q server %q/%q",
			clientWrite, clientRead, serverWrite, serverRead)
	}
}

func newRoutePipePair(t *testing.T, store *fakeStore, namespace string) (client, server *Pipe) {
	t.Helper()
	cfg := testSiloConfig("shared-bucket", "alice")
	cfg.TunnelID = namespace
	client, err := newPipeWithStore(&cfg, "client", testLogger{}, store)
	if err != nil {
		t.Fatalf("%s client pipe: %v", namespace, err)
	}
	server, err = newPipeWithStore(&cfg, "server", testLogger{}, store)
	if err != nil {
		t.Fatalf("%s server pipe: %v", namespace, err)
	}
	return client, server
}

func TestRouteNamespacesIsolateObjects(t *testing.T) {
	store := newFakeStore("shared-bucket")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	singleClient, singleServer := newRoutePipePair(t, store, "route-11")
	multiClient, multiServer := newRoutePipePair(t, store, "route-22")
	for _, pipe := range []*Pipe{singleClient, singleServer, multiClient, multiServer} {
		if err := pipe.Start(ctx); err != nil {
			t.Fatalf("start pipe: %v", err)
		}
		pipe := pipe
		t.Cleanup(func() { _ = pipe.Stop(context.Background()) })
	}

	if err := singleClient.SendMessage(ctx, []byte("from-single"), ""); err != nil {
		t.Fatalf("single send: %v", err)
	}
	if err := multiClient.SendMessage(ctx, []byte("from-multi"), ""); err != nil {
		t.Fatalf("multi send: %v", err)
	}

	if got := receiveWithTimeout(t, singleServer, 5*time.Second); string(got) != "from-single" {
		t.Fatalf("single server received %q", got)
	}
	if got := receiveWithTimeout(t, multiServer, 5*time.Second); string(got) != "from-multi" {
		t.Fatalf("multi server received %q", got)
	}

	extraCtx, extraCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer extraCancel()
	if msg, err := singleServer.ReceiveMessage(extraCtx); err == nil {
		t.Fatalf("single namespace received a foreign message: %q", msg)
	}

	if store.hasPrefix("users/alice/") {
		t.Fatal("legacy unscoped users/<id>/ prefix was used")
	}
	for _, namespace := range []string{"route-11", "route-22"} {
		if !store.recorded(namespace + "/users/alice/c2s/") {
			t.Fatalf("no c2s objects under %s/users/alice/", namespace)
		}
	}
}

func TestMultiServerIgnoresForeignRouteNamespace(t *testing.T) {
	store := newFakeStore("shared-bucket")
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{UserID: "alice", Enabled: true}); err != nil {
		t.Fatalf("registry: %v", err)
	}

	cfg := testSiloConfig("shared-bucket", "")
	cfg.TunnelID = "route-33"
	cfg.Multiuser = true

	server := newMultiServer(cfg, testLogger{}, registry, "", store, app.Values{
		"mux_flush_ms":        5,
		"mux_flush_threshold": 12000,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = server.Stop(stopCtx)
	})
	waitUntil(t, 5*time.Second, "alice session", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return server.sessions["alice"] != nil
	})

	batch := appendBatchMessage(nil, []byte("frame"))
	ownKey := C2SPrefix("route-33", "alice") + "msg_own"
	foreignKey := C2SPrefix("route-22", "alice") + "msg_foreign"
	store.putObject(ownKey, batch, time.Now())
	store.putObject(foreignKey, batch, time.Now())

	waitUntil(t, 5*time.Second, "own object consumption", func() bool {
		return !store.hasObject(ownKey)
	})
	time.Sleep(300 * time.Millisecond)
	if !store.hasObject(foreignKey) {
		t.Fatalf("route-33 server consumed route-22 object %q", foreignKey)
	}
}

func TestSiloURLKeepsRouteNamespace(t *testing.T) {
	descriptor, ok := app.DescriptorFor("silo")
	if !ok || descriptor.EncodeURL == nil || descriptor.DecodeURL == nil {
		t.Fatal("silo descriptor is missing URL codecs")
	}
	raw, err := json.Marshal(Config{
		Endpoint:  "s3.example.com",
		Bucket:    "shared-bucket",
		AccessKey: "key",
		SecretKey: "secret",
		UseSSL:    true,
		TunnelID:  "route-11",
		UserID:    "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	q := url.Values{}
	if err := descriptor.EncodeURL(raw, q); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if q.Get("tunnel_id") != "route-11" {
		t.Fatalf("encoded tunnel_id = %q", q.Get("tunnel_id"))
	}
	decoded, err := descriptor.DecodeURL(q)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var cfg Config
	if err := json.Unmarshal(decoded, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.TunnelID != "route-11" || cfg.UserID != "alice" {
		t.Fatalf("decoded namespace = %q/%q, want route-11/alice", cfg.TunnelID, cfg.UserID)
	}
}
