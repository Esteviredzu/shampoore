package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

func TestSubscriptionRejectsExpiredAndOverLimit(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	public := api.PublicHandler()

	topo.user.ExpiresAt = time.Now().Add(-time.Hour)
	if _, err := store.UpdateUser(context.Background(), topo.user); err != nil {
		t.Fatal(err)
	}
	rec := perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expired subscription = %d, want 403", rec.Code)
	}

	topo.user.ExpiresAt = time.Time{}
	topo.user.TrafficLimitBytes = 100
	if _, err := store.UpdateUser(context.Background(), topo.user); err != nil {
		t.Fatal(err)
	}
	if err := store.AddTraffic(context.Background(), []panelstore.TrafficDelta{{
		UserID: topo.user.ID, UploadBytes: 60, DownloadBytes: 50,
	}}); err != nil {
		t.Fatal(err)
	}
	rec = perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("over-limit subscription = %d, want 403", rec.Code)
	}
	rec = perform(public, http.MethodGet, "/sub/deadbeefdeadbeefdeadbeefdeadbeef", "", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown subscription = %d, want 404", rec.Code)
	}
}

func TestSubscriptionConcurrent(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	public := api.PublicHandler()
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
			if rec.Code != http.StatusOK {
				t.Errorf("concurrent subscription = %d", rec.Code)
			}
		}()
	}
	wg.Wait()
}

func TestPublicHandlerNoStore(t *testing.T) {
	api, _, _, _ := newControlTestAPI(t)
	api.store = nil
	public := api.PublicHandler()
	rec := perform(public, http.MethodGet, "/sub/aaaaaaaaaaaaaaaa", "", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("no store = %d, want 503", rec.Code)
	}
	rec = perform(public, http.MethodGet, "/api/nodes", "", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("public admin route without store = %d, want 404", rec.Code)
	}
}

func TestSubscriptionModularTransports(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)

	relay, err := store.GetNode(context.Background(), topo.relay.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i := range relay.Transports {
		if relay.Transports[i].Type == panelstore.TransportLiveKit {
			relay.Transports[i].OnDemand = true
		}
	}
	relay, err = store.UpdateNode(context.Background(), relay)
	if err != nil {
		t.Fatal(err)
	}

	rec := perform(api.PublicHandler(), http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("subscription = %d: %s", rec.Code, rec.Body.String())
	}
	var settings []app.Settings
	if err := json.Unmarshal(rec.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	if len(settings) != len(topo.routes) {
		t.Fatalf("settings = %d, want %d", len(settings), len(topo.routes))
	}
	byType := make(map[string]app.Settings, len(settings))
	for _, s := range settings {
		byType[s.Type] = s
		if s.UserID != topo.user.ID || s.ExitNodeID != itoa(topo.exit.ID) {
			t.Fatalf("identity = %q/%q", s.UserID, s.ExitNodeID)
		}
	}
	routeID := func(kind string) string {
		for _, route := range topo.routes {
			if route.TransportType == kind {
				return itoa(route.ID)
			}
		}
		t.Fatalf("route for %s not found", kind)
		return ""
	}

	var rawTransport map[string]any
	_ = json.Unmarshal(byType["raw_ws"].Transport, &rawTransport)
	if rawTransport["server_url"] != "wss://ws.example.com" {
		t.Fatalf("raw_ws server_url = %v", rawTransport["server_url"])
	}
	if rawTransport["room"] != "route-"+routeID(panelstore.TransportRawWS) {
		t.Fatalf("raw_ws room = %v", rawTransport["room"])
	}
	if rawTransport["password"] != relay.GeneratedSecrets["raw_ws_password"] {
		t.Fatalf("raw_ws password = %v", rawTransport["password"])
	}
	if rawTransport["batch"] != true || rawTransport["padding"] != true {
		t.Fatalf("raw_ws batching = %v/%v", rawTransport["batch"], rawTransport["padding"])
	}

	var siloTransport map[string]any
	_ = json.Unmarshal(byType["silo"].Transport, &siloTransport)
	if siloTransport["endpoint"] != "s3.example.com" {
		t.Fatalf("silo endpoint = %v", siloTransport["endpoint"])
	}
	if siloTransport["tunnel_id"] != "route-"+routeID(panelstore.TransportSilo) {
		t.Fatalf("silo tunnel_id = %v", siloTransport["tunnel_id"])
	}
	if siloTransport["access_key"] != relay.GeneratedSecrets["s3_access_key"] {
		t.Fatalf("silo access_key = %v", siloTransport["access_key"])
	}

	var lkTransport map[string]any
	_ = json.Unmarshal(byType["livekit_datachannel"].Transport, &lkTransport)
	if lkTransport["signal_url"] != "wss://lk.example.com" {
		t.Fatalf("livekit signal_url = %v", lkTransport["signal_url"])
	}
	if _, leaked := lkTransport["api_secret"]; leaked {
		t.Fatalf("livekit on-demand subscription leaked api_secret: %v", lkTransport)
	}
	if lkTransport["api_key"] != relay.GeneratedSecrets["livekit_api_key"] {
		t.Fatalf("livekit api_key = %v", lkTransport["api_key"])
	}
	if lkTransport["token_url"] != "https://lk.example.com/session" {
		t.Fatalf("livekit token_url = %v", lkTransport["token_url"])
	}
	if lkTransport["token_secret"] != relay.GeneratedSecrets["livekit_token_secret"] {
		t.Fatalf("livekit token_secret = %v", lkTransport["token_secret"])
	}
	if lkTransport["user_id"] != topo.user.ID {
		t.Fatalf("livekit user_id = %v", lkTransport["user_id"])
	}
	if lkTransport["room_prefix"] != "shampoore-route-"+routeID(panelstore.TransportLiveKit) {
		t.Fatalf("livekit room_prefix = %v", lkTransport["room_prefix"])
	}
}
