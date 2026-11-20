package panel

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	_ "github.com/Esteviredzu/shampoore/internal/multipipe"
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
	_ = relay

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
	if _, leaked := lkTransport["api_secret"]; !leaked {
		t.Fatalf("livekit subscription is missing api_secret: %v", lkTransport)
	}
	if lkTransport["api_key"] != relay.GeneratedSecrets["livekit_api_key"] {
		t.Fatalf("livekit api_key = %v", lkTransport["api_key"])
	}
	for _, removed := range []string{"token_url", "token_secret", "token_listen"} {
		if _, ok := lkTransport[removed]; ok {
			t.Fatalf("livekit subscription leaked removed field %s: %v", removed, lkTransport)
		}
	}
	if lkTransport["user_id"] != topo.user.ID {
		t.Fatalf("livekit user_id = %v", lkTransport["user_id"])
	}
	if lkTransport["room_prefix"] != "route-"+routeID(panelstore.TransportLiveKit) {
		t.Fatalf("livekit room_prefix = %v", lkTransport["room_prefix"])
	}
}

func TestSubscriptionLiveKitMultiuserMode(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)

	rec := perform(api.PublicHandler(), http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("subscription = %d: %s", rec.Code, rec.Body.String())
	}
	var settings []app.Settings
	if err := json.Unmarshal(rec.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	var livekit *app.Settings
	for i := range settings {
		if settings[i].Type == "livekit_datachannel" {
			livekit = &settings[i]
			break
		}
	}
	if livekit == nil {
		t.Fatal("livekit subscription settings not found")
	}
	var tr map[string]any
	if err := json.Unmarshal(livekit.Transport, &tr); err != nil {
		t.Fatal(err)
	}
	relay, err := store.GetNode(context.Background(), topo.relay.ID)
	if err != nil {
		t.Fatal(err)
	}
	routeID := ""
	for _, route := range topo.routes {
		if route.TransportType == panelstore.TransportLiveKit {
			routeID = itoa(route.ID)
			break
		}
	}
	if routeID == "" {
		t.Fatal("livekit route not found")
	}
	if tr["room"] != "route-"+routeID+"-"+topo.user.ID {
		t.Fatalf("livekit room = %v", tr["room"])
	}
	if tr["room_prefix"] != "route-"+routeID {
		t.Fatalf("livekit room_prefix = %v", tr["room_prefix"])
	}
	if tr["api_secret"] != relay.GeneratedSecrets["livekit_api_secret"] {
		t.Fatalf("livekit api_secret = %v", tr["api_secret"])
	}
	if _, ok := tr["token_url"]; ok {
		t.Fatalf("livekit subscription leaked token_url: %v", tr)
	}
	if _, ok := tr["token_secret"]; ok {
		t.Fatalf("livekit subscription leaked token_secret: %v", tr)
	}
}

func TestSubscriptionToggleAndProfileTitle(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	public := api.PublicHandler()
	h := api.Handler()

	rec := perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("subscription = %d: %s", rec.Code, rec.Body.String())
	}
	wantTitle := "base64:" + base64.StdEncoding.EncodeToString([]byte(topo.user.Name))
	if got := rec.Header().Get("Profile-Title"); got != wantTitle {
		t.Fatalf("Profile-Title = %q, want %q", got, wantTitle)
	}

	rec = perform(h, http.MethodPut, "/api/settings", `{"subscriptions_enabled":false}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("disable subscriptions = %d: %s", rec.Code, rec.Body.String())
	}
	settings, err := store.GetSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if settings.SubscriptionsEnabled {
		t.Fatal("subscriptions were not disabled")
	}
	if settings.PublicDomain != "sub.example.com" {
		t.Fatalf("public domain = %q, want it preserved", settings.PublicDomain)
	}
	rec = perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("disabled subscription = %d, want 404", rec.Code)
	}

	rec = perform(h, http.MethodPut, "/api/settings",
		`{"public_domain":"sub.example.com","subscriptions_enabled":true}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("enable subscriptions = %d: %s", rec.Code, rec.Body.String())
	}
	rec = perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("re-enabled subscription = %d: %s", rec.Code, rec.Body.String())
	}

	rec = perform(h, http.MethodDelete, "/api/settings/domain", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("delete domain = %d: %s", rec.Code, rec.Body.String())
	}
	settings, err = store.GetSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if settings.PublicDomain != "" || settings.SubscriptionsEnabled {
		t.Fatalf("settings after domain delete = %+v", settings)
	}
	rec = perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("subscription without domain = %d, want 404", rec.Code)
	}
}

func TestUserOfflineBundle(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	h := api.Handler()

	rec := perform(h, http.MethodGet, "/api/users/"+topo.user.ID+"/bundle", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("bundle = %d: %s", rec.Code, rec.Body.String())
	}
	var bundle OfflineBundle
	if err := json.Unmarshal(rec.Body.Bytes(), &bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.Name != topo.user.Name {
		t.Fatalf("bundle name = %q, want %q", bundle.Name, topo.user.Name)
	}
	if len(bundle.Links) != len(topo.routes) {
		t.Fatalf("bundle links = %d, want %d: %+v", len(bundle.Links), len(topo.routes), bundle)
	}
	for _, link := range bundle.Links {
		if !strings.HasPrefix(link, "shampoore://") {
			t.Fatalf("bundle link %q is not a shampoore:// URL", link)
		}
		parsed, err := app.ParseURL(link)
		if err != nil {
			t.Fatalf("parse bundle link %q: %v", link, err)
		}
		if parsed.UserID != topo.user.ID || parsed.ExitNodeID != itoa(topo.exit.ID) {
			t.Fatalf("bundle link identity = %q/%q", parsed.UserID, parsed.ExitNodeID)
		}
	}

	topo.user.AllowedRouteIDs = []int64{topo.routes[0].ID}
	if _, err := store.UpdateUser(context.Background(), topo.user); err != nil {
		t.Fatal(err)
	}
	rec = perform(h, http.MethodGet, "/api/users/"+topo.user.ID+"/bundle", "", authRequest())
	var limited OfflineBundle
	if err := json.Unmarshal(rec.Body.Bytes(), &limited); err != nil {
		t.Fatal(err)
	}
	if len(limited.Links) != 1 {
		t.Fatalf("restricted bundle links = %d, want 1", len(limited.Links))
	}
}

func TestSubscriptionMultiRoutes(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	ctx := context.Background()

	bond, err := store.CreateRoute(ctx, panelstore.Route{
		Name:          "bond-route",
		TransportType: panelstore.TransportMulti,
		MultiStrategy: panelstore.MultiStrategyBond,
		SubRouteIDs:   []int64{topo.routes[0].ID, topo.routes[1].ID},
		Enabled:       true,
	})
	if err != nil {
		t.Fatalf("create bond route: %v", err)
	}
	flow, err := store.CreateRoute(ctx, panelstore.Route{
		Name:          "flowmux-route",
		TransportType: panelstore.TransportMulti,
		MultiStrategy: panelstore.MultiStrategyFlowMux,
		SubRouteIDs:   []int64{topo.routes[1].ID, topo.routes[2].ID},
		Enabled:       true,
	})
	if err != nil {
		t.Fatalf("create flowmux route: %v", err)
	}
	relay, err := store.GetNode(ctx, topo.relay.ID)
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
	if len(settings) != len(topo.routes)+2 {
		t.Fatalf("settings = %d, want %d", len(settings), len(topo.routes)+2)
	}
	byName := make(map[string]app.Settings, len(settings))
	for _, s := range settings {
		byName[s.Name] = s
	}

	type multiChild struct {
		Type      string          `json:"type"`
		Transport json.RawMessage `json:"transport"`
	}
	type multiConfig struct {
		Strategy   string       `json:"strategy"`
		Transports []multiChild `json:"transports"`
	}
	cases := []struct {
		name      string
		strategy  string
		wantTypes []string
	}{
		{"bond-route", panelstore.MultiStrategyBond, []string{"raw_ws", "silo"}},
		{"flowmux-route", panelstore.MultiStrategyFlowMux, []string{"silo", "livekit_datachannel"}},
	}
	for _, tc := range cases {
		s, ok := byName[tc.name]
		if !ok {
			t.Fatalf("multi settings %q not found in %+v", tc.name, byName)
		}
		if s.Type != panelstore.TransportMulti {
			t.Fatalf("%s type = %q", tc.name, s.Type)
		}
		if s.UserID != topo.user.ID || s.ExitNodeID != itoa(topo.exit.ID) {
			t.Fatalf("%s identity = %q/%q", tc.name, s.UserID, s.ExitNodeID)
		}
		if s.EncryptionPassword != relay.GeneratedSecrets["encryption_password"] {
			t.Fatalf("%s encryption password = %q", tc.name, s.EncryptionPassword)
		}
		var cfg multiConfig
		if err := json.Unmarshal(s.Transport, &cfg); err != nil {
			t.Fatalf("%s transport: %v", tc.name, err)
		}
		if cfg.Strategy != tc.strategy {
			t.Fatalf("%s strategy = %q, want %q", tc.name, cfg.Strategy, tc.strategy)
		}
		if len(cfg.Transports) != len(tc.wantTypes) {
			t.Fatalf("%s transports = %d, want %d", tc.name, len(cfg.Transports), len(tc.wantTypes))
		}
		for i, want := range tc.wantTypes {
			if cfg.Transports[i].Type != want {
				t.Fatalf("%s transport %d = %q, want %q", tc.name, i, cfg.Transports[i].Type, want)
			}
		}
		if tc.strategy == panelstore.MultiStrategyBond {
			var raw map[string]any
			if err := json.Unmarshal(cfg.Transports[0].Transport, &raw); err != nil {
				t.Fatal(err)
			}
			if raw["password"] != relay.GeneratedSecrets["raw_ws_password"] {
				t.Fatalf("bond raw_ws password = %v", raw["password"])
			}
		}
	}

	rec = perform(api.Handler(), http.MethodGet, "/api/users/"+topo.user.ID+"/bundle", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("bundle = %d: %s", rec.Code, rec.Body.String())
	}
	var bundle OfflineBundle
	if err := json.Unmarshal(rec.Body.Bytes(), &bundle); err != nil {
		t.Fatal(err)
	}
	if len(bundle.Links) != len(topo.routes)+2 {
		t.Fatalf("bundle links = %d, want %d: %+v", len(bundle.Links), len(topo.routes)+2, bundle)
	}
	multiLinks := 0
	for _, link := range bundle.Links {
		if !strings.HasPrefix(link, "shampoore://") {
			t.Fatalf("bundle link %q is not a shampoore:// URL", link)
		}
		parsed, err := app.ParseURL(link)
		if err != nil {
			t.Fatalf("parse bundle link %q: %v", link, err)
		}
		if parsed.Type != panelstore.TransportMulti {
			continue
		}
		multiLinks++
		if parsed.UserID != topo.user.ID || parsed.ExitNodeID != itoa(topo.exit.ID) {
			t.Fatalf("multi link identity = %q/%q", parsed.UserID, parsed.ExitNodeID)
		}
		if parsed.EncryptionPassword != relay.GeneratedSecrets["encryption_password"] {
			t.Fatalf("multi link encryption password = %q", parsed.EncryptionPassword)
		}
		var cfg multiConfig
		if err := json.Unmarshal(parsed.Transport, &cfg); err != nil {
			t.Fatalf("parse multi transport: %v", err)
		}
		if cfg.Strategy != panelstore.MultiStrategyBond && cfg.Strategy != panelstore.MultiStrategyFlowMux {
			t.Fatalf("multi link strategy = %q", cfg.Strategy)
		}
		if len(cfg.Transports) != 2 {
			t.Fatalf("multi link transports = %d, want 2", len(cfg.Transports))
		}
	}
	if multiLinks != 2 {
		t.Fatalf("multi links = %d, want 2 (bond=%d flowmux=%d)", multiLinks, bond.ID, flow.ID)
	}
}
