package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type topology struct {
	relay  panelstore.Node
	exit   panelstore.Node
	routes []panelstore.Route
	user   panelstore.User
}

func seedTopology(t *testing.T, store *panelstore.Store) topology {
	t.Helper()
	ctx := context.Background()
	relay, err := store.CreateNode(ctx, panelstore.Node{
		Name:    "relay",
		SSHHost: "203.0.113.10",
		IsRelay: true,
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
			{Type: panelstore.TransportSilo, Domain: "s3.example.com", Enabled: true},
			{Type: panelstore.TransportLiveKit, Domain: "lk.example.com", Enabled: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	exit, err := store.CreateNode(ctx, panelstore.Node{
		Name:    "exit",
		SSHHost: "198.51.100.20",
		IsExit:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var routes []panelstore.Route
	for _, kind := range []string{panelstore.TransportRawWS, panelstore.TransportSilo, panelstore.TransportLiveKit} {
		r, err := store.CreateRoute(ctx, panelstore.Route{
			Name:          "route-" + kind,
			EntryNodeID:   relay.ID,
			ExitNodeID:    exit.ID,
			TransportType: kind,
			Enabled:       true,
		})
		if err != nil {
			t.Fatalf("route %s: %v", kind, err)
		}
		routes = append(routes, r)
	}
	user, err := store.CreateUser(ctx, panelstore.User{Name: "Alice", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateSettings(ctx, panelstore.Settings{PublicDomain: "sub.example.com"}); err != nil {
		t.Fatalf("enable subscriptions: %v", err)
	}
	return topology{relay: relay, exit: exit, routes: routes, user: user}
}

func TestPublicHandlerIsolation(t *testing.T) {
	api, _, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, api.store)
	public := api.PublicHandler()

	for _, path := range []string{
		"/api/nodes", "/api/nodes/1", "/api/routes", "/api/users",
		"/api/configs", "/api/stats", "/api/system/stats", "/login", "/", "/static/app.js",
	} {
		rec := perform(public, http.MethodGet, path, "", authRequest())
		if rec.Code != http.StatusNotFound {
			t.Fatalf("public %s = %d, want 404 (%s)", path, rec.Code, rec.Body.String())
		}
	}
	rec := perform(public, http.MethodPost, "/api/node/sync", `{}`, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("public sync without secret = %d, want 401", rec.Code)
	}
	rec = perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("public subscription = %d: %s", rec.Code, rec.Body.String())
	}
}

func TestSubscriptionGeneration(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	public := api.PublicHandler()

	rec := perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("subscription = %d: %s", rec.Code, rec.Body.String())
	}
	var settings []app.Settings
	if err := json.Unmarshal(rec.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	if len(settings) != 3 {
		t.Fatalf("settings = %d, want 3", len(settings))
	}
	byType := make(map[string]app.Settings, len(settings))
	for _, s := range settings {
		byType[s.Type] = s
		if s.UserID != topo.user.ID {
			t.Fatalf("user_id = %q", s.UserID)
		}
		if s.ExitNodeID != itoa(topo.exit.ID) {
			t.Fatalf("exit_node_id = %q", s.ExitNodeID)
		}
	}
	var rawTransport map[string]any
	_ = json.Unmarshal(byType["raw_ws"].Transport, &rawTransport)
	if rawTransport["server_url"] != "wss://ws.example.com" {
		t.Fatalf("raw_ws server_url = %v", rawTransport["server_url"])
	}
	if rawTransport["password"] != topo.relay.GeneratedSecrets["raw_ws_password"] {
		t.Fatalf("raw_ws password = %v", rawTransport["password"])
	}
	var siloTransport map[string]any
	_ = json.Unmarshal(byType["silo"].Transport, &siloTransport)
	if siloTransport["endpoint"] != "s3.example.com" {
		t.Fatalf("silo endpoint = %v", siloTransport["endpoint"])
	}
	var lkTransport map[string]any
	_ = json.Unmarshal(byType["livekit_datachannel"].Transport, &lkTransport)
	if lkTransport["signal_url"] != "wss://lk.example.com" {
		t.Fatalf("livekit signal_url = %v", lkTransport["signal_url"])
	}

	topo.user.AllowedRouteIDs = []int64{topo.routes[0].ID}
	if _, err := store.UpdateUser(context.Background(), topo.user); err != nil {
		t.Fatal(err)
	}
	rec = perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	var limited []app.Settings
	_ = json.Unmarshal(rec.Body.Bytes(), &limited)
	if len(limited) != 1 || limited[0].Type != "raw_ws" {
		t.Fatalf("restricted settings = %+v", limited)
	}
}
