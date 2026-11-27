package provisioner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore"

	_ "github.com/Esteviredzu/shampoore/internal/transport/all"
	"github.com/Esteviredzu/shampoore/internal/transport/silo"
)

func secretsFor(name string) map[string]string {
	return map[string]string{
		"encryption_password": "enc-" + name,
		"raw_ws_password":     "ws-" + name,
		"livekit_api_key":     "key-" + name,
		"livekit_api_secret":  "secret-" + name,
		"minio_root_user":     "minio-" + name,
		"minio_root_password": "minio-pass-" + name,
		"s3_access_key":       "s3-key-" + name,
		"s3_secret_key":       "s3-secret-" + name,
		"s3_bucket":           "bucket-" + name,
		"jellyfin_secret":     "jellyfin-" + name,
		"tunnel_id":           "shampoore",
	}
}

func fileMap(files []File) map[string]File {
	out := make(map[string]File, len(files))
	for _, f := range files {
		out[f.Name] = f
	}
	return out
}

func assertNoSOCKSExposure(t *testing.T, composeData []byte) {
	t.Helper()
	text := string(composeData)
	for _, forbidden := range []string{"--socks-listen", "--exit-upstream", "1080:1080"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("exit compose must not contain %q:\n%s", forbidden, text)
		}
	}
	var compose struct {
		Services map[string]struct {
			Ports   []string `yaml:"ports"`
			Command []string `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(composeData, &compose); err != nil {
		t.Fatalf("compose yaml: %v", err)
	}
	server, ok := compose.Services["shampoore-server"]
	if !ok {
		t.Fatalf("compose has no shampoore-server service:\n%s", text)
	}
	for _, port := range server.Ports {
		if strings.Contains(port, "1080") {
			t.Fatalf("exit compose publishes SOCKS5 port %q:\n%s", port, text)
		}
	}
	for _, arg := range server.Command {
		if strings.HasPrefix(arg, "--socks-listen") || strings.HasPrefix(arg, "--exit-upstream") {
			t.Fatalf("exit command still passes %q:\n%s", arg, text)
		}
	}
}

func TestRenderRelay(t *testing.T) {
	node := panelstore.Node{
		ID:               1,
		Name:             "relay-de",
		IsRelay:          true,
		NodeSecret:       "node-secret",
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
			{Type: panelstore.TransportSilo, Domain: "s3.example.com", Enabled: true},
			{Type: panelstore.TransportLiveKit, Domain: "lk.example.com", Enabled: true},
		},
	}
	files, err := RenderRelay(node, RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(files)
	for _, name := range []string{"docker-compose.yml", "Caddyfile", "livekit.yaml"} {
		if _, ok := m[name]; !ok {
			t.Fatalf("missing %s in %v", name, m)
		}
	}
	caddy := string(m["Caddyfile"].Data)
	for _, domain := range []string{"ws.example.com", "s3.example.com", "lk.example.com"} {
		if !strings.Contains(caddy, domain) {
			t.Fatalf("Caddyfile missing %s:\n%s", domain, caddy)
		}
	}
	for _, proxy := range []string{"reverse_proxy relay:8787", "reverse_proxy silo:9000", "reverse_proxy livekit:7880"} {
		if !strings.Contains(caddy, proxy) {
			t.Fatalf("Caddyfile missing %q:\n%s", proxy, caddy)
		}
	}

	var compose map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &compose); err != nil {
		t.Fatalf("compose yaml: %v", err)
	}
	services, _ := compose["services"].(map[string]any)
	for _, name := range []string{"caddy", "relay", "livekit", "silo"} {
		if services[name] == nil {
			t.Fatalf("compose missing service %q", name)
		}
	}
	for _, name := range []string{"minio", "minio-init"} {
		if services[name] != nil {
			t.Fatalf("compose still contains legacy service %q", name)
		}
	}
	relayService, _ := services["relay"].(map[string]any)
	if relayService["image"] != "debian:bookworm-slim" {
		t.Fatalf("relay image = %v, want debian:bookworm-slim", relayService["image"])
	}
	siloService, _ := services["silo"].(map[string]any)
	if siloService["image"] != "docker.io/pgsty/silo:RELEASE.2026-10-26T00-00-00Z" {
		t.Fatalf("silo image = %v", siloService["image"])
	}
	siloEnv, _ := siloService["environment"].(map[string]any)
	if siloEnv["MINIO_SERVER_URL"] != "https://s3.example.com" {
		t.Fatalf("MINIO_SERVER_URL = %v", siloEnv["MINIO_SERVER_URL"])
	}
	if !hasComposeVolume(siloService, "./silo-data:/data") {
		t.Fatalf("silo volumes = %v", siloService["volumes"])
	}
	if strings.Contains(string(m["docker-compose.yml"].Data), "{{") {
		t.Fatal("unrendered template markers in compose")
	}
	if !strings.Contains(string(m["docker-compose.yml"].Data), "ws-relay") &&
		!strings.Contains(string(m["docker-compose.yml"].Data), "shampoore-relay") {
		t.Fatal("relay service binary is missing from compose")
	}

	var livekit map[string]any
	if err := yaml.Unmarshal(m["livekit.yaml"].Data, &livekit); err != nil {
		t.Fatalf("livekit yaml: %v", err)
	}
	keys, _ := livekit["keys"].(map[string]any)
	if keys["key-relay"] != "secret-relay" {
		t.Fatalf("livekit keys = %+v", keys)
	}
}

func hasComposeVolume(service map[string]any, want string) bool {
	volumes, _ := service["volumes"].([]any)
	for _, volume := range volumes {
		if volume == want {
			return true
		}
	}
	return false
}

func TestRenderRelayLiveKitOnly(t *testing.T) {
	node := panelstore.Node{
		ID:               2,
		Name:             "relay-livekit",
		IsRelay:          true,
		GeneratedSecrets: secretsFor("livekit"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportLiveKit, Domain: "lk.example.com", Enabled: true},
		},
	}
	files, err := RenderRelay(node, RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(files)
	var compose map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &compose); err != nil {
		t.Fatalf("compose yaml: %v", err)
	}
	services, _ := compose["services"].(map[string]any)
	if len(services) != 2 {
		t.Fatalf("services = %v, want only caddy and livekit", services)
	}
	for _, name := range []string{"caddy", "livekit"} {
		if services[name] == nil {
			t.Fatalf("compose missing service %q", name)
		}
	}
	for _, name := range []string{"relay", "silo", "minio", "minio-init"} {
		if services[name] != nil {
			t.Fatalf("livekit-only compose contains unexpected service %q", name)
		}
	}
	caddyService, _ := services["caddy"].(map[string]any)
	deps, _ := caddyService["depends_on"].([]any)
	if len(deps) != 1 || deps[0] != "livekit" {
		t.Fatalf("caddy depends_on = %v, want [livekit]", deps)
	}
	caddy := string(m["Caddyfile"].Data)
	if !strings.Contains(caddy, "reverse_proxy livekit:7880") {
		t.Fatalf("Caddyfile missing livekit upstream:\n%s", caddy)
	}
	if strings.Contains(caddy, "relay:8787") || strings.Contains(caddy, "silo:9000") {
		t.Fatalf("Caddyfile contains disabled upstreams:\n%s", caddy)
	}
}

func TestRenderRelaySiloOnly(t *testing.T) {
	node := panelstore.Node{
		ID:               3,
		Name:             "relay-silo",
		IsRelay:          true,
		GeneratedSecrets: secretsFor("silo"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportSilo, Domain: "s3.example.com", Enabled: true},
		},
	}
	files, err := RenderRelay(node, RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(files)
	var compose map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &compose); err != nil {
		t.Fatalf("compose yaml: %v", err)
	}
	services, _ := compose["services"].(map[string]any)
	if len(services) != 2 {
		t.Fatalf("services = %v, want only caddy and silo", services)
	}
	for _, name := range []string{"caddy", "silo"} {
		if services[name] == nil {
			t.Fatalf("compose missing service %q", name)
		}
	}
	for _, name := range []string{"relay", "livekit", "minio", "minio-init"} {
		if services[name] != nil {
			t.Fatalf("silo-only compose contains unexpected service %q", name)
		}
	}
	siloService, _ := services["silo"].(map[string]any)
	if siloService["image"] != "docker.io/pgsty/silo:RELEASE.2026-10-26T00-00-00Z" {
		t.Fatalf("silo image = %v", siloService["image"])
	}
	caddy := string(m["Caddyfile"].Data)
	if !strings.Contains(caddy, "reverse_proxy silo:9000") {
		t.Fatalf("Caddyfile missing silo upstream:\n%s", caddy)
	}
	if strings.Contains(caddy, "minio:9000") || strings.Contains(caddy, "relay:8787") {
		t.Fatalf("Caddyfile contains unexpected upstreams:\n%s", caddy)
	}
}

func TestMergeRoleFilesForDirectNode(t *testing.T) {
	node := panelstore.Node{
		ID:               5,
		Name:             "direct",
		IsRelay:          true,
		IsExit:           true,
		NodeSecret:       "direct-secret",
		GeneratedSecrets: secretsFor("direct"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
		},
	}
	route := panelstore.Route{
		ID:            9,
		Name:          "direct-route",
		EntryNodeID:   node.ID,
		ExitNodeID:    node.ID,
		TransportType: panelstore.TransportRawWS,
		Enabled:       true,
	}
	relayFiles, err := RenderRelay(node, RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	exitFiles, err := RenderExit(node, []panelstore.Route{route}, []panelstore.Node{node}, RenderOptions{PanelURL: "https://panel.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	merged, err := mergeRoleFiles(relayFiles, exitFiles)
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(merged)
	assertNoSOCKSExposure(t, m["docker-compose.yml"].Data)
	var compose map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &compose); err != nil {
		t.Fatal(err)
	}
	services, _ := compose["services"].(map[string]any)
	for _, name := range []string{"caddy", "relay", "shampoore-server", "node-agent"} {
		if services[name] == nil {
			t.Fatalf("combined compose missing %q", name)
		}
	}
	for _, name := range []string{"livekit", "silo", "minio", "minio-init"} {
		if services[name] != nil {
			t.Fatalf("combined compose contains unexpected %q", name)
		}
	}
	if _, ok := m["configs/route-9.json"]; !ok {
		t.Fatal("combined output missing exit config")
	}
}

func TestRenderExit(t *testing.T) {
	relay := panelstore.Node{
		ID:               1,
		Name:             "relay",
		IsRelay:          true,
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
		},
	}
	exit := panelstore.Node{
		ID:               2,
		Name:             "exit",
		IsExit:           true,
		NodeSecret:       "exit-secret",
		GeneratedSecrets: secretsFor("exit"),
	}
	otherExit := panelstore.Node{ID: 3, Name: "exit-2", IsExit: true, SSHHost: "198.51.100.9"}
	routes := []panelstore.Route{{
		ID:            7,
		Name:          "route-7",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: panelstore.TransportRawWS,
		Enabled:       true,
	}}
	files, err := RenderExit(exit, routes, []panelstore.Node{relay, exit, otherExit}, RenderOptions{PanelURL: "https://panel.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(files)
	compose := string(m["docker-compose.yml"].Data)
	for _, want := range []string{"shampoore-server", "node-agent", "-c", "/opt/shampoore/configs/route-7.json", "./rules:/opt/shampoore/rules:ro", "--rules-dir=/opt/shampoore/rules"} {
		if !strings.Contains(compose, want) {
			t.Fatalf("compose missing %q:\n%s", want, compose)
		}
	}
	assertNoSOCKSExposure(t, m["docker-compose.yml"].Data)
	var exitCompose map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &exitCompose); err != nil {
		t.Fatalf("exit compose yaml: %v", err)
	}
	exitServices, _ := exitCompose["services"].(map[string]any)
	if exitServices["shampoore-server"] == nil || exitServices["node-agent"] == nil {
		t.Fatalf("exit compose services = %+v", exitServices)
	}
	env := string(m["agent.env"].Data)
	for _, want := range []string{"PANEL_URL=https://panel.example.com", "NODE_ID=2", "NODE_SECRET=exit-secret"} {
		if !strings.Contains(env, want) {
			t.Fatalf("agent.env missing %q:\n%s", want, env)
		}
	}
	cfgFile, ok := m["configs/route-7.json"]
	if !ok {
		t.Fatalf("missing configs/route-7.json: %v", m)
	}
	if cfgFile.Mode != 0o600 {
		t.Fatalf("config mode = %o, want 600", cfgFile.Mode)
	}
	var settings app.Settings
	if err := json.Unmarshal(cfgFile.Data, &settings); err != nil {
		t.Fatalf("config json: %v\n%s", err, cfgFile.Data)
	}
	if settings.Type != "raw_ws" {
		t.Fatalf("type = %q", settings.Type)
	}
	var tr map[string]any
	if err := json.Unmarshal(settings.Transport, &tr); err != nil {
		t.Fatal(err)
	}
	if tr["multiuser"] != true {
		t.Fatalf("multiuser = %v", tr["multiuser"])
	}
	if tr["server_url"] != "wss://ws.example.com" {
		t.Fatalf("server_url = %v", tr["server_url"])
	}
	if users, ok := tr["users"].([]any); !ok || len(users) != 0 {
		t.Fatalf("users = %#v, want empty array", tr["users"])
	}
	if _, err := RenderExit(exit, routes, []panelstore.Node{relay, exit}, RenderOptions{}); err == nil {
		t.Fatal("RenderExit accepted an empty panel URL")
	}
}

func TestExitComposeDoesNotExposeSOCKS5(t *testing.T) {
	relay := panelstore.Node{
		ID:               1,
		Name:             "relay",
		IsRelay:          true,
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
		},
	}
	exit := panelstore.Node{
		ID:               2,
		Name:             "exit",
		IsExit:           true,
		NodeSecret:       "exit-secret",
		GeneratedSecrets: secretsFor("exit"),
	}
	route := panelstore.Route{
		ID:            7,
		Name:          "route-7",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: panelstore.TransportRawWS,
		Enabled:       true,
	}

	t.Run("exit", func(t *testing.T) {
		files, err := RenderExit(exit, []panelstore.Route{route}, []panelstore.Node{relay, exit},
			RenderOptions{PanelURL: "https://panel.example.com"})
		if err != nil {
			t.Fatalf("render exit: %v", err)
		}
		assertNoSOCKSExposure(t, fileMap(files)["docker-compose.yml"].Data)
	})

	t.Run("relay_exit", func(t *testing.T) {
		combined := relay
		combined.IsExit = true
		combined.NodeSecret = "combined-secret"
		combinedRoute := route
		combinedRoute.EntryNodeID = combined.ID
		combinedRoute.ExitNodeID = combined.ID

		relayFiles, err := RenderRelay(combined, RenderOptions{})
		if err != nil {
			t.Fatalf("render relay: %v", err)
		}
		exitFiles, err := RenderExit(combined, []panelstore.Route{combinedRoute}, []panelstore.Node{combined},
			RenderOptions{PanelURL: "https://panel.example.com"})
		if err != nil {
			t.Fatalf("render exit: %v", err)
		}
		merged, err := mergeRoleFiles(relayFiles, exitFiles)
		if err != nil {
			t.Fatalf("merge roles: %v", err)
		}
		assertNoSOCKSExposure(t, fileMap(merged)["docker-compose.yml"].Data)
	})
}

func TestRenderExitMultiBondAggregatesSubflows(t *testing.T) {
	relay := panelstore.Node{
		ID:               1,
		Name:             "relay",
		IsRelay:          true,
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
			{Type: panelstore.TransportSilo, Domain: "s3.example.com", Enabled: true},
		},
	}
	exit := panelstore.Node{
		ID:               2,
		Name:             "exit",
		IsExit:           true,
		NodeSecret:       "exit-secret",
		GeneratedSecrets: secretsFor("exit"),
	}
	childWS := panelstore.Route{
		ID: 7, Name: "route-raw", EntryNodeID: relay.ID, ExitNodeID: exit.ID,
		TransportType: panelstore.TransportRawWS, Enabled: true,
	}
	childSilo := panelstore.Route{
		ID: 8, Name: "route-silo", EntryNodeID: relay.ID, ExitNodeID: exit.ID,
		TransportType: panelstore.TransportSilo, Enabled: true,
	}
	multi := panelstore.Route{
		ID: 9, Name: "bond-route", EntryNodeID: relay.ID, ExitNodeID: exit.ID,
		TransportType: panelstore.TransportMulti, MultiStrategy: panelstore.MultiStrategyBond,
		SubRouteIDs: []int64{childWS.ID, childSilo.ID}, Enabled: true,
	}

	files, err := RenderExit(exit, []panelstore.Route{childWS, childSilo, multi},
		[]panelstore.Node{relay, exit}, RenderOptions{PanelURL: "https://panel.example.com"})
	if err != nil {
		t.Fatalf("render exit: %v", err)
	}
	m := fileMap(files)
	file, ok := m["configs/route-9.json"]
	if !ok {
		t.Fatalf("multi config was not rendered: %v", keysOf(m))
	}

	var settings app.Settings
	if err := json.Unmarshal(file.Data, &settings); err != nil {
		t.Fatalf("decode multi settings: %v", err)
	}
	if settings.Type != panelstore.TransportMulti {
		t.Fatalf("multi type = %q", settings.Type)
	}
	if settings.EncryptionPassword != secretsFor("relay")["encryption_password"] {
		t.Fatalf("multi password = %q", settings.EncryptionPassword)
	}

	var payload struct {
		Strategy   string `json:"strategy"`
		Transports []struct {
			Type      string          `json:"type"`
			Transport json.RawMessage `json:"transport"`
		} `json:"transports"`
	}
	if err := json.Unmarshal(settings.Transport, &payload); err != nil {
		t.Fatalf("decode multi transport: %v", err)
	}
	if payload.Strategy != panelstore.MultiStrategyBond {
		t.Fatalf("strategy = %q", payload.Strategy)
	}
	if len(payload.Transports) != 2 {
		t.Fatalf("subflows = %d, want 2", len(payload.Transports))
	}
	types := []string{payload.Transports[0].Type, payload.Transports[1].Type}
	if types[0] != panelstore.TransportRawWS || types[1] != panelstore.TransportSilo {
		t.Fatalf("subflow types = %v", types)
	}

	var rawCfg map[string]any
	if err := json.Unmarshal(payload.Transports[0].Transport, &rawCfg); err != nil {
		t.Fatalf("decode raw_ws options: %v", err)
	}
	if rawCfg["room"] != "route-9" {
		t.Fatalf("raw_ws room = %v, want route-9", rawCfg["room"])
	}
	if rawCfg["multiuser"] != true {
		t.Fatalf("raw_ws server config is not multiuser: %v", rawCfg["multiuser"])
	}

	var siloCfg map[string]any
	if err := json.Unmarshal(payload.Transports[1].Transport, &siloCfg); err != nil {
		t.Fatalf("decode silo options: %v", err)
	}
	if siloCfg["tunnel_id"] != "route-9" {
		t.Fatalf("silo tunnel_id = %v, want route-9", siloCfg["tunnel_id"])
	}
}

func keysOf(m map[string]File) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestRenderExitLiveKitSubscriptionServerConfig(t *testing.T) {
	relay := panelstore.Node{
		ID:               1,
		Name:             "relay",
		IsRelay:          true,
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportLiveKit, Domain: "lk.example.com", Enabled: true},
		},
	}
	exit := panelstore.Node{
		ID:               2,
		Name:             "exit",
		IsExit:           true,
		NodeSecret:       "exit-secret",
		GeneratedSecrets: secretsFor("exit"),
	}
	routes := []panelstore.Route{{
		ID:            9,
		Name:          "route-9",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: panelstore.TransportLiveKit,
		Enabled:       true,
	}}
	files, err := RenderExit(exit, routes, []panelstore.Node{relay, exit}, RenderOptions{PanelURL: "https://panel.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	cfgFile, ok := fileMap(files)["configs/route-9.json"]
	if !ok {
		t.Fatal("missing livekit exit config")
	}
	var settings app.Settings
	if err := json.Unmarshal(cfgFile.Data, &settings); err != nil {
		t.Fatal(err)
	}
	var tr map[string]any
	if err := json.Unmarshal(settings.Transport, &tr); err != nil {
		t.Fatal(err)
	}
	if tr["multiuser"] != true {
		t.Fatalf("multiuser = %v", tr["multiuser"])
	}
	if tr["room_prefix"] != "route-9" {
		t.Fatalf("room_prefix = %v", tr["room_prefix"])
	}
	if _, ok := tr["token_listen"]; ok {
		t.Fatalf("subscription config has token_listen: %v", tr)
	}
	if _, ok := tr["token_secret"]; ok {
		t.Fatalf("subscription config has token_secret: %v", tr)
	}
}

func TestRenderRelayLocalOmitsCaddyAndUsesLoopbackPorts(t *testing.T) {
	node := panelstore.Node{
		ID:               11,
		Name:             "panel-host",
		IsRelay:          true,
		GeneratedSecrets: secretsFor("local"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
			{Type: panelstore.TransportSilo, Domain: "s3.example.com", Enabled: true},
			{Type: panelstore.TransportLiveKit, Domain: "lk.example.com", Enabled: true},
		},
	}
	files, err := RenderRelay(node, RenderOptions{LocalRelay: true})
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(files)
	var compose map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &compose); err != nil {
		t.Fatalf("compose yaml: %v", err)
	}
	services, _ := compose["services"].(map[string]any)
	if services["caddy"] != nil {
		t.Fatal("local relay must not start a containerized Caddy")
	}
	for _, name := range []string{"relay", "livekit", "silo"} {
		if services[name] == nil {
			t.Fatalf("compose missing service %q", name)
		}
	}
	raw := string(m["docker-compose.yml"].Data)
	for _, port := range []string{"127.0.0.1:8787:8787", "127.0.0.1:7880:7880", "127.0.0.1:9000:9000"} {
		if !strings.Contains(raw, port) {
			t.Fatalf("local compose missing loopback port %q:\n%s", port, raw)
		}
	}
	if strings.Contains(raw, "80:80") || strings.Contains(raw, "443:443") {
		t.Fatalf("local compose must not bind 80/443:\n%s", raw)
	}
}

func TestExitRuleFilesSelectsAssignedActiveRules(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"a.txt":        "1.1.1.0/24\n",
		"b.txt":        "2.2.2.0/24\n",
		"global.txt":   "3.3.3.0/24\n",
		"disabled.txt": "4.4.4.0/24\n",
		"rules.state.json": `{
  "a": true,
  "b": true,
  "global": true,
  "disabled": false
}
`,
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store := &fakeStore{bindings: map[string][]int64{
		"a": []int64{2},
		"b": []int64{3},
	}}
	p := New(store, Options{RulesDir: dir})
	out, err := p.exitRuleFiles(context.Background(), panelstore.Node{ID: 2, Name: "exit", IsExit: true})
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(out)
	for _, want := range []string{"rules/a.txt", "rules/global.txt", "rules/rules.state.json", "rules/.keep"} {
		if _, ok := m[want]; !ok {
			t.Fatalf("archive missing %q: %v", want, m)
		}
	}
	for _, unexpected := range []string{"rules/b.txt", "rules/disabled.txt"} {
		if _, ok := m[unexpected]; ok {
			t.Fatalf("archive contains %q: %v", unexpected, m)
		}
	}
	var state map[string]bool
	if err := json.Unmarshal(m["rules/rules.state.json"].Data, &state); err != nil {
		t.Fatal(err)
	}
	if !state["a"] || !state["global"] || state["b"] || state["disabled"] || len(state) != 2 {
		t.Fatalf("shipped rule state = %+v", state)
	}
}

func TestRenderExitAppendsRuleFiles(t *testing.T) {
	exit := panelstore.Node{ID: 2, Name: "exit", IsExit: true}
	ruleFile := File{Name: "rules/a.txt", Mode: 0o644, Data: []byte("1.1.1.0/24\n")}
	files, err := RenderExit(exit, nil, []panelstore.Node{exit}, RenderOptions{
		PanelURL:  "https://panel.example.com",
		RuleFiles: []File{ruleFile},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := fileMap(files)["rules/a.txt"]
	if !ok || string(got.Data) != "1.1.1.0/24\n" {
		t.Fatalf("rule file = %+v, ok=%v", got, ok)
	}
}

func TestRenderExitSiloNamespacesStayIsolated(t *testing.T) {
	relay := panelstore.Node{
		ID:               1,
		Name:             "relay",
		IsRelay:          true,
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
			{Type: panelstore.TransportSilo, Domain: "s3.example.com", Enabled: true},
		},
	}
	exit := panelstore.Node{
		ID:               2,
		Name:             "exit",
		IsExit:           true,
		NodeSecret:       "exit-secret",
		GeneratedSecrets: secretsFor("exit"),
	}
	childWS := panelstore.Route{
		ID: 7, Name: "route-raw", EntryNodeID: relay.ID, ExitNodeID: exit.ID,
		TransportType: panelstore.TransportRawWS, Enabled: true,
	}
	singleSilo := panelstore.Route{
		ID: 8, Name: "s3", EntryNodeID: relay.ID, ExitNodeID: exit.ID,
		TransportType: panelstore.TransportSilo, Enabled: true,
	}
	multi := panelstore.Route{
		ID: 9, Name: "bond", EntryNodeID: relay.ID, ExitNodeID: exit.ID,
		TransportType: panelstore.TransportMulti, MultiStrategy: panelstore.MultiStrategyBond,
		SubRouteIDs: []int64{childWS.ID, singleSilo.ID}, Enabled: true,
	}

	files, err := RenderExit(exit, []panelstore.Route{childWS, singleSilo, multi},
		[]panelstore.Node{relay, exit}, RenderOptions{PanelURL: "https://panel.example.com"})
	if err != nil {
		t.Fatalf("render exit: %v", err)
	}
	m := fileMap(files)

	decode := func(name string) app.Settings {
		t.Helper()
		file, ok := m[name]
		if !ok {
			t.Fatalf("missing %s in %v", name, keysOf(m))
		}
		var settings app.Settings
		if err := json.Unmarshal(file.Data, &settings); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		return settings
	}

	singleCfg := decode("configs/route-8.json")
	if singleCfg.Type != panelstore.TransportSilo {
		t.Fatalf("route-8 type = %q", singleCfg.Type)
	}
	var singleRaw map[string]any
	if err := json.Unmarshal(singleCfg.Transport, &singleRaw); err != nil {
		t.Fatalf("decode route-8 silo options: %v", err)
	}
	if singleRaw["tunnel_id"] != "route-8" {
		t.Fatalf("route-8 tunnel_id = %v, want route-8", singleRaw["tunnel_id"])
	}
	if singleRaw["multiuser"] != true {
		t.Fatalf("route-8 multiuser = %v", singleRaw["multiuser"])
	}

	multiCfg := decode("configs/route-9.json")
	var payload siloMultiPayload
	if err := json.Unmarshal(multiCfg.Transport, &payload); err != nil {
		t.Fatalf("decode route-9 transport: %v", err)
	}
	childTunnelID := ""
	for _, child := range payload.Transports {
		if child.Type != panelstore.TransportSilo {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(child.Transport, &raw); err != nil {
			t.Fatalf("decode bond silo options: %v", err)
		}
		childTunnelID, _ = raw["tunnel_id"].(string)
		if raw["multiuser"] != true {
			t.Fatalf("bond silo multiuser = %v", raw["multiuser"])
		}
	}
	if childTunnelID != "route-9" {
		t.Fatalf("bond silo tunnel_id = %q, want route-9", childTunnelID)
	}

	singlePrefix := silo.C2SPrefix(singleRaw["tunnel_id"].(string), "alice")
	childPrefix := silo.C2SPrefix(childTunnelID, "alice")
	if singlePrefix != "route-8/users/alice/c2s/" {
		t.Fatalf("single silo prefix = %q", singlePrefix)
	}
	if childPrefix != "route-9/users/alice/c2s/" {
		t.Fatalf("bond silo prefix = %q", childPrefix)
	}
	if singlePrefix == childPrefix {
		t.Fatalf("silo prefixes collide: %q", singlePrefix)
	}
}

type siloMultiPayload struct {
	Strategy   string `json:"strategy"`
	Transports []struct {
		Type      string          `json:"type"`
		Transport json.RawMessage `json:"transport"`
	} `json:"transports"`
}

func TestRenderRelayJellyfinOnly(t *testing.T) {
	node := panelstore.Node{
		ID:               43,
		Name:             "media",
		IsRelay:          true,
		GeneratedSecrets: secretsFor("media"),
		Transports: []panelstore.Transport{
			{Type: "jellyfin", Domain: "media.example.com", Enabled: true},
		},
	}
	files, err := RenderRelay(node, RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(files)
	compose := string(m["docker-compose.yml"].Data)
	for _, want := range []string{
		"jellyfin/jellyfin:latest",
		"jellyfin-broker:",
		"shampoore-relay",
		"-jellyfin-listen",
		"-jellyfin-secret",
		"jellyfin-media",
		"http://jellyfin:8096",
	} {
		if !strings.Contains(compose, want) {
			t.Fatalf("relay compose missing %q:\n%s", want, compose)
		}
	}
	var parsed map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &parsed); err != nil {
		t.Fatalf("relay compose yaml: %v", err)
	}
	services, _ := parsed["services"].(map[string]any)
	if services["jellyfin"] == nil || services["jellyfin-broker"] == nil {
		t.Fatalf("relay compose services = %+v", services)
	}
	caddy := string(m["Caddyfile"].Data)
	if !strings.Contains(caddy, "media.example.com") ||
		!strings.Contains(caddy, "reverse_proxy jellyfin-broker:8097") {
		t.Fatalf("Caddyfile missing jellyfin broker upstream:\n%s", caddy)
	}

	localFiles, err := RenderRelay(node, RenderOptions{LocalRelay: true})
	if err != nil {
		t.Fatal(err)
	}
	local := fileMap(localFiles)
	localRaw := string(local["docker-compose.yml"].Data)
	for _, want := range []string{"127.0.0.1:8097:8097", "127.0.0.1:8096:8096"} {
		if !strings.Contains(localRaw, want) {
			t.Fatalf("local relay compose missing %q:\n%s", want, localRaw)
		}
	}
	if strings.Contains(localRaw, "caddy:") {
		t.Fatalf("local relay must not start containerized caddy:\n%s", localRaw)
	}
}

func TestRenderExitJellyfinPairConfig(t *testing.T) {
	relay := panelstore.Node{
		ID:               4,
		Name:             "relay",
		IsRelay:          true,
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: "jellyfin", Domain: "media.example.com", Enabled: true},
		},
	}
	exit := panelstore.Node{
		ID:               5,
		Name:             "exit",
		IsExit:           true,
		NodeSecret:       "exit-secret",
		GeneratedSecrets: secretsFor("exit"),
	}
	routes := []panelstore.Route{{
		ID:            9,
		Name:          "route-9",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: "jellyfin",
		Enabled:       true,
	}}
	files, err := RenderExit(exit, routes, []panelstore.Node{relay, exit}, RenderOptions{PanelURL: "https://panel.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(files)
	cfgFile, ok := m["configs/route-9.json"]
	if !ok {
		t.Fatalf("missing exit config: %v", m)
	}
	var settings app.Settings
	if err := json.Unmarshal(cfgFile.Data, &settings); err != nil {
		t.Fatalf("config json: %v\n%s", err, cfgFile.Data)
	}
	if settings.Type != "jellyfin" {
		t.Fatalf("type = %q", settings.Type)
	}
	if settings.EncryptionPassword != "enc-relay" {
		t.Fatalf("encryption password = %q", settings.EncryptionPassword)
	}
	var tr map[string]any
	if err := json.Unmarshal(settings.Transport, &tr); err != nil {
		t.Fatal(err)
	}
	if tr["mode"] != "pair" || tr["role"] != "exit" || tr["multiuser"] != true {
		t.Fatalf("transport = %+v", tr)
	}
	if tr["url"] != "https://media.example.com" || tr["namespace"] != "route-9" {
		t.Fatalf("transport = %+v", tr)
	}
	if _, ok := tr["listen"]; ok {
		t.Fatalf("pair exit must not listen: %+v", tr)
	}
	if _, ok := tr["upstream_url"]; ok {
		t.Fatalf("pair exit must not proxy upstream: %+v", tr)
	}
	if users, ok := tr["users"].([]any); !ok || len(users) != 0 {
		t.Fatalf("users = %#v, want empty array", tr["users"])
	}
}
func TestRenderRelayIncludesNodeAgentHeartbeat(t *testing.T) {
	node := panelstore.Node{
		ID:               11,
		Name:             "relay-agent",
		IsRelay:          true,
		NodeSecret:       "relay-secret",
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
		},
	}
	files, err := RenderRelay(node, RenderOptions{PanelURL: "https://panel.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(files)
	var compose map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &compose); err != nil {
		t.Fatalf("compose yaml: %v\n%s", err, m["docker-compose.yml"].Data)
	}
	services, _ := compose["services"].(map[string]any)
	agent, ok := services["node-agent"].(map[string]any)
	if !ok {
		t.Fatalf("relay compose has no node-agent service:\n%s", m["docker-compose.yml"].Data)
	}
	envFile, _ := agent["env_file"].([]any)
	if len(envFile) != 1 || envFile[0] != "./agent.env" {
		t.Fatalf("node-agent env_file = %v", agent["env_file"])
	}
	command, _ := agent["command"].([]any)
	if len(command) != 1 || command[0] != "/opt/shampoore/bin/shampoore-node-agent" {
		t.Fatalf("node-agent command = %v", agent["command"])
	}
	env, ok := m["agent.env"]
	if !ok {
		t.Fatalf("relay render has no agent.env: %v", m)
	}
	for _, want := range []string{
		"PANEL_URL=https://panel.example.com",
		"NODE_ID=11",
		"NODE_SECRET=relay-secret",
		"INTERVAL=2s",
	} {
		if !strings.Contains(string(env.Data), want) {
			t.Fatalf("agent.env missing %q:\n%s", want, env.Data)
		}
	}

	plain, err := RenderRelay(node, RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	plainMap := fileMap(plain)
	var plainCompose map[string]any
	if err := yaml.Unmarshal(plainMap["docker-compose.yml"].Data, &plainCompose); err != nil {
		t.Fatalf("compose yaml: %v", err)
	}
	plainServices, _ := plainCompose["services"].(map[string]any)
	if plainServices["node-agent"] != nil {
		t.Fatal("node-agent must be omitted without a panel URL")
	}
	if _, ok := plainMap["agent.env"]; ok {
		t.Fatal("agent.env must be omitted without a panel URL")
	}
}
