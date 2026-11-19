package provisioner

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore"

	_ "github.com/Esteviredzu/shampoore/internal/transport/livekit_datachannel"
	_ "github.com/Esteviredzu/shampoore/internal/transport/raw_ws"
	_ "github.com/Esteviredzu/shampoore/internal/transport/silo"
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
	for _, proxy := range []string{"reverse_proxy relay:8787", "reverse_proxy minio:9000", "reverse_proxy livekit:7880"} {
		if !strings.Contains(caddy, proxy) {
			t.Fatalf("Caddyfile missing %q:\n%s", proxy, caddy)
		}
	}

	var compose map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &compose); err != nil {
		t.Fatalf("compose yaml: %v", err)
	}
	services, _ := compose["services"].(map[string]any)
	for _, name := range []string{"caddy", "relay", "livekit", "minio", "minio-init"} {
		if services[name] == nil {
			t.Fatalf("compose missing service %q", name)
		}
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

func TestMergeRoleFilesForDirectNode(t *testing.T) {
	node := panelstore.Node{
		ID:               5,
		Name:             "direct",
		IsRelay:          true,
		IsExit:           true,
		SocksPort:        1080,
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
	var compose map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &compose); err != nil {
		t.Fatal(err)
	}
	services, _ := compose["services"].(map[string]any)
	for _, name := range []string{"caddy", "relay", "livekit", "minio", "shampoore-server", "node-agent"} {
		if services[name] == nil {
			t.Fatalf("combined compose missing %q", name)
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
		SocksPort:        1080,
		NodeSecret:       "exit-secret",
		GeneratedSecrets: secretsFor("exit"),
	}
	otherExit := panelstore.Node{ID: 3, Name: "exit-2", IsExit: true, SSHHost: "198.51.100.9", SocksPort: 1081}
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
	for _, want := range []string{"shampoore-server", "node-agent", "socks-listen=:1080", "3=socks5://198.51.100.9:1081", "-c", "/opt/shampoore/configs/route-7.json"} {
		if !strings.Contains(compose, want) {
			t.Fatalf("compose missing %q:\n%s", want, compose)
		}
	}
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
