package provisioner

import (
	"context"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

func decoyRelayNode(repoURL string) panelstore.Node {
	return panelstore.Node{
		ID:               1,
		Name:             "relay-decoy",
		SSHHost:          "203.0.113.1",
		SSHUser:          "root",
		SSHPort:          22,
		IsRelay:          true,
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", DecoyRepoURL: repoURL, Enabled: true},
		},
	}
}

func TestDecoyFilesPlaceholderWithoutLink(t *testing.T) {
	p := New(&fakeStore{}, Options{})
	files, err := p.decoyFiles(context.Background(), decoyRelayNode(""))
	if err != nil {
		t.Fatalf("decoy files: %v", err)
	}
	if len(files) != 1 || files[0].Name != decoyPlaceholder || len(files[0].Data) != 0 {
		t.Fatalf("placeholder files = %+v", files)
	}

	files, err = p.decoyFiles(context.Background(), panelstore.Node{ID: 2, Name: "exit", IsExit: true})
	if err != nil || files != nil {
		t.Fatalf("exit decoy files = %+v, err=%v", files, err)
	}

	disabled := decoyRelayNode("https://github.com/owner/repo")
	disabled.Transports[0].Enabled = false
	files, err = p.decoyFiles(context.Background(), disabled)
	if err != nil {
		t.Fatalf("disabled decoy files: %v", err)
	}
	if len(files) != 1 || files[0].Name != decoyPlaceholder {
		t.Fatalf("disabled placeholder files = %+v", files)
	}
}

func TestDecoyFilesDownloadsSite(t *testing.T) {
	zipData := buildZipArchive(t, map[string]string{
		"repo-main/index.html": "<html>site</html>",
		"repo-main/app.js":     "console.log('x')",
	})
	srv := newDecoyHTTPServer(t, zipData, nil)

	p := New(&fakeStore{}, Options{HTTPClient: srv.Client()})
	files, err := p.decoyFiles(context.Background(), decoyRelayNode(srv.URL+"/owner/repo"))
	if err != nil {
		t.Fatalf("decoy files: %v", err)
	}
	got := fileMap(files)
	if len(got) != 2 {
		t.Fatalf("files = %+v", got)
	}
	if _, ok := got["decoy/index.html"]; !ok {
		t.Fatalf("missing decoy/index.html in %+v", got)
	}
	if _, ok := got["decoy/app.js"]; !ok {
		t.Fatalf("missing decoy/app.js in %+v", got)
	}
}

func TestDeployNodeCleansOldDecoySite(t *testing.T) {
	zipData := buildZipArchive(t, map[string]string{
		"repo-main/index.html": "<html>site</html>",
	})
	srv := newDecoyHTTPServer(t, zipData, nil)

	relay := decoyRelayNode(srv.URL + "/owner/repo")
	store := &fakeStore{nodes: []panelstore.Node{relay}}
	remote := &fakeRemote{failOnce: map[string]bool{}}
	p := New(store, Options{
		LocalBinDir: writeFakeBinaries(t),
		HTTPClient:  srv.Client(),
		Dial:        func(context.Context, panelstore.Node) (Remote, error) { return remote, nil },
	})
	result, err := p.DeployNode(context.Background(), relay)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !result.Success {
		t.Fatalf("deploy result: %+v", result)
	}

	cleaned := false
	for _, cmd := range remote.commands {
		if strings.Contains(cmd, "rm -rf /opt/shampoore/decoy") {
			cleaned = true
		}
	}
	if !cleaned {
		t.Fatalf("deploy did not clean /opt/shampoore/decoy: %v", remote.commands)
	}

	uploaded := fileMap(remote.uploads)
	if _, ok := uploaded["decoy/index.html"]; !ok {
		t.Fatalf("decoy site was not uploaded: %+v", uploaded)
	}
}

func TestDeployNodeUploadsDecoyPlaceholderWithoutLink(t *testing.T) {
	relay := decoyRelayNode("")
	store := &fakeStore{nodes: []panelstore.Node{relay}}
	remote := &fakeRemote{failOnce: map[string]bool{}}
	p := New(store, Options{
		LocalBinDir: writeFakeBinaries(t),
		Dial:        func(context.Context, panelstore.Node) (Remote, error) { return remote, nil },
	})
	if _, err := p.DeployNode(context.Background(), relay); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	uploaded := fileMap(remote.uploads)
	if _, ok := uploaded[decoyPlaceholder]; !ok {
		t.Fatalf("decoy placeholder was not uploaded: %+v", uploaded)
	}
	if _, ok := uploaded["decoy/index.html"]; ok {
		t.Fatalf("unexpected decoy site upload: %+v", uploaded)
	}
}

func TestRenderRelayMountsDecoyDir(t *testing.T) {
	files, err := RenderRelay(decoyRelayNode(""), RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m := fileMap(files)
	compose := string(m["docker-compose.yml"].Data)
	for _, want := range []string{"./decoy:/opt/shampoore/decoy:ro", "-decoy-dir", "/opt/shampoore/decoy"} {
		if !strings.Contains(compose, want) {
			t.Fatalf("compose missing %q:\n%s", want, compose)
		}
	}

	var parsed map[string]any
	if err := yaml.Unmarshal(m["docker-compose.yml"].Data, &parsed); err != nil {
		t.Fatalf("compose yaml: %v", err)
	}
	services, _ := parsed["services"].(map[string]any)
	relay, _ := services["relay"].(map[string]any)
	command, _ := relay["command"].([]any)
	var args []string
	for _, item := range command {
		args = append(args, strings.TrimSpace(item.(string)))
	}
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-decoy-dir /opt/shampoore/decoy") {
		t.Fatalf("relay command = %q", joined)
	}

	caddy := string(m["Caddyfile"].Data)
	if !strings.Contains(caddy, "reverse_proxy relay:8787") || !strings.Contains(caddy, "ws.example.com") {
		t.Fatalf("Caddyfile must proxy raw_ws domain to relay:8787:\n%s", caddy)
	}
}
