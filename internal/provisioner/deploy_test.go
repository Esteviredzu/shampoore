package provisioner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type fakeStore struct {
	nodes  []panelstore.Node
	routes []panelstore.Route
}

func (f *fakeStore) GetNode(_ context.Context, id int64) (panelstore.Node, error) {
	for _, n := range f.nodes {
		if n.ID == id {
			return n, nil
		}
	}
	return panelstore.Node{}, panelstore.ErrNotFound
}

func (f *fakeStore) ListNode(context.Context) ([]panelstore.Node, error) { return f.nodes, nil }
func (f *fakeStore) ListRoutes(context.Context) ([]panelstore.Route, error) {
	return f.routes, nil
}
func (f *fakeStore) ListRoutesForExit(_ context.Context, exitNodeID int64) ([]panelstore.Route, error) {
	out := []panelstore.Route{}
	for _, r := range f.routes {
		if r.ExitNodeID == exitNodeID {
			out = append(out, r)
		}
	}
	return out, nil
}

type fakeRemote struct {
	commands []string
	uploads  []File
	dir      string
	failOnce map[string]bool
}

func (f *fakeRemote) Run(_ context.Context, cmd string) (string, error) {
	f.commands = append(f.commands, cmd)
	if f.failOnce[cmd] {
		delete(f.failOnce, cmd)
		return "", os.ErrDeadlineExceeded
	}
	switch {
	case cmd == "id -u":
		return "0\n", nil
	case strings.Contains(cmd, "docker --version"):
		return "Docker version 27.0.0\n", nil
	case strings.Contains(cmd, "compose version"):
		return "Docker Compose version v2.29.0\n", nil
	default:
		return "ok\n", nil
	}
}

func (f *fakeRemote) Upload(_ context.Context, dir string, files []File, _ bool) error {
	f.dir = dir
	f.uploads = append(f.uploads, files...)
	return nil
}

func (f *fakeRemote) Close() error { return nil }

func writeFakeBinaries(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"shampoore-server", "shampoore-node-agent", "shampoore-relay"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("binary-"+name), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func exitFixture() (panelstore.Node, panelstore.Node, panelstore.Route) {
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
		SSHHost:          "198.51.100.2",
		SSHUser:          "root",
		SSHPort:          22,
		IsExit:           true,
		SocksPort:        1080,
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
	return relay, exit, route
}

func TestDeployNodeOverFakeSSH(t *testing.T) {
	relay, exit, route := exitFixture()
	store := &fakeStore{nodes: []panelstore.Node{relay, exit}, routes: []panelstore.Route{route}}
	remote := &fakeRemote{}
	p := New(store, Options{
		PanelURL:    "https://panel.example.com",
		LocalBinDir: writeFakeBinaries(t),
		Dial: func(context.Context, panelstore.Node) (Remote, error) {
			return remote, nil
		},
	})
	result, err := p.DeployNode(context.Background(), exit)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !result.Success {
		t.Fatalf("deploy failed: %+v", result)
	}
	if remote.dir != "/opt/shampoore" {
		t.Fatalf("upload dir = %q", remote.dir)
	}
	joined := ""
	for _, f := range remote.uploads {
		joined += f.Name + "\n"
	}
	for _, want := range []string{"docker-compose.yml", "agent.env", "configs/route-7.json", "bin/shampoore-server", "bin/shampoore-node-agent"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("uploads missing %q:\n%s", want, joined)
		}
	}
	foundUp := false
	for _, cmd := range remote.commands {
		if strings.Contains(cmd, "up -d") {
			foundUp = true
		}
	}
	if !foundUp {
		t.Fatalf("compose up was not executed: %v", remote.commands)
	}
}

func TestDeployNodeInstallsDocker(t *testing.T) {
	relay := panelstore.Node{
		ID:               1,
		Name:             "relay",
		SSHHost:          "203.0.113.1",
		SSHUser:          "root",
		SSHPort:          22,
		IsRelay:          true,
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
		},
	}
	store := &fakeStore{nodes: []panelstore.Node{relay}}
	remote := &fakeRemote{failOnce: map[string]bool{"docker --version": true}}
	p := New(store, Options{
		LocalBinDir: writeFakeBinaries(t),
		Dial:        func(context.Context, panelstore.Node) (Remote, error) { return remote, nil },
	})
	result, err := p.DeployNode(context.Background(), relay)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !result.Success {
		t.Fatalf("deploy result: %+v", result)
	}
	found := false
	for _, cmd := range remote.commands {
		if strings.Contains(cmd, "get.docker.com") {
			found = true
		}
	}
	if !found {
		t.Fatalf("docker installer was not executed: %v", remote.commands)
	}
}
