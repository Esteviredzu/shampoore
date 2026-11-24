package provisioner

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type fakeStore struct {
	nodes    []panelstore.Node
	routes   []panelstore.Route
	bindings map[string][]int64
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

func (f *fakeStore) GetSettings(context.Context) (panelstore.Settings, error) {
	return panelstore.DefaultSettings(), nil
}

func (f *fakeStore) ListGeoRuleBindings(context.Context) (map[string][]int64, error) {
	return f.bindings, nil
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

func TestSetNodeRunningUsesComposeStopAndUp(t *testing.T) {
	remote := &fakeRemote{failOnce: map[string]bool{}}
	p := New(&fakeStore{}, Options{
		Dial: func(context.Context, panelstore.Node) (Remote, error) { return remote, nil },
	})
	node := panelstore.Node{ID: 2, Name: "exit", SSHHost: "198.51.100.2", SSHUser: "root", SSHPort: 22}

	res, err := p.SetNodeRunning(context.Background(), node, false)
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if !res.Success || res.Running {
		t.Fatalf("stop result = %+v", res)
	}
	if !commandContains(remote.commands, "docker-compose.yml stop") {
		t.Fatalf("stop command not issued: %v", remote.commands)
	}

	res, err = p.SetNodeRunning(context.Background(), node, true)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if !res.Success || !res.Running {
		t.Fatalf("start result = %+v", res)
	}
	if !commandContains(remote.commands, "docker-compose.yml up -d") {
		t.Fatalf("start command not issued: %v", remote.commands)
	}
}

func commandContains(commands []string, want string) bool {
	for _, cmd := range commands {
		if strings.Contains(cmd, want) {
			return true
		}
	}
	return false
}

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
	forceRecreate := false
	for _, cmd := range remote.commands {
		if strings.Contains(cmd, "up -d") {
			foundUp = true
			if strings.Contains(cmd, "--force-recreate") && strings.Contains(cmd, "--remove-orphans") {
				forceRecreate = true
			}
		}
	}
	if !foundUp {
		t.Fatalf("compose up was not executed: %v", remote.commands)
	}
	if !forceRecreate {
		t.Fatalf("compose up did not force recreation: %v", remote.commands)
	}
}

func TestDeployNodeCreatesSiloBucket(t *testing.T) {
	relay := panelstore.Node{
		ID:               1,
		Name:             "relay-silo",
		SSHHost:          "203.0.113.1",
		SSHUser:          "root",
		SSHPort:          22,
		IsRelay:          true,
		GeneratedSecrets: secretsFor("silo"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportSilo, Domain: "s3.example.com", Enabled: true},
		},
	}
	store := &fakeStore{nodes: []panelstore.Node{relay}}
	remote := &fakeRemote{}
	p := New(store, Options{
		LocalBinDir: writeFakeBinaries(t),
		Dial:        func(context.Context, panelstore.Node) (Remote, error) { return remote, nil },
	})
	if _, err := p.DeployNode(context.Background(), relay); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	bucket := relay.GeneratedSecrets["s3_bucket"]
	want := "docker exec shampoore-silo mkdir -p '/data/" + bucket + "'"
	found := false
	for _, cmd := range remote.commands {
		if strings.Contains(cmd, want) {
			found = true
		}
	}
	if !found {
		t.Fatalf("silo bucket command %q not executed: %v", want, remote.commands)
	}
}

func TestCleanupNodeRemovesContainersAndFiles(t *testing.T) {
	relay, _, _ := exitFixture()
	remote := &fakeRemote{}
	p := New(&fakeStore{nodes: []panelstore.Node{relay}}, Options{
		Dial: func(context.Context, panelstore.Node) (Remote, error) { return remote, nil },
	})
	result, err := p.CleanupNode(context.Background(), relay)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if !result.Success {
		t.Fatalf("cleanup failed: %+v", result)
	}
	joined := strings.Join(remote.commands, "\n")
	for _, want := range []string{
		"systemctl disable --now shampoore-server",
		"/etc/systemd/system/shampoore",
		"/usr/local/bin/shampoore-",
		"down -v --remove-orphans",
		"label=com.docker.compose.project=shampoore",
		"docker volume rm",
		"rm -rf /opt/shampoore /var/lib/shampoore",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("cleanup did not run %q:\n%s", want, joined)
		}
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

func TestUploadArchiveRoundTrip(t *testing.T) {
	files := []File{
		{Name: "docker-compose.yml", Mode: 0o600, Data: []byte("services:\n  relay:\n    image: example\n")},
		{Name: "bin/shampoore-relay", Mode: 0o755, Data: []byte("relay-binary-data")},
	}
	var archive bytes.Buffer
	if err := writeUploadArchive(&archive, files); err != nil {
		t.Fatalf("write archive: %v", err)
	}

	gz, err := gzip.NewReader(bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatalf("gzip reader (stream is not gzip): %v", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	got := make(map[string]File, len(files))
	var order []string
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("tar read %q: %v", hdr.Name, err)
		}
		got[hdr.Name] = File{Name: hdr.Name, Mode: hdr.Mode, Data: data}
		order = append(order, hdr.Name)
	}
	if len(order) != len(files) {
		t.Fatalf("archive entries = %v, want %d files", order, len(files))
	}
	for i, want := range files {
		if order[i] != want.Name {
			t.Fatalf("entry %d = %q, want %q", i, order[i], want.Name)
		}
		entry := got[want.Name]
		if entry.Mode != want.Mode {
			t.Errorf("%s mode = %o, want %o", want.Name, entry.Mode, want.Mode)
		}
		if !bytes.Equal(entry.Data, want.Data) {
			t.Errorf("%s data = %q, want %q", want.Name, entry.Data, want.Data)
		}
	}

	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
}

func TestUploadFailsBeforeRemoteOnBadArchive(t *testing.T) {

	remote := &sshRemote{}
	err := remote.Upload(context.Background(), "/opt/shampoore", []File{
		{Name: "../escape", Mode: 0o644, Data: []byte("x")},
	}, false)
	if err == nil {
		t.Fatal("expected archive build error")
	}
	if !strings.Contains(err.Error(), "build upload archive") {
		t.Fatalf("error = %v, want archive build failure", err)
	}
}

func TestValidateArchiveName(t *testing.T) {
	for _, name := range []string{"docker-compose.yml", "configs/route-7.json", "bin/shampoore-server", "rules/.keep"} {
		if err := validateArchiveName(name); err != nil {
			t.Errorf("validateArchiveName(%q) = %v, want nil", name, err)
		}
	}
	for _, name := range []string{"", "/etc/passwd", "..", "../escape", "configs/../../escape", "./"} {
		if err := validateArchiveName(name); err == nil {
			t.Errorf("validateArchiveName(%q) = nil, want error", name)
		}
	}
}
func TestDeployRelayUploadsAgentAndStartsHeartbeat(t *testing.T) {
	relay := panelstore.Node{
		ID:               9,
		Name:             "relay-agent",
		SSHHost:          "203.0.113.9",
		SSHUser:          "root",
		SSHPort:          22,
		IsRelay:          true,
		NodeSecret:       "relay-secret",
		GeneratedSecrets: secretsFor("relay"),
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
		},
	}
	store := &fakeStore{nodes: []panelstore.Node{relay}}
	remote := &fakeRemote{}
	p := New(store, Options{
		PanelURL:    "https://panel.example.com",
		LocalBinDir: writeFakeBinaries(t),
		Dial:        func(context.Context, panelstore.Node) (Remote, error) { return remote, nil },
	})
	if _, err := p.DeployNode(context.Background(), relay); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	uploads := make(map[string]string, len(remote.uploads))
	for _, f := range remote.uploads {
		uploads[f.Name] = string(f.Data)
	}
	for _, name := range []string{"bin/shampoore-relay", "bin/shampoore-node-agent", "agent.env", "docker-compose.yml"} {
		if _, ok := uploads[name]; !ok {
			t.Fatalf("relay deploy did not upload %q", name)
		}
	}
	if got := uploads["bin/shampoore-node-agent"]; got != "binary-shampoore-node-agent" {
		t.Fatalf("uploaded agent binary = %q", got)
	}
	if !strings.Contains(uploads["agent.env"], "INTERVAL=5s") {
		t.Fatalf("agent.env = %q", uploads["agent.env"])
	}
	compose := uploads["docker-compose.yml"]
	if !strings.Contains(compose, "node-agent:") || !strings.Contains(compose, "shampoore-node-agent") {
		t.Fatalf("relay compose does not run the agent:\n%s", compose)
	}
}
