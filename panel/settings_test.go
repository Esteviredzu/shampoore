package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type fakeCaddy struct {
	mu     sync.Mutex
	sites  []CaddySite
	status CaddyStatus
	err    error
}

func (f *fakeCaddy) Sync(_ context.Context, sites []CaddySite) (CaddyStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sites = append([]CaddySite(nil), sites...)
	status := f.status
	if status.Message == "" && f.err == nil {
		status = CaddyStatus{Applied: true, Message: "ok"}
	}
	return status, f.err
}

func (f *fakeCaddy) lastSites() []CaddySite {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]CaddySite(nil), f.sites...)
}

func TestSettingsAPIUpdatesPublicURLAndCaddy(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	api.publicURL = ""
	caddy := &fakeCaddy{}
	api.caddy = caddy
	h := api.Handler()

	rec := perform(h, http.MethodGet, "/api/settings", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("get settings = %d: %s", rec.Code, rec.Body.String())
	}
	var view settingsView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.PublicDomain != "" || view.PublicURL != "" {
		t.Fatalf("unexpected initial settings: %+v", view)
	}

	rec = perform(h, http.MethodPut, "/api/settings",
		`{"public_domain":"Sub.Example.com","public_url":""}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("put settings = %d: %s", rec.Code, rec.Body.String())
	}
	settings, err := store.GetSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if settings.PublicDomain != "sub.example.com" || settings.PublicURL != "https://sub.example.com" {
		t.Fatalf("stored settings = %+v", settings)
	}
	if got := api.currentPublicURL(); got != "https://sub.example.com" {
		t.Fatalf("currentPublicURL = %q", got)
	}
	sites := caddy.lastSites()
	if len(sites) != 1 || sites[0].Domain != "sub.example.com" || sites[0].Upstream != "127.0.0.1:8443" {
		t.Fatalf("caddy sites = %+v", sites)
	}

	if _, err := store.CreateNode(context.Background(), panelstore.Node{
		Name: "local-relay", SSHHost: "127.0.0.1", IsRelay: true,
		Transports: []panelstore.Transport{{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true}},
	}); err != nil {
		t.Fatal(err)
	}
	api.syncLocalCaddy(context.Background())
	sites = caddy.lastSites()
	foundRelay := false
	for _, site := range sites {
		if site.Domain == "ws.example.com" && site.Upstream == "127.0.0.1:8787" {
			foundRelay = true
		}
	}
	if !foundRelay {
		t.Fatalf("local relay domain missing from caddy sites: %+v", sites)
	}

	rec = perform(h, http.MethodPut, "/api/settings", `{"public_domain":"bad domain"}`, authRequest())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid domain = %d, want 400", rec.Code)
	}
}

func TestLocalCaddyDropInAndManagedBlock(t *testing.T) {
	t.Run("drop-in", func(t *testing.T) {
		dir := t.TempDir()
		caddyfile := filepath.Join(dir, "Caddyfile")
		if err := os.WriteFile(caddyfile, []byte("import conf.d/*\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		c := &localCaddy{
			caddyfile: caddyfile,
			confDir:   filepath.Join(dir, "conf.d"),
			readFile:  os.ReadFile,
			writeFile: writeFileAtomic,
			mkdirAll:  os.MkdirAll,
			run:       func(context.Context, string, ...string) (string, error) { return "", nil },
		}
		sites := []CaddySite{{Domain: "sub.example.com", Upstream: "127.0.0.1:8443"}}
		for i := 0; i < 2; i++ {
			status, err := c.Sync(context.Background(), sites)
			if err != nil || !status.Applied {
				t.Fatalf("sync %d: %+v %v", i, status, err)
			}
		}
		data, err := os.ReadFile(filepath.Join(dir, "conf.d", caddyConfFile))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(data), "sub.example.com") != 1 {
			t.Fatalf("drop-in is not idempotent:\n%s", data)
		}
		if !strings.Contains(string(data), "reverse_proxy 127.0.0.1:8443") {
			t.Fatalf("drop-in missing upstream:\n%s", data)
		}
	})

	t.Run("managed block", func(t *testing.T) {
		dir := t.TempDir()
		caddyfile := filepath.Join(dir, "Caddyfile")
		if err := os.WriteFile(caddyfile, []byte("example.org {\n\trespond \"ok\"\n}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		c := &localCaddy{
			caddyfile: caddyfile,
			confDir:   filepath.Join(dir, "conf.d"),
			readFile:  os.ReadFile,
			writeFile: writeFileAtomic,
			mkdirAll:  os.MkdirAll,
			run:       func(context.Context, string, ...string) (string, error) { return "", nil },
		}
		sites := []CaddySite{{Domain: "sub.example.com", Upstream: "127.0.0.1:8443"}}
		for i := 0; i < 2; i++ {
			if _, err := c.Sync(context.Background(), sites); err != nil {
				t.Fatal(err)
			}
		}
		data, err := os.ReadFile(caddyfile)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(data), caddyBlockBegin) != 1 {
			t.Fatalf("managed block duplicated:\n%s", data)
		}
		if !strings.Contains(string(data), "example.org") {
			t.Fatalf("managed block clobbered existing sites:\n%s", data)
		}
	})
}

func TestDeleteNodeCleansServerAndHonoursForce(t *testing.T) {
	api, store, deployer, _ := newControlTestAPI(t)
	node, err := store.CreateNode(context.Background(), panelstore.Node{
		Name: "relay", SSHHost: "203.0.113.10", IsRelay: true,
		Transports: []panelstore.Transport{{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := api.Handler()

	deployer.cleanupErr = context.DeadlineExceeded
	rec := perform(h, http.MethodDelete, "/api/nodes/"+itoa(node.ID), "", authRequest())
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("cleanup failure = %d, want 502: %s", rec.Code, rec.Body.String())
	}
	if _, err := store.GetNode(context.Background(), node.ID); err != nil {
		t.Fatalf("node was deleted despite cleanup failure: %v", err)
	}

	rec = perform(h, http.MethodDelete, "/api/nodes/"+itoa(node.ID)+"?force=true", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("forced delete = %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := store.GetNode(context.Background(), node.ID); err == nil {
		t.Fatal("node still exists after forced delete")
	}
	if deployer.cleanupCalls != 2 {
		t.Fatalf("cleanup calls = %d, want 2", deployer.cleanupCalls)
	}
}

func TestRouteChangeRedeploysDeployedExit(t *testing.T) {
	api, store, deployer, _ := newControlTestAPI(t)
	relay, err := store.CreateNode(context.Background(), panelstore.Node{
		Name: "relay", SSHHost: "203.0.113.10", IsRelay: true,
		Transports: []panelstore.Transport{{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exit, err := store.CreateNode(context.Background(), panelstore.Node{
		Name: "exit", SSHHost: "198.51.100.20", IsExit: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetNodeStatus(context.Background(), exit.ID, panelstore.NodeStatusDeployed, ""); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	deployer.started = started
	h := api.Handler()
	body := `{"name":"de-fr","entry_node_id":` + itoa(relay.ID) + `,"exit_node_id":` + itoa(exit.ID) + `,"transport_type":"raw_ws","enabled":true}`
	rec := perform(h, http.MethodPost, "/api/routes", body, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create route = %d: %s", rec.Code, rec.Body.String())
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("route change did not trigger a background redeploy")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		deployer.mu.Lock()
		calls := deployer.calls
		deployer.mu.Unlock()
		if calls > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background deploy did not run")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDisableSubscriptionsDropsCaddySite(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	api.publicURL = ""
	caddy := &fakeCaddy{}
	api.caddy = caddy
	h := api.Handler()

	rec := perform(h, http.MethodPut, "/api/settings", `{"public_domain":"sub.example.com"}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("set domain = %d: %s", rec.Code, rec.Body.String())
	}
	sites := caddy.lastSites()
	if len(sites) != 1 || sites[0].Domain != "sub.example.com" {
		t.Fatalf("caddy sites after enabling = %+v", sites)
	}

	rec = perform(h, http.MethodPut, "/api/settings", `{"subscriptions_enabled":false}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("disable subscriptions = %d: %s", rec.Code, rec.Body.String())
	}
	if sites := caddy.lastSites(); len(sites) != 0 {
		t.Fatalf("caddy sites after disabling = %+v, want none", sites)
	}
	settings, err := store.GetSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if settings.PublicDomain != "sub.example.com" || settings.SubscriptionsEnabled {
		t.Fatalf("settings after disabling = %+v", settings)
	}

	rec = perform(h, http.MethodPut, "/api/settings", `{"subscriptions_enabled":true}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("re-enable subscriptions = %d: %s", rec.Code, rec.Body.String())
	}
	if sites := caddy.lastSites(); len(sites) != 1 || sites[0].Domain != "sub.example.com" {
		t.Fatalf("caddy sites after re-enabling = %+v", sites)
	}

	rec = perform(h, http.MethodDelete, "/api/settings/domain", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("delete domain = %d: %s", rec.Code, rec.Body.String())
	}
	if sites := caddy.lastSites(); len(sites) != 0 {
		t.Fatalf("caddy sites after domain delete = %+v, want none", sites)
	}
}
