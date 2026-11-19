package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type geoTestServer struct {
	mu          sync.Mutex
	body        string
	status      int
	contentType string
}

func (s *geoTestServer) set(body string, status int, contentType string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.body = body
	s.status = status
	s.contentType = contentType
}

func (s *geoTestServer) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	body := s.body
	status := s.status
	contentType := s.contentType
	s.mu.Unlock()
	if contentType == "" {
		contentType = "text/plain"
	}
	if status == 0 {
		status = http.StatusOK
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func newGeoTestAPI(t *testing.T) (*API, *panelstore.Store, string) {
	t.Helper()
	api, store, _, dir := newControlTestAPI(t)
	rulesDir := filepath.Join(dir, "rules")
	if err := os.MkdirAll(rulesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	api.rulesDir = rulesDir
	return api, store, rulesDir
}

func geoImportBody(t *testing.T, name, url string, interval int) string {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"name":                    name,
		"url":                     url,
		"update_interval_minutes": interval,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestGeoRuleImportRefreshAndDelete(t *testing.T) {
	api, store, rulesDir := newGeoTestAPI(t)
	srv := &geoTestServer{}
	srv.set("# comment\n1.2.3.0/24\n2001:db8::/32\n", http.StatusOK, "text/plain")
	server := httptest.NewServer(srv)
	defer server.Close()
	h := api.Handler()

	rec := perform(h, http.MethodPost, "/api/georules/import-url",
		geoImportBody(t, "ru", server.URL+"/ru.txt", 60), authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("import = %d: %s", rec.Code, rec.Body.String())
	}
	path := filepath.Join(rulesDir, "ru.txt")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "1.2.3.0/24\n2001:db8::/32\n" {
		t.Fatalf("rule file = %q", data)
	}
	source, err := store.GetGeoRuleSource(context.Background(), "ru")
	if err != nil {
		t.Fatal(err)
	}
	if source.URL != server.URL+"/ru.txt" || source.UpdateIntervalMinutes != 60 || source.LastUpdatedAt.IsZero() {
		t.Fatalf("source = %+v", source)
	}

	rec = perform(h, http.MethodGet, "/api/georules", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("list sources = %d: %s", rec.Code, rec.Body.String())
	}
	var sources []panelstore.GeoRuleSource
	if err := json.Unmarshal(rec.Body.Bytes(), &sources); err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].Name != "ru" || sources[0].URL != server.URL+"/ru.txt" ||
		sources[0].UpdateIntervalMinutes != 60 || sources[0].LastUpdatedAt.IsZero() {
		t.Fatalf("sources = %+v", sources)
	}

	srv.set("5.6.7.0/24\n", http.StatusOK, "text/plain")
	rec = perform(h, http.MethodPost, "/api/georules/ru/refresh", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("refresh = %d: %s", rec.Code, rec.Body.String())
	}
	rec = perform(h, http.MethodGet, "/api/georules", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("list sources after refresh = %d: %s", rec.Code, rec.Body.String())
	}
	sources = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &sources); err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].LastUpdatedAt.IsZero() {
		t.Fatalf("sources after refresh = %+v", sources)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "5.6.7.0/24\n" {
		t.Fatalf("refreshed rule file = %q", data)
	}

	rec = perform(h, http.MethodDelete, "/api/georules/ru", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("delete rule = %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("rule file still exists: %v", err)
	}
	if _, err := store.GetGeoRuleSource(context.Background(), "ru"); err == nil {
		t.Fatal("geo rule source still exists after delete")
	}
	rec = perform(h, http.MethodGet, "/api/georules", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("list sources after delete = %d: %s", rec.Code, rec.Body.String())
	}
	sources = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &sources); err != nil {
		t.Fatal(err)
	}
	if len(sources) != 0 {
		t.Fatalf("sources after delete = %+v", sources)
	}
}

func TestGeoRuleSourcesListEmpty(t *testing.T) {
	api, _, _ := newGeoTestAPI(t)
	rec := perform(api.Handler(), http.MethodGet, "/api/georules", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("list sources = %d: %s", rec.Code, rec.Body.String())
	}
	var sources []panelstore.GeoRuleSource
	if err := json.Unmarshal(rec.Body.Bytes(), &sources); err != nil {
		t.Fatal(err)
	}
	if len(sources) != 0 {
		t.Fatalf("sources = %+v", sources)
	}
}

func TestGeoRuleImportRejectsHTML(t *testing.T) {
	api, _, rulesDir := newGeoTestAPI(t)
	srv := &geoTestServer{}
	srv.set("<!DOCTYPE html><html><body>rate limited</body></html>", http.StatusOK, "text/html")
	server := httptest.NewServer(srv)
	defer server.Close()

	rec := perform(api.Handler(), http.MethodPost, "/api/georules/import-url",
		geoImportBody(t, "htmlrule", server.URL+"/index.html", 0), authRequest())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("html import = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(rulesDir, "htmlrule.txt")); !os.IsNotExist(err) {
		t.Fatalf("html import created a rule file: %v", err)
	}
}

func TestGeoRuleRefreshKeepsOldFileOnError(t *testing.T) {
	api, store, rulesDir := newGeoTestAPI(t)
	srv := &geoTestServer{}
	srv.set("1.2.3.0/24\n", http.StatusOK, "text/plain")
	server := httptest.NewServer(srv)
	defer server.Close()
	h := api.Handler()

	rec := perform(h, http.MethodPost, "/api/georules/import-url",
		geoImportBody(t, "ru", server.URL+"/ru.txt", 0), authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("import = %d: %s", rec.Code, rec.Body.String())
	}
	before, err := store.GetGeoRuleSource(context.Background(), "ru")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(rulesDir, "ru.txt")

	srv.set("", http.StatusInternalServerError, "text/plain")
	rec = perform(h, http.MethodPost, "/api/georules/ru/refresh", "", authRequest())
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("failed refresh = %d, want 502: %s", rec.Code, rec.Body.String())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(data)) != "1.2.3.0/24" {
		t.Fatalf("old rule file was overwritten: %q", data)
	}
	after, err := store.GetGeoRuleSource(context.Background(), "ru")
	if err != nil {
		t.Fatal(err)
	}
	if after.LastError == "" {
		t.Fatal("last_error was not recorded")
	}
	if !after.LastUpdatedAt.Equal(before.LastUpdatedAt) {
		t.Fatalf("last_updated_at changed on failed refresh: %v -> %v", before.LastUpdatedAt, after.LastUpdatedAt)
	}

	srv.set("<html><body>not a list</body></html>", http.StatusOK, "text/html")
	rec = perform(h, http.MethodPost, "/api/georules/ru/refresh", "", authRequest())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("html refresh = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(data)) != "1.2.3.0/24" {
		t.Fatalf("old rule file was overwritten by html: %q", data)
	}
}

func TestGeoRuleUpdaterRefreshesDueSources(t *testing.T) {
	api, store, rulesDir := newGeoTestAPI(t)
	srv := &geoTestServer{}
	srv.set("9.9.9.0/24\n", http.StatusOK, "text/plain")
	server := httptest.NewServer(srv)
	defer server.Close()

	if _, err := store.UpsertGeoRuleSource(context.Background(), panelstore.GeoRuleSource{
		Name:                  "due",
		URL:                   server.URL + "/due.txt",
		UpdateIntervalMinutes: 1,
		LastUpdatedAt:         time.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	api.geoUpdateEvery = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api.StartGeoRuleUpdater(ctx)

	path := filepath.Join(rulesDir, "due.txt")
	deadline := time.Now().Add(3 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			if strings.TrimSpace(string(data)) != "9.9.9.0/24" {
				t.Fatalf("updated rule file = %q", data)
			}
			break
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("updater did not refresh the due source")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
}

func geoTestExit(t *testing.T, store *panelstore.Store, name, host string) panelstore.Node {
	t.Helper()
	node, err := store.CreateNode(context.Background(), panelstore.Node{
		Name: name, SSHHost: host, IsExit: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetNodeStatus(context.Background(), node.ID, panelstore.NodeStatusDeployed, ""); err != nil {
		t.Fatal(err)
	}
	return node
}

func geoTestRelay(t *testing.T, store *panelstore.Store, name, host string) panelstore.Node {
	t.Helper()
	node, err := store.CreateNode(context.Background(), panelstore.Node{
		Name: name, SSHHost: host, IsRelay: true,
		Transports: []panelstore.Transport{
			{Type: panelstore.TransportRawWS, Domain: "ws.example.com", Enabled: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func TestGeoRuleImportNodeBindingTriggersRedeploy(t *testing.T) {
	api, store, _ := newGeoTestAPI(t)
	deployer, ok := api.provisioner.(*fakeDeployer)
	if !ok {
		t.Fatal("unexpected deployer type")
	}
	deployer.started = make(chan struct{})
	exit := geoTestExit(t, store, "exit-a", "198.51.100.11")

	srv := &geoTestServer{}
	srv.set("1.2.3.0/24\n", http.StatusOK, "text/plain")
	server := httptest.NewServer(srv)
	defer server.Close()
	body, err := json.Marshal(map[string]any{
		"name":                    "ru",
		"url":                     server.URL + "/ru.txt",
		"update_interval_minutes": 0,
		"exit_node_ids":           []int64{exit.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := perform(api.Handler(), http.MethodPost, "/api/georules/import-url", string(body), authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("import = %d: %s", rec.Code, rec.Body.String())
	}
	ids, err := store.GeoRuleNodes(context.Background(), "ru")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != exit.ID {
		t.Fatalf("bindings = %v", ids)
	}
	select {
	case <-deployer.started:
	case <-time.After(3 * time.Second):
		t.Fatal("import did not schedule a redeploy of the bound exit")
	}
}

func TestGeoRuleManualRuleBindingsAndUpdate(t *testing.T) {
	api, store, rulesDir := newGeoTestAPI(t)
	exitA := geoTestExit(t, store, "exit-a", "198.51.100.21")
	exitB := geoTestExit(t, store, "exit-b", "198.51.100.22")
	relay := geoTestRelay(t, store, "relay", "203.0.113.21")
	h := api.Handler()

	body, err := json.Marshal(map[string]any{
		"name":          "manual",
		"cidrs":         []string{"1.2.3.0/24"},
		"exit_node_ids": []int64{exitA.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := perform(h, http.MethodPost, "/api/rules", string(body), authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create rule = %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(rulesDir, "manual.txt")); err != nil {
		t.Fatalf("rule file not created: %v", err)
	}
	ids, err := store.GeoRuleNodes(context.Background(), "manual")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != exitA.ID {
		t.Fatalf("created bindings = %v", ids)
	}

	rec = perform(h, http.MethodPut, "/api/georules/manual/nodes",
		`{"exit_node_ids":[`+itoa(exitB.ID)+`]}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("update nodes = %d: %s", rec.Code, rec.Body.String())
	}
	ids, err = store.GeoRuleNodes(context.Background(), "manual")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != exitB.ID {
		t.Fatalf("updated bindings = %v", ids)
	}

	rec = perform(h, http.MethodPut, "/api/georules/manual/nodes",
		`{"exit_node_ids":[`+itoa(relay.ID)+`]}`, authRequest())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("relay binding = %d, want 400: %s", rec.Code, rec.Body.String())
	}

	rec = perform(h, http.MethodPut, "/api/georules/missing/nodes", `{"exit_node_ids":[]}`, authRequest())
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing rule = %d, want 404: %s", rec.Code, rec.Body.String())
	}

	rec = perform(h, http.MethodDelete, "/api/rules/manual", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("delete rule = %d: %s", rec.Code, rec.Body.String())
	}
	ids, err = store.GeoRuleNodes(context.Background(), "manual")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("bindings after delete = %v", ids)
	}
}

func TestGeoRuleImportWithoutNodeFieldKeepsBinding(t *testing.T) {
	api, store, _ := newGeoTestAPI(t)
	exit := geoTestExit(t, store, "exit-a", "198.51.100.31")
	srv := &geoTestServer{}
	srv.set("1.2.3.0/24\n", http.StatusOK, "text/plain")
	server := httptest.NewServer(srv)
	defer server.Close()
	h := api.Handler()

	body, err := json.Marshal(map[string]any{
		"name": "ru", "url": server.URL + "/ru.txt", "update_interval_minutes": 0,
		"exit_node_ids": []int64{exit.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := perform(h, http.MethodPost, "/api/georules/import-url", string(body), authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("import = %d: %s", rec.Code, rec.Body.String())
	}

	body, err = json.Marshal(map[string]any{
		"name": "ru", "url": server.URL + "/ru.txt", "update_interval_minutes": 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	rec = perform(h, http.MethodPost, "/api/georules/import-url", string(body), authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("re-import = %d: %s", rec.Code, rec.Body.String())
	}
	ids, err := store.GeoRuleNodes(context.Background(), "ru")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != exit.ID {
		t.Fatalf("bindings were lost on re-import: %v", ids)
	}
}
