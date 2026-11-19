package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/control"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/provisioner"
)

type fakeDeployer struct {
	mu           sync.Mutex
	calls        int
	cleanupCalls int
	started      chan struct{}
	block        chan struct{}
	once         sync.Once
	err          error
	cleanupErr   error
}

func (f *fakeDeployer) CleanupNode(_ context.Context, node panelstore.Node) (*provisioner.CleanupResult, error) {
	f.mu.Lock()
	f.cleanupCalls++
	err := f.cleanupErr
	f.mu.Unlock()
	result := &provisioner.CleanupResult{NodeID: node.ID, Success: err == nil}
	if err != nil {
		result.Steps = []provisioner.Step{{Name: "cleanup", Error: err.Error()}}
		return result, err
	}
	result.Steps = []provisioner.Step{{Name: "cleanup", Output: "ok"}}
	return result, nil
}

func (f *fakeDeployer) DeployNode(_ context.Context, node panelstore.Node) (*provisioner.DeployResult, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.started != nil {
		f.once.Do(func() { close(f.started) })
	}
	if f.block != nil {
		<-f.block
	}
	if f.err != nil {
		return &provisioner.DeployResult{NodeID: node.ID, Steps: []provisioner.Step{{Name: "fake", Error: f.err.Error()}}}, f.err
	}
	return &provisioner.DeployResult{NodeID: node.ID, Success: true, Steps: []provisioner.Step{{Name: "fake", Output: "ok"}}}, nil
}

func newControlTestAPI(t *testing.T) (*API, *panelstore.Store, *fakeDeployer, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := panelstore.Open(context.Background(), filepath.Join(dir, "panel.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	deployer := &fakeDeployer{}
	api := New(
		control.NewClient(filepath.Join(dir, "control.sock")),
		&fakeManager{},
		logging.New("panel-test", false),
		"secret-token",
		dir,
		nil,
		"",
		WithStore(store),
		WithProvisioner(deployer),
		WithPublicURL("https://panel.example.com"),
	)
	return api, store, deployer, dir
}

func authRequest() func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer secret-token") }
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func TestControlPlaneRequiresAdminAuth(t *testing.T) {
	api, _, _, _ := newControlTestAPI(t)
	rec := perform(api.Handler(), http.MethodGet, "/api/nodes", "", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want 401", rec.Code)
	}
	rec = perform(api.Handler(), http.MethodGet, "/api/nodes", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("authorized status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
}

func TestControlPlaneNodeRouteUserCRUD(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	h := api.Handler()

	relayBody := `{
		"name":"relay-de","ssh_host":"203.0.113.10","is_relay":true,
		"transports":[{"type":"raw_ws","domain":"ws.example.com","enabled":true},
		              {"type":"silo","domain":"s3.example.com","enabled":true},
		              {"type":"livekit","domain":"lk.example.com","enabled":true}]
	}`
	rec := perform(h, http.MethodPost, "/api/nodes", relayBody, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create relay = %d: %s", rec.Code, rec.Body.String())
	}
	var relay panelstore.Node
	if err := json.Unmarshal(rec.Body.Bytes(), &relay); err != nil {
		t.Fatal(err)
	}
	if relay.ID == 0 || relay.NodeSecret == "" || len(relay.GeneratedSecrets) == 0 {
		t.Fatalf("relay not initialized: %+v", relay)
	}

	exitBody := `{"name":"exit-fr","ssh_host":"198.51.100.20","is_exit":true,"socks_port":1080}`
	rec = perform(h, http.MethodPost, "/api/nodes", exitBody, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create exit = %d: %s", rec.Code, rec.Body.String())
	}
	var exit panelstore.Node
	_ = json.Unmarshal(rec.Body.Bytes(), &exit)

	routeBody := `{"name":"de-fr","entry_node_id":` + itoa(relay.ID) + `,"exit_node_id":` + itoa(exit.ID) + `,"transport_type":"raw_ws","enabled":true}`
	rec = perform(h, http.MethodPost, "/api/routes", routeBody, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create route = %d: %s", rec.Code, rec.Body.String())
	}
	var route panelstore.Route
	_ = json.Unmarshal(rec.Body.Bytes(), &route)

	rec = perform(h, http.MethodPost, "/api/users", `{"name":"Alice","enabled":true}`, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create user = %d: %s", rec.Code, rec.Body.String())
	}
	var created struct {
		panelstore.User
		SubscriptionURL string `json:"subscription_url"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.SubToken == "" {
		t.Fatalf("user not initialized: %+v", created)
	}
	if created.SubscriptionURL != "https://panel.example.com/sub/"+created.SubToken {
		t.Fatalf("subscription url = %q", created.SubscriptionURL)
	}

	rec = perform(h, http.MethodGet, "/api/routes", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("list routes = %d", rec.Code)
	}
	var routes []panelstore.Route
	if err := json.Unmarshal(rec.Body.Bytes(), &routes); err != nil || len(routes) != 1 {
		t.Fatalf("routes = %+v, %v", routes, err)
	}

	rec = perform(h, http.MethodDelete, "/api/routes/"+itoa(route.ID), "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("delete route = %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := store.GetRoute(context.Background(), route.ID); err == nil {
		t.Fatal("route still exists after delete")
	}
}

func TestDeployNodeAndConflict(t *testing.T) {
	api, store, deployer, _ := newControlTestAPI(t)
	h := api.Handler()
	rec := perform(h, http.MethodPost, "/api/nodes", `{"name":"relay","ssh_host":"203.0.113.1","is_relay":true,"transports":[{"type":"raw_ws","domain":"ws.example.com","enabled":true}]}`, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create node = %d: %s", rec.Code, rec.Body.String())
	}
	var node panelstore.Node
	_ = json.Unmarshal(rec.Body.Bytes(), &node)

	rec = perform(h, http.MethodPost, "/api/nodes/"+itoa(node.ID)+"/deploy", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("deploy = %d: %s", rec.Code, rec.Body.String())
	}
	if deployer.calls != 1 {
		t.Fatalf("deploy calls = %d", deployer.calls)
	}
	got, _ := store.GetNode(context.Background(), node.ID)
	if got.Status != panelstore.NodeStatusDeployed {
		t.Fatalf("status = %q", got.Status)
	}

	blocking := &fakeDeployer{started: make(chan struct{}), block: make(chan struct{})}
	api.provisioner = blocking
	done := make(chan int, 1)
	go func() {
		rec := perform(h, http.MethodPost, "/api/nodes/"+itoa(node.ID)+"/deploy", "", authRequest())
		done <- rec.Code
	}()
	<-blocking.started
	rec = perform(h, http.MethodPost, "/api/nodes/"+itoa(node.ID)+"/deploy", "", authRequest())
	if rec.Code != http.StatusConflict {
		t.Fatalf("concurrent deploy = %d, want 409", rec.Code)
	}
	close(blocking.block)
	<-done
}

func TestControlPlaneRejectsDuplicateTransportTypes(t *testing.T) {
	api, _, _, _ := newControlTestAPI(t)
	rec := perform(api.Handler(), http.MethodPost, "/api/nodes", `{
		"name":"relay-dup","ssh_host":"203.0.113.44","is_relay":true,
		"transports":[
			{"type":"livekit","domain":"lk-one.example.com","enabled":true},
			{"type":"livekit","domain":"lk-two.example.com","enabled":true}
		]
	}`, authRequest())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("duplicate transports = %d, want 400: %s", rec.Code, rec.Body.String())
	}
}
