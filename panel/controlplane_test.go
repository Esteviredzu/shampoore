package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
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
	if relay.ID == 0 || relay.NodeSecret == "" || len(relay.Secrets) == 0 {
		t.Fatalf("relay not initialized: %+v", relay)
	}
	if relay.EncryptionCipher != panelstore.EncryptionCipherName {
		t.Fatalf("encryption cipher = %q", relay.EncryptionCipher)
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

func TestControlPlaneNodeSecretsExposedAndEditable(t *testing.T) {
	api, _, _, _ := newControlTestAPI(t)
	h := api.Handler()

	createBody := `{
		"name":"relay-secrets","ssh_host":"203.0.113.77","is_relay":true,
		"transports":[{"type":"raw_ws","domain":"ws.example.com","enabled":true}],
		"secrets":{
			"encryption_password":"custom-enc",
			"raw_ws_password":"custom-raw",
			"livekit_api_key":"custom-key",
			"livekit_api_secret":"custom-secret",
			"minio_root_user":"custom-minio",
			"minio_root_password":"custom-minio-pass",
			"s3_bucket":"custom-bucket"
		}
	}`
	rec := perform(h, http.MethodPost, "/api/nodes", createBody, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create node = %d: %s", rec.Code, rec.Body.String())
	}
	var created panelstore.Node
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.EncryptionCipher != panelstore.EncryptionCipherName {
		t.Fatalf("encryption_cipher = %q", created.EncryptionCipher)
	}
	for key, want := range map[string]string{
		"encryption_password": "custom-enc",
		"raw_ws_password":     "custom-raw",
		"livekit_api_key":     "custom-key",
		"livekit_api_secret":  "custom-secret",
		"minio_root_user":     "custom-minio",
		"minio_root_password": "custom-minio-pass",
		"s3_bucket":           "custom-bucket",
	} {
		if got := created.Secrets[key]; got != want {
			t.Fatalf("created secret %q = %q, want %q", key, got, want)
		}
	}

	rec = perform(h, http.MethodGet, "/api/nodes/"+itoa(created.ID), "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("get node = %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "generated_secrets") {
		t.Fatalf("GET /api/nodes leaked generated_secrets: %s", rec.Body.String())
	}
	var fetched panelstore.Node
	if err := json.Unmarshal(rec.Body.Bytes(), &fetched); err != nil {
		t.Fatal(err)
	}
	if fetched.Secrets["raw_ws_password"] != "custom-raw" || fetched.EncryptionCipher != panelstore.EncryptionCipherName {
		t.Fatalf("fetched node secrets/cipher = %+v", fetched)
	}
	rec = perform(h, http.MethodGet, "/api/nodes", "", authRequest())
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"secrets"`) || !strings.Contains(rec.Body.String(), `"encryption_cipher"`) {
		t.Fatalf("list nodes = %d: %s", rec.Code, rec.Body.String())
	}

	updateBody := `{
		"name":"relay-secrets","ssh_host":"203.0.113.77","is_relay":true,
		"transports":[{"type":"raw_ws","domain":"ws.example.com","enabled":true}],
		"secrets":{"raw_ws_password":"rotated-raw"}
	}`
	rec = perform(h, http.MethodPut, "/api/nodes/"+itoa(created.ID), updateBody, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("update node = %d: %s", rec.Code, rec.Body.String())
	}
	var updated panelstore.Node
	if err := json.Unmarshal(rec.Body.Bytes(), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Secrets["raw_ws_password"] != "rotated-raw" {
		t.Fatalf("rotated raw_ws_password = %q", updated.Secrets["raw_ws_password"])
	}
	if updated.Secrets["encryption_password"] != "custom-enc" {
		t.Fatalf("partial update rotated encryption_password: %q", updated.Secrets["encryption_password"])
	}

	badBody := `{
		"name":"relay-secrets","ssh_host":"203.0.113.77","is_relay":true,
		"transports":[{"type":"raw_ws","domain":"ws.example.com","enabled":true}],
		"secrets":{"raw_ws_password":"bad\nvalue"}
	}`
	rec = perform(h, http.MethodPut, "/api/nodes/"+itoa(created.ID), badBody, authRequest())
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "control characters") {
		t.Fatalf("invalid secret update = %d, want 400 control-character error: %s", rec.Code, rec.Body.String())
	}
}

func TestControlPlaneMultiRouteAPI(t *testing.T) {
	api, _, _, _ := newControlTestAPI(t)
	h := api.Handler()

	rec := perform(h, http.MethodPost, "/api/nodes", `{
		"name":"relay-multi-api","ssh_host":"203.0.113.60","is_relay":true,
		"transports":[{"type":"raw_ws","domain":"ws.example.com","enabled":true},
		              {"type":"silo","domain":"s3.example.com","enabled":true}]
	}`, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create relay = %d: %s", rec.Code, rec.Body.String())
	}
	var relay panelstore.Node
	_ = json.Unmarshal(rec.Body.Bytes(), &relay)
	rec = perform(h, http.MethodPost, "/api/nodes", `{"name":"exit-multi-api","ssh_host":"198.51.100.60","is_exit":true}`, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create exit = %d: %s", rec.Code, rec.Body.String())
	}
	var exit panelstore.Node
	_ = json.Unmarshal(rec.Body.Bytes(), &exit)

	var singles []panelstore.Route
	for _, kind := range []string{"raw_ws", "silo"} {
		body := `{"name":"multi-api-` + kind + `","entry_node_id":` + itoa(relay.ID) +
			`,"exit_node_id":` + itoa(exit.ID) + `,"transport_type":"` + kind + `","enabled":true}`
		rec = perform(h, http.MethodPost, "/api/routes", body, authRequest())
		if rec.Code != http.StatusCreated {
			t.Fatalf("create %s route = %d: %s", kind, rec.Code, rec.Body.String())
		}
		var route panelstore.Route
		_ = json.Unmarshal(rec.Body.Bytes(), &route)
		singles = append(singles, route)
	}

	multiBody := `{"name":"multi-bond-api","transport_type":"multi","multi_strategy":"bond","sub_route_ids":[` +
		itoa(singles[0].ID) + `,` + itoa(singles[1].ID) + `],"enabled":true}`
	rec = perform(h, http.MethodPost, "/api/routes", multiBody, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create multi route = %d: %s", rec.Code, rec.Body.String())
	}
	var multi panelstore.Route
	if err := json.Unmarshal(rec.Body.Bytes(), &multi); err != nil {
		t.Fatal(err)
	}
	if multi.TransportType != panelstore.TransportMulti || multi.MultiStrategy != panelstore.MultiStrategyBond {
		t.Fatalf("multi route = %+v", multi)
	}
	if len(multi.SubRouteIDs) != 2 || multi.EntryNodeID != relay.ID || multi.ExitNodeID != exit.ID {
		t.Fatalf("multi route endpoints/sub-routes = %+v", multi)
	}

	rec = perform(h, http.MethodGet, "/api/routes", "", authRequest())
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"sub_route_ids"`) {
		t.Fatalf("list routes = %d: %s", rec.Code, rec.Body.String())
	}

	rec = perform(h, http.MethodDelete, "/api/routes/"+itoa(singles[0].ID), "", authRequest())
	if rec.Code != http.StatusConflict {
		t.Fatalf("delete child route = %d, want 409: %s", rec.Code, rec.Body.String())
	}
	rec = perform(h, http.MethodDelete, "/api/routes/"+itoa(multi.ID), "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("delete multi route = %d: %s", rec.Code, rec.Body.String())
	}
	rec = perform(h, http.MethodDelete, "/api/routes/"+itoa(singles[0].ID), "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("delete child route after multi removal = %d: %s", rec.Code, rec.Body.String())
	}
}
