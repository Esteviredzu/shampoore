package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/server/manager"
)

type fakePanel struct {
	users   []auth.UserSpec
	traffic [][]panelstore.TrafficDelta
	metrics []*panelstore.NodeMetrics
	active  [][]string
}

func (f *fakePanel) Sync(_ context.Context, _ int64, traffic []panelstore.TrafficDelta, metrics *panelstore.NodeMetrics, activeUserIDs []string) ([]auth.UserSpec, error) {
	f.traffic = append(f.traffic, append([]panelstore.TrafficDelta(nil), traffic...))
	f.metrics = append(f.metrics, metrics)
	f.active = append(f.active, append([]string(nil), activeUserIDs...))
	return f.users, nil
}

type fakeControl struct {
	usage   []manager.UsageEntry
	entries []manager.EntryView
	active  []string

	updates     []manager.ConfigEntry
	syncs       []manager.ConfigEntry
	syncRestart bool
	syncErr     error
}

func (f *fakeControl) Usage() ([]manager.UsageEntry, error) { return f.usage, nil }
func (f *fakeControl) List() ([]manager.EntryView, error)   { return f.entries, nil }
func (f *fakeControl) Update(_ string, entry manager.ConfigEntry) error {
	f.updates = append(f.updates, entry)
	return nil
}

func (f *fakeControl) SyncUsers(_ string, entry manager.ConfigEntry) (bool, error) {
	f.syncs = append(f.syncs, entry)
	return f.syncRestart, f.syncErr
}

func (f *fakeControl) ActiveUserIDs() ([]string, error) {
	return append([]string(nil), f.active...), nil
}

func multiuserSettings(t *testing.T, multiuser bool) *app.Settings {
	t.Helper()
	transport := map[string]any{
		"server_url": "wss://ws.example.com",
		"room":       "route-1",
		"password":   "pw",
		"users":      []any{},
	}
	if multiuser {
		transport["multiuser"] = true
	}
	raw, err := json.Marshal(transport)
	if err != nil {
		t.Fatal(err)
	}
	return &app.Settings{Name: "route-1", Type: "raw_ws", Transport: raw}
}

func TestSyncOnceReportsDeltasAndUpdatesUsers(t *testing.T) {
	panel := &fakePanel{users: []auth.UserSpec{{UserID: "alice", Enabled: true}}}
	ctrl := &fakeControl{
		usage: []manager.UsageEntry{
			{Config: "route-1", UserID: "alice", UploadBytes: 100, DownloadBytes: 200},
			{Config: "route-2", UserID: "alice", UploadBytes: 50, DownloadBytes: 50},
		},
		entries: []manager.EntryView{
			{Name: "route-1", Enabled: true, Settings: multiuserSettings(t, true)},
			{Name: "static", Enabled: true, Settings: multiuserSettings(t, false)},
		},
	}
	agent := NewWithClients(Config{NodeID: 2, StateFile: t.TempDir() + "/state.json"}, panel, ctrl)

	if err := agent.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(panel.traffic) != 1 || len(panel.traffic[0]) != 1 {
		t.Fatalf("traffic = %+v", panel.traffic)
	}
	if got := panel.traffic[0][0]; got.UploadBytes != 150 || got.DownloadBytes != 250 {
		t.Fatalf("delta = %+v", got)
	}
	if len(ctrl.syncs) != 1 || ctrl.syncs[0].Name != "route-1" {
		t.Fatalf("syncs = %+v", ctrl.syncs)
	}
	if len(ctrl.updates) != 0 {
		t.Fatalf("users-only change restarted the config: %+v", ctrl.updates)
	}
	var tr map[string]any
	if err := json.Unmarshal(ctrl.syncs[0].Settings.Transport, &tr); err != nil {
		t.Fatal(err)
	}
	users, _ := tr["users"].([]any)
	if len(users) != 1 {
		t.Fatalf("users not applied: %+v", tr["users"])
	}

	ctrl.usage = []manager.UsageEntry{
		{Config: "route-1", UserID: "alice"},
		{Config: "route-2", UserID: "alice", UploadBytes: 50, DownloadBytes: 50},
	}
	if err := agent.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(panel.traffic) != 2 || len(panel.traffic[1]) != 0 {
		t.Fatalf("second traffic = %+v", panel.traffic)
	}
	if len(ctrl.updates) != 0 {
		t.Fatalf("unexpected update: %+v", ctrl.updates)
	}
	if len(ctrl.syncs) != 1 {
		t.Fatalf("unexpected soft sync: %+v", ctrl.syncs)
	}

	ctrl.usage = []manager.UsageEntry{{Config: "route-1", UserID: "alice", UploadBytes: 10, DownloadBytes: 20}}
	panel.traffic = nil
	if err := agent.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(panel.traffic) != 1 || len(panel.traffic[0]) != 1 {
		t.Fatalf("reset traffic = %+v", panel.traffic)
	}
	if got := panel.traffic[0][0]; got.UploadBytes != 10 || got.DownloadBytes != 20 {
		t.Fatalf("reset delta = %+v", got)
	}
}

func TestPatchUsersSkipsNonMultiuser(t *testing.T) {
	settings := multiuserSettings(t, false)
	before := string(settings.Transport)
	changed, err := patchUsers(settings, []auth.UserSpec{{UserID: "alice", Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if changed || string(settings.Transport) != before {
		t.Fatal("non-multiuser config was modified")
	}
}

func TestPatchUsersFillsMultiChildren(t *testing.T) {
	raw := json.RawMessage(`{
		"strategy": "bond",
		"transports": [
			{"type": "raw_ws", "transport": {"multiuser": true, "room": "route-9"}},
			{"type": "silo", "transport": {"multiuser": true, "tunnel_id": "route-9"}},
			{"type": "livekit_datachannel", "transport": {"multiuser": false}}
		]
	}`)
	settings := &app.Settings{Type: "multi", Transport: raw}
	users := []auth.UserSpec{{UserID: "alice", Enabled: true}}

	changed, err := patchUsers(settings, users)
	if err != nil {
		t.Fatalf("patch users: %v", err)
	}
	if !changed {
		t.Fatal("multi config was not patched")
	}

	var payload struct {
		Transports []struct {
			Transport map[string]json.RawMessage `json:"transport"`
		} `json:"transports"`
	}
	if err := json.Unmarshal(settings.Transport, &payload); err != nil {
		t.Fatalf("decode patched transport: %v", err)
	}
	if len(payload.Transports) != 3 {
		t.Fatalf("subflows = %d, want 3", len(payload.Transports))
	}
	for i := 0; i < 2; i++ {
		var got []auth.UserSpec
		if err := json.Unmarshal(payload.Transports[i].Transport["users"], &got); err != nil {
			t.Fatalf("subflow %d users: %v", i, err)
		}
		if len(got) != 1 || got[0].UserID != "alice" {
			t.Fatalf("subflow %d users = %+v", i, got)
		}
	}
	if _, ok := payload.Transports[2].Transport["users"]; ok {
		t.Fatal("non-multiuser subflow was patched")
	}

	again, err := patchUsers(settings, users)
	if err != nil {
		t.Fatalf("second patch: %v", err)
	}
	if again {
		t.Fatal("identical users list was reported as a change")
	}
}

func TestStateKeysAreStable(t *testing.T) {
	keys := []string{stateKey("a", "u1"), stateKey("a", "u2"), stateKey("b", "u1")}
	sort.Strings(keys)
	if keys[0] != "a\x00u1" || keys[1] != "a\x00u2" || keys[2] != "b\x00u1" {
		t.Fatalf("unexpected keys: %q", keys)
	}
}

type fakeMetrics struct {
	metrics panelstore.NodeMetrics
	err     error
}

func (f fakeMetrics) Sample() (panelstore.NodeMetrics, error) { return f.metrics, f.err }

func TestSyncOnceReportsHostMetrics(t *testing.T) {
	panel := &fakePanel{}
	ctrl := &fakeControl{}
	metrics := fakeMetrics{metrics: panelstore.NodeMetrics{
		CPUPercent:     33.5,
		MemUsedBytes:   256 << 20,
		MemTotalBytes:  1024 << 20,
		DiskUsedBytes:  4 << 30,
		DiskTotalBytes: 32 << 30,
	}}
	agent := NewWithClients(Config{
		NodeID:    7,
		StateFile: t.TempDir() + "/state.json",
		Metrics:   metrics,
	}, panel, ctrl)
	if err := agent.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(panel.metrics) != 1 || panel.metrics[0] == nil {
		t.Fatalf("metrics were not sent: %+v", panel.metrics)
	}
	if got := panel.metrics[0]; got.CPUPercent != 33.5 || got.MemTotalBytes != 1024<<20 || got.DiskUsedBytes != 4<<30 {
		t.Fatalf("sent metrics = %+v", got)
	}
}

func TestSyncOnceSurvivesMetricsFailure(t *testing.T) {
	panel := &fakePanel{}
	ctrl := &fakeControl{}
	agent := NewWithClients(Config{
		NodeID:    7,
		StateFile: t.TempDir() + "/state.json",
		Metrics:   fakeMetrics{err: errors.New("no /proc")},
	}, panel, ctrl)
	if err := agent.SyncOnce(context.Background()); err != nil {
		t.Fatalf("metrics failure must not break sync: %v", err)
	}
	if len(panel.metrics) != 1 || panel.metrics[0] != nil {
		t.Fatalf("metrics after failure = %+v", panel.metrics)
	}
}

type failingControl struct{}

func (failingControl) Usage() ([]manager.UsageEntry, error) {
	return nil, errors.New("control socket is unavailable")
}
func (failingControl) List() ([]manager.EntryView, error) {
	return nil, errors.New("control socket is unavailable")
}
func (failingControl) Update(string, manager.ConfigEntry) error {
	return errors.New("control socket is unavailable")
}
func (failingControl) SyncUsers(string, manager.ConfigEntry) (bool, error) {
	return false, errors.New("control socket is unavailable")
}
func (failingControl) ActiveUserIDs() ([]string, error) {
	return nil, errors.New("control socket is unavailable")
}

func TestSyncOnceWithoutControlStillReportsMetrics(t *testing.T) {
	panel := &fakePanel{}
	metrics := fakeMetrics{metrics: panelstore.NodeMetrics{
		CPUPercent:     12,
		MemUsedBytes:   128 << 20,
		MemTotalBytes:  512 << 20,
		DiskUsedBytes:  1 << 30,
		DiskTotalBytes: 8 << 30,
	}}
	agent := NewWithClients(Config{
		NodeID:    9,
		StateFile: t.TempDir() + "/state.json",
		Metrics:   metrics,
	}, panel, failingControl{})
	if err := agent.SyncOnce(context.Background()); err != nil {
		t.Fatalf("relay heartbeat must survive a missing control socket: %v", err)
	}
	if len(panel.metrics) != 1 || panel.metrics[0] == nil {
		t.Fatalf("metrics were not sent: %+v", panel.metrics)
	}
	if got := panel.metrics[0]; got.CPUPercent != 12 || got.DiskTotalBytes != 8<<30 {
		t.Fatalf("sent metrics = %+v", got)
	}
	if len(panel.traffic) != 1 || len(panel.traffic[0]) != 0 {
		t.Fatalf("traffic = %+v, want an empty heartbeat", panel.traffic)
	}
}

func TestHTTPPanelSendsMetrics(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode: %v", err)
		}
		_, _ = w.Write([]byte(`{"node_id":3,"users":[]}`))
	}))
	defer server.Close()
	panel := NewHTTPPanel(server.URL, "secret")
	metrics := &panelstore.NodeMetrics{CPUPercent: 12.5, MemUsedBytes: 100, MemTotalBytes: 200}
	if _, err := panel.Sync(context.Background(), 3, nil, metrics, []string{"alice"}); err != nil {
		t.Fatal(err)
	}
	raw, ok := got["metrics"].(map[string]any)
	if !ok {
		t.Fatalf("metrics missing in payload: %+v", got)
	}
	if raw["cpu_percent"] != 12.5 || raw["mem_used_bytes"] != float64(100) {
		t.Fatalf("payload metrics = %+v", raw)
	}
	active, ok := got["active_user_ids"].([]any)
	if !ok || len(active) != 1 || active[0] != "alice" {
		t.Fatalf("payload active_user_ids = %+v", got["active_user_ids"])
	}
}

func TestSyncOnceSendsActiveUserIDs(t *testing.T) {
	panel := &fakePanel{}
	ctrl := &fakeControl{active: []string{"alice", "bob"}}
	agent := NewWithClients(Config{NodeID: 2, StateFile: t.TempDir() + "/state.json"}, panel, ctrl)
	if err := agent.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(panel.active) != 1 || len(panel.active[0]) != 2 || panel.active[0][0] != "alice" || panel.active[0][1] != "bob" {
		t.Fatalf("active_user_ids sent = %+v", panel.active)
	}
}

func TestApplyUsersFallsBackWhenSyncUsersRestarts(t *testing.T) {
	ctrl := &fakeControl{
		entries: []manager.EntryView{
			{Name: "route-1", Enabled: true, Settings: multiuserSettings(t, true)},
		},
		syncRestart: true,
	}
	agent := NewWithClients(Config{NodeID: 2, StateFile: t.TempDir() + "/state.json"}, &fakePanel{}, ctrl)
	restarted, err := agent.applyUsers([]auth.UserSpec{{UserID: "alice", Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ctrl.syncs) != 1 {
		t.Fatalf("sync-users calls = %+v", ctrl.syncs)
	}
	if len(ctrl.updates) != 0 {
		t.Fatalf("a restarted soft sync must not be repeated via Update: %+v", ctrl.updates)
	}
	if len(restarted) != 1 || restarted[0] != "route-1" {
		t.Fatalf("restarted = %v, want [route-1]", restarted)
	}
}

func TestApplyUsersFallsBackWhenSyncUsersFails(t *testing.T) {
	ctrl := &fakeControl{
		entries: []manager.EntryView{
			{Name: "route-1", Enabled: true, Settings: multiuserSettings(t, true)},
		},
		syncErr: errors.New("old server"),
	}
	agent := NewWithClients(Config{NodeID: 2, StateFile: t.TempDir() + "/state.json"}, &fakePanel{}, ctrl)
	restarted, err := agent.applyUsers([]auth.UserSpec{{UserID: "alice", Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ctrl.syncs) != 1 || len(ctrl.updates) != 1 {
		t.Fatalf("calls: syncs=%+v updates=%+v", ctrl.syncs, ctrl.updates)
	}
	if len(restarted) != 1 || restarted[0] != "route-1" {
		t.Fatalf("restarted = %v, want [route-1]", restarted)
	}
}

func TestUsersOnlyChangeDetectsNonUserEdits(t *testing.T) {
	before := json.RawMessage(`{"multiuser":true,"room":"r","users":[{"user_id":"alice"}]}`)
	sameUsers := json.RawMessage(`{"multiuser":true,"room":"r","users":[{"user_id":"bob","max_speed_kbps":64}]}`)
	if !usersOnlyChange(before, sameUsers) {
		t.Fatal("users-only change was not detected")
	}
	roomChanged := json.RawMessage(`{"multiuser":true,"room":"other","users":[{"user_id":"alice"}]}`)
	if usersOnlyChange(before, roomChanged) {
		t.Fatal("room change was treated as users-only")
	}
	if usersOnlyChange(before, json.RawMessage(`{`)) {
		t.Fatal("invalid JSON was treated as users-only")
	}
}
