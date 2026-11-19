package nodeagent

import (
	"context"
	"encoding/json"
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
}

func (f *fakePanel) Sync(_ context.Context, _ int64, traffic []panelstore.TrafficDelta) ([]auth.UserSpec, error) {
	f.traffic = append(f.traffic, append([]panelstore.TrafficDelta(nil), traffic...))
	return f.users, nil
}

type fakeControl struct {
	usage   []manager.UsageEntry
	entries []manager.EntryView
	updates []manager.ConfigEntry
}

func (f *fakeControl) Usage() ([]manager.UsageEntry, error) { return f.usage, nil }
func (f *fakeControl) List() ([]manager.EntryView, error)   { return f.entries, nil }
func (f *fakeControl) Update(_ string, entry manager.ConfigEntry) error {
	f.updates = append(f.updates, entry)
	return nil
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
	if len(ctrl.updates) != 1 || ctrl.updates[0].Name != "route-1" {
		t.Fatalf("updates = %+v", ctrl.updates)
	}
	var tr map[string]any
	if err := json.Unmarshal(ctrl.updates[0].Settings.Transport, &tr); err != nil {
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
	if len(ctrl.updates) != 1 {
		t.Fatalf("unexpected update: %+v", ctrl.updates)
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

func TestStateKeysAreStable(t *testing.T) {
	keys := []string{stateKey("a", "u1"), stateKey("a", "u2"), stateKey("b", "u1")}
	sort.Strings(keys)
	if keys[0] != "a\x00u1" || keys[1] != "a\x00u2" || keys[2] != "b\x00u1" {
		t.Fatalf("unexpected keys: %q", keys)
	}
}
