package manager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	_ "github.com/Esteviredzu/shampoore/internal/transport/raw_ws"
	"github.com/Esteviredzu/shampoore/internal/userlimit"
)

func speedSettings(t *testing.T, name, users string) *app.Settings {
	t.Helper()
	raw := `{"name":"` + name + `","type":"raw_ws","transport":{
		"server_url":"wss://ws.example.com","room":"` + name + `","password":"pw",
		"multiuser":true,"users":` + users + `}}`
	var s app.Settings
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("decode settings: %v", err)
	}
	if err := s.ApplyDefaults(); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	return &s
}

func TestSyncUserLimitsMirrorsConfigUsers(t *testing.T) {
	m := &Manager{
		entries: map[string]*ManagedEntry{
			"route-1": {
				Name: "route-1",
				settings: speedSettings(t, "route-1",
					`[{"user_id":"alice","enabled":true,"max_speed_kbps":512}]`),
			},
			"route-2": {
				Name: "route-2",
				settings: speedSettings(t, "route-2",
					`[{"user_id":"alice","enabled":true,"max_speed_kbps":1024},{"user_id":"bob","enabled":true}]`),
			},
		},
		limits: userlimit.New(),
	}

	m.syncUserLimits()
	if got := m.limits.Limit("alice"); got != 1024*1024 {
		t.Fatalf("alice limit = %d, want %d (the largest configured cap)", got, 1024*1024)
	}
	if got := m.limits.Limit("bob"); got != 0 {
		t.Fatalf("bob limit = %d, want 0", got)
	}
	if ids := m.ActiveUserIDs(); len(ids) != 0 {
		t.Fatalf("active users without traffic = %v", ids)
	}

	delete(m.entries, "route-2")
	m.syncUserLimits()
	if got := m.limits.Limit("alice"); got != 512*1024 {
		t.Fatalf("alice limit after config removal = %d, want %d", got, 512*1024)
	}
	if got := m.limits.Limit("bob"); got != 0 {
		t.Fatalf("bob limit after config removal = %d, want 0", got)
	}
}

func TestSyncUsersAppliesLimitWithoutRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "route-1.json")
	before := speedSettings(t, "route-1",
		`[{"user_id":"alice","enabled":true,"max_speed_kbps":128}]`)
	after := speedSettings(t, "route-1",
		`[{"user_id":"alice","enabled":true,"max_speed_kbps":4096}]`)

	m := &Manager{
		entries: map[string]*ManagedEntry{
			"route-1": {Name: "route-1", FilePath: path, settings: before},
		},
		limits: userlimit.New(),
	}
	m.syncUserLimits()
	if got := m.limits.Limit("alice"); got != 128*1024 {
		t.Fatalf("initial limit = %d", got)
	}

	restarted, err := m.SyncUsers("route-1", ConfigEntry{Name: "route-1", Settings: after})
	if err != nil {
		t.Fatalf("sync users: %v", err)
	}
	if restarted {
		t.Fatal("a users-only change must not restart the config")
	}
	if got := m.limits.Limit("alice"); got != 4096*1024 {
		t.Fatalf("limit after sync = %d, want %d", got, 4096*1024)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rewritten config: %v", err)
	}
	var stored app.Settings
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatalf("decode rewritten config: %v", err)
	}
	specs := app.UsersFromTransport(stored.Transport)
	if len(specs) != 1 || specs[0].MaxSpeedKBps != 4096 {
		t.Fatalf("rewritten config users = %+v, want alice/4096", specs)
	}

	m.mu.Lock()
	settings := m.entries["route-1"].settings
	m.mu.Unlock()
	if settings == nil || !sameTransportIgnoringUsers(settings.Transport, after.Transport) {
		t.Fatal("in-memory settings were not updated")
	}
}

func TestSameTransportIgnoringUsers(t *testing.T) {
	a := speedSettings(t, "route-1", `[{"user_id":"alice","enabled":true}]`)
	b := speedSettings(t, "route-1", `[{"user_id":"bob","enabled":true,"max_speed_kbps":10}]`)
	if !sameTransportIgnoringUsers(a.Transport, b.Transport) {
		t.Fatal("different user lists must look like a users-only change")
	}

	other := speedSettings(t, "route-1", `[{"user_id":"alice","enabled":true}]`)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(other.Transport, &raw); err != nil {
		t.Fatal(err)
	}
	raw["room"] = json.RawMessage(`"another-room"`)
	other.Transport, _ = json.Marshal(raw)
	if sameTransportIgnoringUsers(a.Transport, other.Transport) {
		t.Fatal("a room change must not look like a users-only change")
	}
}
