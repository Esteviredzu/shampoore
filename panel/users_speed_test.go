package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type userAPIRow struct {
	panelstore.User
	Online          bool  `json:"online"`
	CurrentSpeedBPS int64 `json:"current_speed_bps"`
}

func findUserRow(t *testing.T, rows []userAPIRow, id string) userAPIRow {
	t.Helper()
	for _, row := range rows {
		if row.ID == id {
			return row
		}
	}
	t.Fatalf("user %s not found in %+v", id, rows)
	return userAPIRow{}
}

func TestUserMaxSpeedAPIRoundTrip(t *testing.T) {
	api, _, _, _ := newControlTestAPI(t)
	h := api.Handler()

	rec := perform(h, http.MethodPost, "/api/users", `{"name":"Fast","enabled":true,"max_speed_kbps":512}`, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create user = %d: %s", rec.Code, rec.Body.String())
	}
	var created userAPIRow
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.MaxSpeedKBps != 512 {
		t.Fatalf("created max_speed_kbps = %d, want 512", created.MaxSpeedKBps)
	}
	if created.Online {
		t.Fatal("a fresh user must not be online")
	}

	rec = perform(h, http.MethodGet, "/api/users", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("list users = %d: %s", rec.Code, rec.Body.String())
	}
	var rows []userAPIRow
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	row := findUserRow(t, rows, created.ID)
	if row.MaxSpeedKBps != 512 || row.Online {
		t.Fatalf("listed user = %+v, want 512 KB/s and offline", row)
	}

	rec = perform(h, http.MethodPatch, "/api/users/"+created.ID, `{"max_speed_kbps":0}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("reset speed = %d: %s", rec.Code, rec.Body.String())
	}
	var updated userAPIRow
	if err := json.Unmarshal(rec.Body.Bytes(), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.MaxSpeedKBps != 0 {
		t.Fatalf("max_speed_kbps after reset = %d, want 0", updated.MaxSpeedKBps)
	}

	rec = perform(h, http.MethodPatch, "/api/users/"+created.ID, `{"max_speed_kbps":-1}`, authRequest())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("negative speed = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	rec = perform(h, http.MethodPost, "/api/users", `{"name":"Bad","max_speed_kbps":-1}`, authRequest())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("negative create = %d, want 400: %s", rec.Code, rec.Body.String())
	}
}

func TestNodeSyncReportsOnlineUsersAndSpeedLimit(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	user := topo.user
	user.MaxSpeedKBps = 2048
	if _, err := store.UpdateUser(context.Background(), user); err != nil {
		t.Fatal(err)
	}

	body := `{"node_id":` + itoa(topo.exit.ID) +
		`,"traffic":[],"active_user_ids":["` + user.ID + `","ghost-user"]}`
	rec := perform(api.PublicHandler(), http.MethodPost, "/api/node/sync", body, func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+topo.exit.NodeSecret)
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("sync = %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Users []auth.UserSpec `json:"users"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Users) != 1 {
		t.Fatalf("sync users = %+v", resp.Users)
	}
	if resp.Users[0].MaxSpeedKBps != 2048 {
		t.Fatalf("synced max_speed_kbps = %d, want 2048", resp.Users[0].MaxSpeedKBps)
	}

	rec = perform(api.Handler(), http.MethodGet, "/api/users", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("list users = %d: %s", rec.Code, rec.Body.String())
	}
	var rows []userAPIRow
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	row := findUserRow(t, rows, user.ID)
	if !row.Online {
		t.Fatalf("user was not marked online: %+v", row)
	}
	if row.LastSeenNode != topo.exit.ID {
		t.Fatalf("last_seen_node = %d, want %d", row.LastSeenNode, topo.exit.ID)
	}
	if row.LastSeenAt.IsZero() {
		t.Fatal("last_seen_at was not stored")
	}

	stored, err := store.GetUser(context.Background(), user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Online(stored.LastSeenAt) {
		t.Fatalf("stored user is not online at its own timestamp: %+v", stored)
	}
	if stored.Online(stored.LastSeenAt.Add(panelstore.UserOnlineWindow + time.Second)) {
		t.Fatal("online window is not applied")
	}
}

func TestUserCurrentSpeedInUserList(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	now := time.Now()

	api.speed.Observe(topo.exit.ID, nil, now.Add(-4*time.Second))
	api.speed.Observe(topo.exit.ID, []panelstore.TrafficDelta{{
		UserID:        topo.user.ID,
		UploadBytes:   512,
		DownloadBytes: 1536,
	}}, now.Add(-2*time.Second))

	rec := perform(api.Handler(), http.MethodGet, "/api/users", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("list users = %d: %s", rec.Code, rec.Body.String())
	}
	var rows []userAPIRow
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	row := findUserRow(t, rows, topo.user.ID)
	if row.CurrentSpeedBPS != 1024 {
		t.Fatalf("current_speed_bps = %d, want 1024", row.CurrentSpeedBPS)
	}
	if row.Online {
		t.Fatalf("speed sample must not mark the user online: %+v", row)
	}
}

func TestNodeSyncFeedsLiveSpeed(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	body := `{"node_id":` + itoa(topo.exit.ID) +
		`,"traffic":[{"user_id":"` + topo.user.ID + `","download_bytes":8192}],` +
		`"active_user_ids":["` + topo.user.ID + `"]}`
	sync := func() {
		t.Helper()
		rec := perform(api.PublicHandler(), http.MethodPost, "/api/node/sync", body, func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+topo.exit.NodeSecret)
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("sync = %d: %s", rec.Code, rec.Body.String())
		}
	}

	sync()
	sync()

	rec := perform(api.Handler(), http.MethodGet, "/api/users", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("list users = %d: %s", rec.Code, rec.Body.String())
	}
	var rows []userAPIRow
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	row := findUserRow(t, rows, topo.user.ID)
	if !row.Online {
		t.Fatalf("user was not marked online: %+v", row)
	}
	if row.CurrentSpeedBPS <= 0 {
		t.Fatalf("current_speed_bps = %d, want > 0 after two syncs", row.CurrentSpeedBPS)
	}
}
