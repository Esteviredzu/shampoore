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

func TestNodeSyncTrafficAndUsers(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	public := api.PublicHandler()

	body := `{"node_id":` + itoa(topo.exit.ID) + `,"traffic":[{"user_id":"` + topo.user.ID + `","upload_bytes":100,"download_bytes":250}]}`
	withSecret := func(secret string) func(*http.Request) {
		return func(r *http.Request) {
			if secret != "" {
				r.Header.Set("Authorization", "Bearer "+secret)
			}
		}
	}
	if rec := perform(public, http.MethodPost, "/api/node/sync", body, withSecret("wrong")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad secret = %d, want 401", rec.Code)
	}
	rec := perform(public, http.MethodPost, "/api/node/sync", body, withSecret(topo.exit.NodeSecret))
	if rec.Code != http.StatusOK {
		t.Fatalf("sync = %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		NodeID int64           `json:"node_id"`
		Users  []auth.UserSpec `json:"users"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.NodeID != topo.exit.ID || len(resp.Users) != 1 {
		t.Fatalf("sync response = %+v", resp)
	}
	if len(resp.Users[0].AllowedExits) != 1 || resp.Users[0].AllowedExits[0] != itoa(topo.exit.ID) {
		t.Fatalf("allowed exits = %+v", resp.Users[0].AllowedExits)
	}
	got, _ := store.GetUser(context.Background(), topo.user.ID)
	if got.UsedUploadBytes != 100 || got.UsedDownloadBytes != 250 {
		t.Fatalf("usage = %+v", got)
	}

	rec = perform(public, http.MethodPost, "/api/node/sync", `{"node_id":999,"traffic":[]}`, func(r *http.Request) {
		r.Header.Set("X-Node-Secret", topo.exit.NodeSecret)
	})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("node mismatch = %d, want 403", rec.Code)
	}
	negative := `{"traffic":[{"user_id":"` + topo.user.ID + `","upload_bytes":-5}]}`
	rec = perform(public, http.MethodPost, "/api/node/sync", negative, func(r *http.Request) {
		r.Header.Set("X-Node-Secret", topo.exit.NodeSecret)
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("negative delta = %d, want 400", rec.Code)
	}

	topo.user.ExpiresAt = time.Now().Add(-time.Hour)
	if _, err := store.UpdateUser(context.Background(), topo.user); err != nil {
		t.Fatal(err)
	}
	rec = perform(public, http.MethodPost, "/api/node/sync", body, withSecret(topo.exit.NodeSecret))
	resp.Users = nil
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Users) != 0 {
		t.Fatalf("expired user returned: %+v", resp.Users)
	}
}

func TestNodeSyncStoresHostMetrics(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	public := api.PublicHandler()

	body := `{"node_id":` + itoa(topo.exit.ID) + `,"traffic":[],"metrics":{"cpu_percent":37.5,"mem_used_bytes":536870912,"mem_total_bytes":1073741824,"disk_used_bytes":10737418240,"disk_total_bytes":42949672960}}`
	rec := perform(public, http.MethodPost, "/api/node/sync", body, func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+topo.exit.NodeSecret)
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("sync with metrics = %d: %s", rec.Code, rec.Body.String())
	}
	node, err := store.GetNode(context.Background(), topo.exit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if node.CPUPercent != 37.5 || node.MemUsedBytes != 536870912 || node.MemTotalBytes != 1073741824 {
		t.Fatalf("stored metrics = %+v", node)
	}
	if node.DiskUsedBytes != 10737418240 || node.DiskTotalBytes != 42949672960 {
		t.Fatalf("stored disk metrics = %+v", node)
	}
	if node.LastSeen.IsZero() {
		t.Fatal("last_seen was not stored")
	}
	if node.Status != panelstore.NodeStatusOnline {
		t.Fatalf("status after heartbeat = %q", node.Status)
	}

	invalid := `{"node_id":` + itoa(topo.exit.ID) + `,"traffic":[],"metrics":{"cpu_percent":250}}`
	rec = perform(public, http.MethodPost, "/api/node/sync", invalid, func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+topo.exit.NodeSecret)
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid metrics = %d, want 400: %s", rec.Code, rec.Body.String())
	}
}
func TestRelayNodeHeartbeatStoresAndServesMetrics(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	if topo.relay.NodeSecret == "" {
		t.Fatal("relay node secret was not generated")
	}
	public := api.PublicHandler()

	createdAt := topo.relay.UpdatedAt
	body := `{"node_id":` + itoa(topo.relay.ID) + `,"traffic":[],"metrics":{"cpu_percent":21.5,"mem_used_bytes":134217728,"mem_total_bytes":536870912,"disk_used_bytes":1073741824,"disk_total_bytes":8589934592}}`
	rec := perform(public, http.MethodPost, "/api/node/sync", body, func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+topo.relay.NodeSecret)
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("relay heartbeat = %d: %s", rec.Code, rec.Body.String())
	}
	node, err := store.GetNode(context.Background(), topo.relay.ID)
	if err != nil {
		t.Fatal(err)
	}
	if node.CPUPercent != 21.5 || node.MemUsedBytes != 134217728 || node.MemTotalBytes != 536870912 {
		t.Fatalf("relay metrics = %+v", node)
	}
	if node.DiskUsedBytes != 1073741824 || node.DiskTotalBytes != 8589934592 {
		t.Fatalf("relay disk metrics = %+v", node)
	}
	if node.LastSeen.IsZero() || node.Status != panelstore.NodeStatusOnline {
		t.Fatalf("relay heartbeat state = %+v", node)
	}
	if !node.UpdatedAt.Equal(createdAt) {
		t.Fatalf("heartbeat with metrics moved updated_at: %v -> %v", createdAt, node.UpdatedAt)
	}

	rec = perform(public, http.MethodPost, "/api/node/sync", `{"node_id":`+itoa(topo.relay.ID)+`,"traffic":[]}`, func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+topo.relay.NodeSecret)
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics-less heartbeat = %d: %s", rec.Code, rec.Body.String())
	}
	afterTouch, err := store.GetNode(context.Background(), topo.relay.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !afterTouch.UpdatedAt.Equal(createdAt) {
		t.Fatalf("metrics-less heartbeat moved updated_at: %v -> %v", createdAt, afterTouch.UpdatedAt)
	}
	if !afterTouch.LastSeen.After(node.LastSeen) {
		t.Fatalf("metrics-less heartbeat did not move last_seen: %v", afterTouch.LastSeen)
	}

	rec = perform(api.Handler(), http.MethodGet, "/api/nodes", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("list nodes = %d: %s", rec.Code, rec.Body.String())
	}
	var nodes []panelstore.Node
	if err := json.Unmarshal(rec.Body.Bytes(), &nodes); err != nil {
		t.Fatalf("decode nodes: %v", err)
	}
	var found *panelstore.Node
	for i := range nodes {
		if nodes[i].ID == topo.relay.ID {
			found = &nodes[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("relay node missing from /api/nodes: %+v", nodes)
	}
	if found.CPUPercent != 21.5 || found.MemTotalBytes != 536870912 || found.DiskTotalBytes != 8589934592 {
		t.Fatalf("/api/nodes telemetry = %+v", found)
	}
	if found.LastSeen.IsZero() {
		t.Fatal("/api/nodes did not expose last_seen")
	}
	if !found.UpdatedAt.Equal(createdAt) {
		t.Fatalf("/api/nodes updated_at changed on heartbeat: %v -> %v", createdAt, found.UpdatedAt)
	}
}
