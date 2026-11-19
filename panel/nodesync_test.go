package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/auth"
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
