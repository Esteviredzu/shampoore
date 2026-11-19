package panel

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

func TestSubscriptionRejectsExpiredAndOverLimit(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	public := api.PublicHandler()

	topo.user.ExpiresAt = time.Now().Add(-time.Hour)
	if _, err := store.UpdateUser(context.Background(), topo.user); err != nil {
		t.Fatal(err)
	}
	rec := perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expired subscription = %d, want 403", rec.Code)
	}

	topo.user.ExpiresAt = time.Time{}
	topo.user.TrafficLimitBytes = 100
	if _, err := store.UpdateUser(context.Background(), topo.user); err != nil {
		t.Fatal(err)
	}
	if err := store.AddTraffic(context.Background(), []panelstore.TrafficDelta{{
		UserID: topo.user.ID, UploadBytes: 60, DownloadBytes: 50,
	}}); err != nil {
		t.Fatal(err)
	}
	rec = perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("over-limit subscription = %d, want 403", rec.Code)
	}
	rec = perform(public, http.MethodGet, "/sub/deadbeefdeadbeefdeadbeefdeadbeef", "", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown subscription = %d, want 404", rec.Code)
	}
}

func TestSubscriptionConcurrent(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	public := api.PublicHandler()
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := perform(public, http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
			if rec.Code != http.StatusOK {
				t.Errorf("concurrent subscription = %d", rec.Code)
			}
		}()
	}
	wg.Wait()
}

func TestPublicHandlerNoStore(t *testing.T) {
	api, _, _, _ := newControlTestAPI(t)
	api.store = nil
	public := api.PublicHandler()
	rec := perform(public, http.MethodGet, "/sub/aaaaaaaaaaaaaaaa", "", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("no store = %d, want 503", rec.Code)
	}
	rec = perform(public, http.MethodGet, "/api/nodes", "", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("public admin route without store = %d, want 404", rec.Code)
	}
}
