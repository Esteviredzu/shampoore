package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

func TestSubscriptionFollowsUserRouteOrder(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	topo.user.AllowedRouteIDs = []int64{topo.routes[2].ID, topo.routes[0].ID, topo.routes[1].ID}
	if _, err := store.UpdateUser(context.Background(), topo.user); err != nil {
		t.Fatal(err)
	}
	wantTypes := []string{"livekit_datachannel", "raw_ws", "silo"}

	rec := perform(api.PublicHandler(), http.MethodGet, "/sub/"+topo.user.SubToken, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("subscription = %d: %s", rec.Code, rec.Body.String())
	}
	var settings []app.Settings
	if err := json.Unmarshal(rec.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	if len(settings) != len(wantTypes) {
		t.Fatalf("settings = %d, want %d", len(settings), len(wantTypes))
	}
	for i, want := range wantTypes {
		if settings[i].Type != want {
			t.Fatalf("settings[%d].type = %q, want %q (order %v)", i, settings[i].Type, want, settings)
		}
	}

	rec = perform(api.Handler(), http.MethodGet, "/api/users/"+topo.user.ID+"/bundle", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("bundle = %d: %s", rec.Code, rec.Body.String())
	}
	var bundle OfflineBundle
	if err := json.Unmarshal(rec.Body.Bytes(), &bundle); err != nil {
		t.Fatal(err)
	}
	if len(bundle.Links) != len(wantTypes) {
		t.Fatalf("bundle links = %d, want %d", len(bundle.Links), len(wantTypes))
	}
	for i, link := range bundle.Links {
		parsed, err := app.ParseSource(link)
		if err != nil {
			t.Fatalf("bundle link %d: %v", i, err)
		}
		if parsed.Type != wantTypes[i] {
			t.Fatalf("bundle link[%d] type = %q, want %q", i, parsed.Type, wantTypes[i])
		}
	}
}

func TestUserPatchRouteOrderAndFields(t *testing.T) {
	api, store, _, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)
	order := []int64{topo.routes[2].ID, topo.routes[1].ID}

	body := `{"enabled":false,"allowed_route_ids":[` + itoa(order[0]) + `,` + itoa(order[1]) + `]}`
	rec := perform(api.Handler(), http.MethodPatch, "/api/users/"+topo.user.ID, body, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("patch user = %d: %s", rec.Code, rec.Body.String())
	}
	var updated struct {
		panelstore.User
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Enabled {
		t.Fatal("enabled=false was not applied")
	}
	if len(updated.AllowedRouteIDs) != 2 || updated.AllowedRouteIDs[0] != order[0] || updated.AllowedRouteIDs[1] != order[1] {
		t.Fatalf("route order = %v, want %v", updated.AllowedRouteIDs, order)
	}
	if updated.Name != topo.user.Name {
		t.Fatalf("patch changed the name to %q", updated.Name)
	}

	rec = perform(api.Handler(), http.MethodPatch, "/api/users/"+topo.user.ID, `{"route_ids":[`+itoa(topo.routes[0].ID)+`]}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("patch route_ids alias = %d: %s", rec.Code, rec.Body.String())
	}
	got, err := store.GetUser(context.Background(), topo.user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.AllowedRouteIDs) != 1 || got.AllowedRouteIDs[0] != topo.routes[0].ID {
		t.Fatalf("route_ids alias = %v", got.AllowedRouteIDs)
	}

	rec = perform(api.Handler(), http.MethodPatch, "/api/users/"+topo.user.ID, `{"expires_at":null}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("clear expiry = %d: %s", rec.Code, rec.Body.String())
	}
	got, _ = store.GetUser(context.Background(), topo.user.ID)
	if !got.ExpiresAt.IsZero() {
		t.Fatalf("expiry was not cleared: %v", got.ExpiresAt)
	}
}
