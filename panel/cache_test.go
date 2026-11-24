package panel

import (
	"net/http"
	"testing"
)

func TestStaticAssetsAreNotCached(t *testing.T) {
	api, _, _, _ := newControlTestAPI(t)
	h := api.Handler()

	for _, path := range []string{"/", "/app.js", "/styles.css", "/locales/ru.json"} {
		rec := perform(h, http.MethodGet, path, "", authRequest())
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", path, rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Cache-Control"); got != "no-store, max-age=0" {
			t.Fatalf("%s Cache-Control = %q, want no-store", path, got)
		}
		if got := rec.Header().Get("Pragma"); got != "no-cache" {
			t.Fatalf("%s Pragma = %q, want no-cache", path, got)
		}
	}

	rec := perform(h, http.MethodGet, "/api/nodes", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/nodes = %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store, max-age=0" {
		t.Fatalf("/api/nodes Cache-Control = %q, want no-store", got)
	}
}
