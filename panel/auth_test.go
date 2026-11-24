package panel

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/control"
	"github.com/Esteviredzu/shampoore/internal/logging"
)

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind: "paneltest",
		Build: func(context.Context, *app.Settings, string, logging.ContextLogger, app.Values) (*app.BuildResult, error) {
			return &app.BuildResult{}, nil
		},
	})
	if err != nil {
		panic(err)
	}
}

type fakeManager struct {
	mu       sync.Mutex
	running  bool
	started  int
	stopped  int
	shutdown int
}

func (f *fakeManager) IsRunning() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.running
}

func (f *fakeManager) Start() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running = true
	f.started++
	return nil
}

func (f *fakeManager) Stop() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running = false
	f.stopped++
	return nil
}

func (f *fakeManager) Logs(int) []string { return nil }

func (f *fakeManager) counters() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started, f.stopped
}

func newTestAPI(t *testing.T, token string) (*API, *fakeManager, string) {
	t.Helper()
	dir := t.TempDir()
	sup := &fakeManager{}
	api := New(control.NewClient(filepath.Join(dir, "control.sock")), sup, logging.New("panel-test", false), token, dir, nil, "")
	return api, sup, dir
}

func perform(h http.Handler, method, target string, body string, mutate func(*http.Request)) *httptest.ResponseRecorder {
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if mutate != nil {
		mutate(req)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestEnsureToken(t *testing.T) {
	token, err := EnsureToken("")
	if err != nil {
		t.Fatal(err)
	}
	if len(token) != 64 {
		t.Fatalf("token length = %d, want 64", len(token))
	}
	raw, err := hex.DecodeString(token)
	if err != nil || len(raw) != 32 {
		t.Fatalf("token is not 32-byte hex: %v (%d bytes)", err, len(raw))
	}

	other, err := EnsureToken("")
	if err != nil {
		t.Fatal(err)
	}
	if other == token {
		t.Fatal("generated tokens must be unique")
	}

	if got, err := EnsureToken("provided"); err != nil || got != "provided" {
		t.Fatalf("EnsureToken(provided) = %q, %v", got, err)
	}
}

func TestHandlerRequiresAuth(t *testing.T) {
	api, _, _ := newTestAPI(t, "secret")
	h := api.Handler()

	if rec := perform(h, http.MethodGet, "/api/configs", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/configs without auth = %d, want 401", rec.Code)
	}
	if rec := perform(h, http.MethodGet, "/api/users", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/users without auth = %d, want 401", rec.Code)
	}
	if rec := perform(h, http.MethodPost, "/api/server/stop", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("POST /api/server/stop without auth = %d, want 401", rec.Code)
	}

	rec := perform(h, http.MethodGet, "/", "", nil)
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/login" {
		t.Fatalf("GET / without auth = %d Location=%q, want 303 /login", rec.Code, rec.Header().Get("Location"))
	}
	if rec := perform(h, http.MethodGet, "/app.js", "", nil); rec.Code != http.StatusSeeOther {
		t.Fatalf("GET /app.js without auth = %d, want 303", rec.Code)
	}

	if rec := perform(h, http.MethodGet, "/login", "", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Shampoore Panel") {
		t.Fatalf("GET /login = %d body=%q", rec.Code, rec.Body.String())
	}
	if rec := perform(h, http.MethodGet, "/login.html", "", nil); rec.Code != http.StatusOK {
		t.Fatalf("GET /login.html = %d, want 200", rec.Code)
	}

	if rec := perform(h, http.MethodGet, "/api/configs?token=secret", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("query token accepted: %d", rec.Code)
	}
	if rec := perform(h, http.MethodGet, "/api/configs", "", func(r *http.Request) {
		r.Header.Set("X-Panel-Token", "secret")
	}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("X-Panel-Token accepted: %d", rec.Code)
	}

	if rec := perform(h, http.MethodGet, "/api/configs", "", func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer secret")
	}); rec.Code != http.StatusOK {
		t.Fatalf("bearer auth = %d, want 200", rec.Code)
	}
	if rec := perform(h, http.MethodGet, "/", "", func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer secret")
	}); rec.Code != http.StatusOK {
		t.Fatalf("authenticated static = %d, want 200", rec.Code)
	}
}

func TestHandlerRejectsEmptyToken(t *testing.T) {
	api, _, _ := newTestAPI(t, "")
	h := api.Handler()
	if rec := perform(h, http.MethodGet, "/api/configs", "", func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer ")
	}); rec.Code != http.StatusInternalServerError {
		t.Fatalf("empty panel token = %d, want 500", rec.Code)
	}
}

func TestLoginSetsHttpOnlyCookie(t *testing.T) {
	api, _, _ := newTestAPI(t, "secret")
	h := api.Handler()

	if rec := perform(h, http.MethodPost, "/api/login", `{"token":"wrong"}`, nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token = %d, want 401", rec.Code)
	}

	rec := perform(h, http.MethodPost, "/api/login", `{"token":"secret"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("login = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies = %d, want 1", len(cookies))
	}
	cookie := cookies[0]
	if cookie.Name != authCookieName || cookie.Value != "secret" {
		t.Fatalf("unexpected cookie %q=%q", cookie.Name, cookie.Value)
	}
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" {
		t.Fatalf("cookie flags: HttpOnly=%v SameSite=%v Path=%q", cookie.HttpOnly, cookie.SameSite, cookie.Path)
	}

	if rec := perform(h, http.MethodGet, "/api/configs", "", func(r *http.Request) {
		r.AddCookie(&http.Cookie{Name: authCookieName, Value: "secret"})
	}); rec.Code != http.StatusOK {
		t.Fatalf("cookie auth = %d, want 200", rec.Code)
	}
}

func TestLoginRateLimit(t *testing.T) {
	api, _, _ := newTestAPI(t, "secret")
	h := api.Handler()

	blocked := false
	for i := 0; i < loginMaxFailures+2; i++ {
		rec := perform(h, http.MethodPost, "/api/login", `{"token":"wrong"}`, nil)
		if rec.Code == http.StatusTooManyRequests {
			blocked = true
			if rec.Header().Get("Retry-After") == "" {
				t.Fatal("429 response without Retry-After")
			}
			break
		}
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401 or 429", i, rec.Code)
		}
	}
	if !blocked {
		t.Fatal("limiter did not block failed attempts")
	}
	if rec := perform(h, http.MethodPost, "/api/login", `{"token":"secret"}`, nil); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("blocked login = %d, want 429", rec.Code)
	}
}

func TestConfigFilePermissions(t *testing.T) {
	api, _, dir := newTestAPI(t, "secret")
	h := api.Handler()

	content := `{"type":"paneltest","transport":{},"encryption_password":"secret"}`
	body, err := json.Marshal(map[string]string{"name": "cfg1", "content": content})
	if err != nil {
		t.Fatal(err)
	}
	rec := perform(h, http.MethodPost, "/api/configs", string(body), func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer secret")
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("add config = %d, want 201 (%s)", rec.Code, rec.Body.String())
	}

	configPath := filepath.Join(dir, "cfg1.json")
	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("config permissions = %o, want 600", perm)
	}

	stateInfo, err := os.Stat(filepath.Join(dir, ".state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := stateInfo.Mode().Perm(); perm != 0o600 {
		t.Fatalf("state permissions = %o, want 600", perm)
	}
}

func TestRestartStartsOfflineServer(t *testing.T) {
	api, sup, _ := newTestAPI(t, "secret")
	h := api.Handler()

	rec := perform(h, http.MethodPost, "/api/configs/demo/restart", "", func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer secret")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("restart = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	var body struct {
		OK      bool `json:"ok"`
		Started bool `json:"started"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.OK || !body.Started {
		t.Fatalf("restart response = %+v, want ok+started", body)
	}
	if started, _ := sup.counters(); started != 1 {
		t.Fatalf("start calls = %d, want 1", started)
	}
}
