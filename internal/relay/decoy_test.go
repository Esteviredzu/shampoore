package relay

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func getBody(t *testing.T, srv *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return resp.StatusCode, string(body)
}

func writeDecoySite(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, data := range files {
		full := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestDecoySiteAbsentReturns404(t *testing.T) {
	srv := httptest.NewServer(New(""))
	defer srv.Close()
	s := srv.Config.Handler.(*Server)
	if s.DecoyDir() != "" {
		t.Fatalf("unexpected decoy dir %q", s.DecoyDir())
	}

	for _, path := range []string{"/", "/index.html", "/style.css", "/app.js", "/missing/asset.png"} {
		status, body := getBody(t, srv, path)
		if status != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404 (body=%q)", path, status, body)
		}
	}

	resp, err := http.Post(srv.URL+"/", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("post status = %d, want 404", resp.StatusCode)
	}
}

func TestDecoyArchiveMissingIndexReturns404(t *testing.T) {
	dir := writeDecoySite(t, map[string]string{
		"style.css": "body{background:#000}",
	})
	srv := httptest.NewServer(New(""))
	defer srv.Close()
	srv.Config.Handler.(*Server).SetDecoyDir(dir)

	for _, path := range []string{"/", "/style.css"} {
		status, body := getBody(t, srv, path)
		if status != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404 (body=%q)", path, status, body)
		}
	}
}

func TestDecoySiteServedFromDirectory(t *testing.T) {
	dir := writeDecoySite(t, map[string]string{
		"index.html":       "<html>external-index</html>",
		"style.css":        "body{background:#000}",
		"assets/app.js":    "console.log('decoy')",
		"assets/logo.json": `{"ok":true}`,
	})

	srv := httptest.NewServer(New("secret"))
	defer srv.Close()
	srv.Config.Handler.(*Server).SetDecoyDir(dir)

	status, body := getBody(t, srv, "/")
	if status != http.StatusOK || !strings.Contains(body, "external-index") {
		t.Fatalf("index: status=%d body=%q", status, body)
	}
	status, body = getBody(t, srv, "/index.html")
	if status != http.StatusOK || !strings.Contains(body, "external-index") {
		t.Fatalf("explicit index: status=%d body=%q", status, body)
	}
	status, body = getBody(t, srv, "/style.css")
	if status != http.StatusOK || !strings.Contains(body, "background") {
		t.Fatalf("style: status=%d body=%q", status, body)
	}
	status, body = getBody(t, srv, "/assets/app.js")
	if status != http.StatusOK || !strings.Contains(body, "decoy") {
		t.Fatalf("nested asset: status=%d body=%q", status, body)
	}

	for _, path := range []string{"/missing/asset.png", "/app.js"} {
		status, body = getBody(t, srv, path)
		if status != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404 (body=%q)", path, status, body)
		}
	}
}

func TestDecoyDirRejectsTraversal(t *testing.T) {
	dir := writeDecoySite(t, map[string]string{
		"index.html": "INDEX",
	})
	srv := httptest.NewServer(New(""))
	defer srv.Close()
	srv.Config.Handler.(*Server).SetDecoyDir(dir)

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.URL.Path = "/../../etc/passwd"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("traversal request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNotFound || strings.Contains(string(body), "root:") {
		t.Fatalf("traversal leaked: status=%d body=%q", resp.StatusCode, body)
	}
}

func TestDecoyRejectsNonGETWhenSiteConfigured(t *testing.T) {
	dir := writeDecoySite(t, map[string]string{
		"index.html": "<html>site</html>",
	})
	srv := httptest.NewServer(New(""))
	defer srv.Close()
	srv.Config.Handler.(*Server).SetDecoyDir(dir)

	resp, err := http.Post(srv.URL+"/", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestDecoyDirDisabledOnReset(t *testing.T) {
	dir := writeDecoySite(t, map[string]string{
		"index.html": "<html>site</html>",
	})
	s := New("")
	s.SetDecoyDir(dir)
	if status := serveDecoyStatus(t, s, "/"); status != http.StatusOK {
		t.Fatalf("status with site = %d, want 200", status)
	}
	s.SetDecoyDir("  ")
	if status := serveDecoyStatus(t, s, "/"); status != http.StatusNotFound {
		t.Fatalf("status after reset = %d, want 404", status)
	}
}

func serveDecoyStatus(t *testing.T, s *Server, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	s.ServeHTTP(rec, req)
	return rec.Code
}
