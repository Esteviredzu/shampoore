package panel

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/control"
	"github.com/Esteviredzu/shampoore/internal/logging"
)

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		current string
		latest  string
		want    int
	}{
		{"v2.0.0", "v2.1.0", -1},
		{"v2.1.0", "v2.2.0", -1},
		{"v2.2.1", "v2.2.1", 0},
		{"2.2.1", "v2.2.1", 0},
		{"v2.2.1", "v2.2.0", 1},
		{"v2.2.10", "v2.2.9", 1},
		{"v2", "v2.0.1", -1},
		{"v2.0.0-rc1", "v2.0.0", -1},
		{"v2.0.0", "v2.0.0-rc1", 1},
		{"v2.0.0-rc1", "v2.0.0-rc2", -1},
		{"v2.0.0+build.5", "v2.0.0", 0},
		{"", "v2.0.0", -1},
		{"v2.0.0", "", 1},
	}
	for _, test := range tests {
		if got := CompareVersions(test.current, test.latest); got != test.want {
			t.Errorf("CompareVersions(%q, %q) = %d, want %d", test.current, test.latest, got, test.want)
		}
	}
}

func TestParseGitHubReleaseSelectsPlatformAsset(t *testing.T) {
	payload := []byte(`{
		"tag_name": "v2.2.1",
		"body": "  Fixes and improvements  ",
		"html_url": "https://github.com/acme/shampoore-core/releases/tag/v2.2.1",
		"published_at": "2025-02-03T04:05:06Z",
		"assets": [
			{"name": "shampoore-linux-arm64.tar.gz", "browser_download_url": "https://example.com/arm64.tar.gz"},
			{"name": "shampoore-linux-amd64.tar.gz", "browser_download_url": "https://example.com/amd64.tar.gz"},
			{"name": "SHA256SUMS", "browser_download_url": "https://example.com/SHA256SUMS"}
		]
	}`)
	info, err := parseGitHubRelease(payload, "linux", "amd64")
	if err != nil {
		t.Fatalf("parseGitHubRelease: %v", err)
	}
	if info.TagName != "v2.2.1" || info.Notes != "Fixes and improvements" {
		t.Fatalf("release info = %+v", info)
	}
	if info.Asset.URL != "https://example.com/amd64.tar.gz" {
		t.Fatalf("asset url = %q", info.Asset.URL)
	}
	if info.Checksums.URL != "https://example.com/SHA256SUMS" {
		t.Fatalf("checksum url = %q", info.Checksums.URL)
	}
	if got := formatUpdateTime(info.PublishedAt); got != "2025-02-03T04:05:06Z" {
		t.Fatalf("published_at = %q", got)
	}

	missing, err := parseGitHubRelease(payload, "linux", "riscv64")
	if err != nil {
		t.Fatalf("parseGitHubRelease: %v", err)
	}
	if missing.Asset.URL != "" {
		t.Fatalf("unexpected asset for riscv64: %+v", missing.Asset)
	}
}

func TestParseGitHubReleaseRejectsEmptyTag(t *testing.T) {
	if _, err := parseGitHubRelease([]byte(`{"assets":[]}`), "linux", "amd64"); err == nil {
		t.Fatal("expected an error for a release without tag_name")
	}
}

func newUpdaterTestAPI(t *testing.T, opts ...Option) (*API, string) {
	t.Helper()
	dir := t.TempDir()
	base := []Option{WithPanelVersion("v2.0.0"), WithUpdateBinaryDir(dir), WithUpdateRepo("acme/shampoore-core")}
	base = append(base, opts...)
	api := New(
		control.NewClient(filepath.Join(dir, "control.sock")),
		&fakeManager{},
		logging.New("panel-update-test", false),
		"secret-token",
		dir,
		nil,
		"",
		base...,
	)
	api.updater.goos = "linux"
	api.updater.goarch = "amd64"
	return api, dir
}

func TestCheckSystemUpdateEndpoint(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/shampoore-core/releases/latest" {
			http.NotFound(w, r)
			return
		}
		atomic.AddInt32(&calls, 1)
		if got := r.Header.Get("User-Agent"); !strings.HasPrefix(got, "shampoore-panel/") {
			t.Errorf("unexpected User-Agent %q", got)
		}
		base := "http://" + r.Host
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name":     "v2.1.0",
			"body":         "Notes",
			"published_at": "2025-01-02T03:04:05Z",
			"assets": []map[string]any{
				{"name": "shampoore-linux-amd64.tar.gz", "browser_download_url": base + "/shampoore-linux-amd64.tar.gz"},
				{"name": "SHA256SUMS", "browser_download_url": base + "/SHA256SUMS"},
			},
		})
	}))
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	h := api.Handler()

	rec := perform(h, http.MethodGet, "/api/system/update", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET update = %d: %s", rec.Code, rec.Body.String())
	}
	var info UpdateInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.CurrentVersion != "v2.0.0" || info.LatestVersion != "v2.1.0" || !info.UpdateAvailable {
		t.Fatalf("update info = %+v", info)
	}
	if info.ReleaseNotes != "Notes" || info.PublishedAt != "2025-01-02T03:04:05Z" {
		t.Fatalf("update info = %+v", info)
	}
	if !strings.HasSuffix(info.AssetURL, "/shampoore-linux-amd64.tar.gz") {
		t.Fatalf("asset url = %q", info.AssetURL)
	}

	if rec := perform(h, http.MethodGet, "/api/system/update", "", authRequest()); rec.Code != http.StatusOK {
		t.Fatalf("cached GET = %d: %s", rec.Code, rec.Body.String())
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("GitHub calls = %d, want 1 (cache)", got)
	}

	api.updater.mu.Lock()
	api.updater.cachedAt = time.Now().Add(-2 * updateCacheTTL)
	api.updater.mu.Unlock()
	if rec := perform(h, http.MethodGet, "/api/system/update", "", authRequest()); rec.Code != http.StatusOK {
		t.Fatalf("refreshed GET = %d: %s", rec.Code, rec.Body.String())
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("GitHub calls after cache expiry = %d, want 2", got)
	}
}

func TestCheckSystemUpdateUpstreamFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rate limited", http.StatusForbidden)
	}))
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	rec := perform(api.Handler(), http.MethodGet, "/api/system/update", "", authRequest())
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("GET update = %d, want 502: %s", rec.Code, rec.Body.String())
	}
}

func TestSystemVersionEndpoint(t *testing.T) {
	api, _ := newUpdaterTestAPI(t)
	rec := perform(api.Handler(), http.MethodGet, "/api/system/version", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET version = %d: %s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Version string `json:"version"`
		OS      string `json:"os"`
		Arch    string `json:"arch"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Version != "v2.0.0" || payload.OS == "" || payload.Arch == "" {
		t.Fatalf("version payload = %+v", payload)
	}
}

func buildTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, name := range names {
		body := []byte(files[name])
		header := &tar.Header{Name: name, Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("tar header: %v", err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatalf("tar body: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func newReleaseBinaries(prefix string) map[string]string {
	files := make(map[string]string, len(updateBinaryNames))
	for _, name := range updateBinaryNames {
		files[prefix+name] = "new-" + name
	}
	return files
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func releaseServer(t *testing.T, archive []byte, checksums string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/acme/shampoore-core/releases/latest":
			base := "http://" + r.Host
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tag_name":     "v2.1.0",
				"body":         "Release notes for v2.1.0",
				"html_url":     "https://example.com/releases/v2.1.0",
				"published_at": "2025-01-02T03:04:05Z",
				"assets": []map[string]any{
					{"name": "shampoore-linux-amd64.tar.gz", "browser_download_url": base + "/download/shampoore-linux-amd64.tar.gz"},
					{"name": "SHA256SUMS", "browser_download_url": base + "/download/SHA256SUMS"},
				},
			})
		case "/download/shampoore-linux-amd64.tar.gz":
			_, _ = w.Write(archive)
		case "/download/SHA256SUMS":
			_, _ = w.Write([]byte(checksums))
		default:
			http.NotFound(w, r)
		}
	}))
}

func writeOldBinaries(t *testing.T, binDir string) map[string]string {
	t.Helper()
	old := make(map[string]string, len(updateBinaryNames))
	for _, name := range updateBinaryNames {
		content := "old-" + name
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(content), 0o755); err != nil {
			t.Fatalf("write old binary: %v", err)
		}
		old[name] = content
	}
	return old
}

func TestInstallUpdateReplacesBinariesAtomically(t *testing.T) {
	binDir := t.TempDir()
	writeOldBinaries(t, binDir)
	files := newReleaseBinaries("shampoore-linux-amd64/")
	archive := buildTarGz(t, files)
	checksums := fmt.Sprintf("%s  shampoore-linux-amd64.tar.gz\n", sha256Hex(archive))
	srv := releaseServer(t, archive, checksums)
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateBinaryDir(binDir), WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	api.updater.downloadClient = srv.Client()
	restarted := make(chan struct{}, 1)
	api.updater.mu.Lock()
	api.updater.restart = func() error {
		restarted <- struct{}{}
		return nil
	}
	api.updater.mu.Unlock()

	rec := perform(api.Handler(), http.MethodPost, "/api/system/update", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("POST update = %d: %s", rec.Code, rec.Body.String())
	}
	var result installResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.OK || result.LatestVersion != "v2.1.0" || !result.Restarting {
		t.Fatalf("install result = %+v", result)
	}
	if len(result.Updated) != len(updateBinaryNames) {
		t.Fatalf("updated = %v", result.Updated)
	}
	for _, name := range updateBinaryNames {
		path := filepath.Join(binDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if want := "new-" + name; string(data) != want {
			t.Fatalf("%s = %q, want %q", name, data, want)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o755 {
			t.Fatalf("%s mode = %o, want 0755", name, info.Mode().Perm())
		}
	}
	entries, err := os.ReadDir(binDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".new") {
			t.Fatalf("staged file was not renamed: %s", entry.Name())
		}
	}
	select {
	case <-restarted:
	case <-time.After(2 * time.Second):
		t.Fatal("panel restart was not scheduled")
	}
}

func TestInstallUpdateChecksumMismatchKeepsBinaries(t *testing.T) {
	binDir := t.TempDir()
	old := writeOldBinaries(t, binDir)
	archive := buildTarGz(t, newReleaseBinaries("shampoore-linux-amd64/"))
	checksums := fmt.Sprintf("%s  shampoore-linux-amd64.tar.gz\n", sha256Hex([]byte("corrupted")))
	srv := releaseServer(t, archive, checksums)
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateBinaryDir(binDir), WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	api.updater.downloadClient = srv.Client()
	api.updater.mu.Lock()
	api.updater.restart = func() error { return nil }
	api.updater.mu.Unlock()

	rec := perform(api.Handler(), http.MethodPost, "/api/system/update", "", authRequest())
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("POST update = %d, want 502: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "checksum mismatch") {
		t.Fatalf("unexpected error: %s", rec.Body.String())
	}
	for name, want := range old {
		data, err := os.ReadFile(filepath.Join(binDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if string(data) != want {
			t.Fatalf("%s was modified: %q", name, data)
		}
	}
}

func TestInstallUpdateRejectsConcurrentRun(t *testing.T) {
	api, _ := newUpdaterTestAPI(t)
	api.updater.installMu.Lock()
	rec := perform(api.Handler(), http.MethodPost, "/api/system/update", "", authRequest())
	api.updater.installMu.Unlock()
	if rec.Code != http.StatusConflict {
		t.Fatalf("POST update = %d, want 409: %s", rec.Code, rec.Body.String())
	}
}

func TestApplyUpdateBinariesStagingFailureKeepsOldBinaries(t *testing.T) {
	binDir := t.TempDir()
	old := writeOldBinaries(t, binDir)
	if err := os.Mkdir(filepath.Join(binDir, "shampoore-server.new"), 0o755); err != nil {
		t.Fatal(err)
	}
	binaries := make(map[string][]byte, len(updateBinaryNames))
	for _, name := range updateBinaryNames {
		binaries[name] = []byte("new-" + name)
	}
	if _, err := applyUpdateBinaries(binDir, binaries); err == nil {
		t.Fatal("expected a staging error")
	}
	for name, want := range old {
		data, err := os.ReadFile(filepath.Join(binDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if string(data) != want {
			t.Fatalf("%s was replaced despite a staging error", name)
		}
	}
	if _, err := os.Stat(filepath.Join(binDir, "shampoore-panel.new")); !os.IsNotExist(err) {
		t.Fatalf("staged file left behind: %v", err)
	}
}

func TestApplyUpdateBinariesRejectsUnknownBinary(t *testing.T) {
	if _, err := applyUpdateBinaries(t.TempDir(), map[string][]byte{"evil": []byte("x")}); err == nil {
		t.Fatal("expected an error for an unknown binary")
	}
}

func extractTestArchive(t *testing.T, archive []byte) (map[string][]byte, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "release.tar.gz")
	if err := os.WriteFile(path, archive, 0o600); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	return extractUpdateBinariesFromFile(path)
}

func TestExtractUpdateBinariesRejectsTraversal(t *testing.T) {
	files := newReleaseBinaries("shampoore-linux-amd64/")
	files["../../evil"] = "payload"
	archive := buildTarGz(t, files)
	if _, err := extractTestArchive(t, archive); err == nil {
		t.Fatal("expected a path traversal error")
	}
}

func TestExtractUpdateBinariesRequiresAllBinaries(t *testing.T) {
	files := newReleaseBinaries("shampoore-linux-amd64/")
	delete(files, "shampoore-linux-amd64/shampoore-relay")
	archive := buildTarGz(t, files)
	_, err := extractTestArchive(t, archive)
	if err == nil || !strings.Contains(err.Error(), "shampoore-relay") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseSHA256SUMS(t *testing.T) {
	sum := strings.Repeat("ab", 32)
	data := "# generated by release.sh\n" +
		strings.ToUpper(sum) + "  shampoore-linux-amd64.tar.gz\n" +
		sum + " *shampoore-linux-arm64.tar.gz\n" +
		"not-a-hash  ignored.tar.gz\n"
	sums, err := parseSHA256SUMS([]byte(data))
	if err != nil {
		t.Fatalf("parseSHA256SUMS: %v", err)
	}
	if sums["shampoore-linux-amd64.tar.gz"] != sum {
		t.Fatalf("amd64 sum = %q", sums["shampoore-linux-amd64.tar.gz"])
	}
	if sums["shampoore-linux-arm64.tar.gz"] != sum {
		t.Fatalf("arm64 sum = %q", sums["shampoore-linux-arm64.tar.gz"])
	}
	if _, ok := sums["ignored.tar.gz"]; ok {
		t.Fatal("malformed line was accepted")
	}
	if _, err := parseSHA256SUMS([]byte("garbage")); err == nil {
		t.Fatal("expected an error for malformed SHA256SUMS")
	}
}
