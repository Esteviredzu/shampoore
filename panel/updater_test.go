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
		{"v0.33", "v0.33.0", 0},
		{"v2.0.0-rc1", "v2.0.0", -1},
		{"v2.0.0", "v2.0.0-rc1", 1},
		{"v2.0.0-rc1", "v2.0.0-rc2", -1},

		{"v0.33.0-beta.1", "v0.33.0-beta.2", -1},
		{"v0.33.0-beta.2", "v0.33.0-beta.1", 1},
		{"v0.33.0-beta.9", "v0.33.0-beta.10", -1},
		{"v0.33.0-beta.1", "v0.33.0", -1},
		{"v0.33.0", "v0.33.0-beta.2", 1},
		{"v0.33.0", "v0.34.0-beta.1", -1},
		{"v0.33.0-beta.3", "v0.34.0-beta.1", -1},
		{"v0.33.0-beta.3", "v0.33.0-beta.3", 0},
		{"v2.0.0+build.5", "v2.0.0", 0},

		{"dev", "v2.0.0", -1},
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

func TestParseGitHubReleasesSkipsDraftsAndDetectsPrerelease(t *testing.T) {
	payload := []byte(`[
		{"tag_name":"v0.33.0","draft":false,"prerelease":false,"assets":[]},
		{"tag_name":"v0.34.0-beta.1","draft":false,"prerelease":true,"assets":[]},
		{"tag_name":"v0.35.0-rc.1","draft":false,"prerelease":false,"assets":[]},
		{"tag_name":"v0.36.0","draft":true,"prerelease":false,"assets":[]},
		{"tag_name":"","draft":false,"prerelease":false,"assets":[]}
	]`)
	releases, err := parseGitHubReleases(payload, "linux", "amd64")
	if err != nil {
		t.Fatalf("parseGitHubReleases: %v", err)
	}
	if len(releases) != 3 {
		t.Fatalf("releases = %+v, want 3 (draft and empty tag skipped)", releases)
	}
	if releases[0].Prerelease || !releases[1].Prerelease {
		t.Fatalf("prerelease flags = %+v", releases)
	}

	if !releases[2].Prerelease {
		t.Fatalf("v0.35.0-rc.1 was not detected as prerelease: %+v", releases[2])
	}
}

func mustRelease(t *testing.T, tag string, prerelease bool) releaseInfo {
	t.Helper()
	release, ok := releaseInfoFromGitHub(githubRelease{TagName: tag, Prerelease: prerelease}, "linux", "amd64")
	if !ok {
		t.Fatalf("releaseInfoFromGitHub(%q) failed", tag)
	}
	return *release
}

func TestSelectUpdateCandidates(t *testing.T) {
	tests := []struct {
		name       string
		current    string
		releases   []releaseInfo
		wantStable string
		wantBeta   string
	}{
		{
			name:    "old stable sees both channels",
			current: "v0.32.0",
			releases: []releaseInfo{
				mustRelease(t, "v0.32.0", false),
				mustRelease(t, "v0.33.0", false),
				mustRelease(t, "v0.34.0-beta.1", true),
				mustRelease(t, "v0.34.0-beta.2", true),
			},
			wantStable: "v0.33.0",
			wantBeta:   "v0.34.0-beta.2",
		},
		{
			name:    "latest stable sees only a newer beta",
			current: "v0.33.0",
			releases: []releaseInfo{
				mustRelease(t, "v0.33.0", false),
				mustRelease(t, "v0.33.0-beta.5", true),
				mustRelease(t, "v0.34.0-beta.1", true),
			},
			wantStable: "",
			wantBeta:   "v0.34.0-beta.1",
		},
		{
			name:    "beta.1 sees beta.2",
			current: "v0.33.0-beta.1",
			releases: []releaseInfo{
				mustRelease(t, "v0.32.0", false),
				mustRelease(t, "v0.33.0-beta.1", true),
				mustRelease(t, "v0.33.0-beta.2", true),
			},
			wantStable: "",
			wantBeta:   "v0.33.0-beta.2",
		},
		{
			name:    "beta.1 sees final stable but drops older beta",
			current: "v0.33.0-beta.1",
			releases: []releaseInfo{
				mustRelease(t, "v0.33.0-beta.2", true),
				mustRelease(t, "v0.33.0", false),
			},
			wantStable: "v0.33.0",
			wantBeta:   "",
		},
		{
			name:    "latest beta has no updates",
			current: "v0.33.0-beta.3",
			releases: []releaseInfo{
				mustRelease(t, "v0.33.0-beta.1", true),
				mustRelease(t, "v0.33.0-beta.2", true),
				mustRelease(t, "v0.33.0-beta.3", true),
			},
			wantStable: "",
			wantBeta:   "",
		},
		{
			name:    "stable newer than beta wins and hides the beta",
			current: "v0.32.0",
			releases: []releaseInfo{
				mustRelease(t, "v0.33.0", false),
				mustRelease(t, "v0.33.0-beta.2", true),
				mustRelease(t, "v0.32.5-beta.1", true),
			},
			wantStable: "v0.33.0",
			wantBeta:   "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stable, beta := selectUpdateCandidates(test.current, test.releases)
			gotStable, gotBeta := "", ""
			if stable != nil {
				gotStable = stable.TagName
			}
			if beta != nil {
				gotBeta = beta.TagName
			}
			if gotStable != test.wantStable || gotBeta != test.wantBeta {
				t.Fatalf("selectUpdateCandidates(%s) = stable %q, beta %q; want %q, %q",
					test.current, gotStable, gotBeta, test.wantStable, test.wantBeta)
			}
		})
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
		if r.URL.Path != "/repos/acme/shampoore-core/releases" {
			http.NotFound(w, r)
			return
		}
		atomic.AddInt32(&calls, 1)
		if got := r.Header.Get("User-Agent"); !strings.HasPrefix(got, "shampoore-panel/") {
			t.Errorf("unexpected User-Agent %q", got)
		}
		base := "http://" + r.Host
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{
				"tag_name":     "v2.1.0",
				"body":         "Notes",
				"published_at": "2025-01-02T03:04:05Z",
				"prerelease":   false,
				"assets": []map[string]any{
					{"name": "shampoore-linux-amd64.tar.gz", "browser_download_url": base + "/shampoore-linux-amd64.tar.gz"},
					{"name": "SHA256SUMS", "browser_download_url": base + "/SHA256SUMS"},
				},
			},
			{
				"tag_name":     "v2.2.0-beta.1",
				"body":         "Beta notes",
				"published_at": "2025-02-02T03:04:05Z",
				"prerelease":   true,
				"assets": []map[string]any{
					{"name": "shampoore-linux-amd64.tar.gz", "browser_download_url": base + "/beta/shampoore-linux-amd64.tar.gz"},
				},
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
	if info.LatestStable == nil || info.LatestStable.TagName != "v2.1.0" {
		t.Fatalf("latest_stable = %+v", info.LatestStable)
	}
	if info.LatestBeta == nil || info.LatestBeta.TagName != "v2.2.0-beta.1" {
		t.Fatalf("latest_beta = %+v", info.LatestBeta)
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

func TestCheckSystemUpdateBetaOnly(t *testing.T) {
	srv := updateReleaseServer(t, []fakeRelease{
		{Tag: "v2.0.0", Body: "current stable", Published: "2025-01-01T00:00:00Z"},
		{Tag: "v2.1.0-beta.1", Body: "new beta", Published: "2025-02-01T00:00:00Z", Prerelease: true},
	})
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	rec := perform(api.Handler(), http.MethodGet, "/api/system/update", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET update = %d: %s", rec.Code, rec.Body.String())
	}
	var info UpdateInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.LatestStable != nil {
		t.Fatalf("latest_stable = %+v, want nil", info.LatestStable)
	}
	if info.LatestBeta == nil || info.LatestBeta.TagName != "v2.1.0-beta.1" {
		t.Fatalf("latest_beta = %+v", info.LatestBeta)
	}
	if !info.UpdateAvailable || info.LatestVersion != "v2.1.0-beta.1" || !info.Prerelease {
		t.Fatalf("update info = %+v", info)
	}
}

func TestCheckSystemUpdateUpToDate(t *testing.T) {
	srv := updateReleaseServer(t, []fakeRelease{
		{Tag: "v2.0.0", Body: "stable", Published: "2025-01-01T00:00:00Z"},
		{Tag: "v2.0.0-beta.2", Body: "older beta", Published: "2024-12-01T00:00:00Z", Prerelease: true},
	})
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	rec := perform(api.Handler(), http.MethodGet, "/api/system/update", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET update = %d: %s", rec.Code, rec.Body.String())
	}
	var info UpdateInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.LatestStable != nil || info.LatestBeta != nil || info.UpdateAvailable {
		t.Fatalf("update info = %+v, want no candidates", info)
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

func releaseBinaries(marker string) map[string]string {
	files := make(map[string]string, len(updateBinaryNames))
	for _, name := range updateBinaryNames {
		files["shampoore-linux-amd64/"+name] = marker + name
	}
	return files
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type fakeRelease struct {
	Tag        string
	Body       string
	Published  string
	Prerelease bool
	Draft      bool
	Archive    []byte
	Checksums  string

	Assets []map[string]any
}

func updateReleaseServer(t *testing.T, releases []fakeRelease) *httptest.Server {
	t.Helper()
	archives := make(map[string][]byte, len(releases))
	checksums := make(map[string]string, len(releases))
	for _, release := range releases {
		archives[release.Tag] = release.Archive
		checksums[release.Tag] = release.Checksums
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/acme/shampoore-core/releases" {
			base := "http://" + r.Host
			payload := make([]map[string]any, 0, len(releases))
			for _, release := range releases {
				assets := release.Assets
				if assets == nil {
					assets = []map[string]any{
						{"name": "shampoore-linux-amd64.tar.gz", "browser_download_url": base + "/download/" + release.Tag + "/shampoore-linux-amd64.tar.gz"},
						{"name": "SHA256SUMS", "browser_download_url": base + "/download/" + release.Tag + "/SHA256SUMS"},
					}
				}
				payload = append(payload, map[string]any{
					"tag_name":     release.Tag,
					"body":         release.Body,
					"published_at": release.Published,
					"prerelease":   release.Prerelease,
					"draft":        release.Draft,
					"assets":       assets,
				})
			}
			_ = json.NewEncoder(w).Encode(payload)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/download/") {
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/download/"), "/")
			if len(parts) != 2 {
				http.NotFound(w, r)
				return
			}
			tag, name := parts[0], parts[1]
			switch name {
			case "shampoore-linux-amd64.tar.gz":
				if archive, ok := archives[tag]; ok {
					_, _ = w.Write(archive)
					return
				}
			case "SHA256SUMS":
				if checksum, ok := checksums[tag]; ok {
					_, _ = w.Write([]byte(checksum))
					return
				}
			}
		}
		http.NotFound(w, r)
	}))
}

func releaseServer(t *testing.T, archive []byte, checksums string) *httptest.Server {
	t.Helper()
	return updateReleaseServer(t, []fakeRelease{{
		Tag:       "v2.1.0",
		Body:      "Release notes for v2.1.0",
		Published: "2025-01-02T03:04:05Z",
		Archive:   archive,
		Checksums: checksums,
	}})
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

func silenceUpdaterRestart(api *API) {
	api.updater.mu.Lock()
	api.updater.restart = func() error { return nil }
	api.updater.mu.Unlock()
}

func newBetaUpdateServer(t *testing.T) *httptest.Server {
	t.Helper()
	stableArchive := buildTarGz(t, releaseBinaries("stable-"))
	betaArchive := buildTarGz(t, releaseBinaries("beta-"))
	return updateReleaseServer(t, []fakeRelease{
		{
			Tag:       "v2.1.0",
			Body:      "stable release",
			Published: "2025-01-02T03:04:05Z",
			Archive:   stableArchive,
			Checksums: fmt.Sprintf("%s  shampoore-linux-amd64.tar.gz\n", sha256Hex(stableArchive)),
		},
		{
			Tag:        "v2.2.0-beta.1",
			Body:       "beta release",
			Published:  "2025-02-02T03:04:05Z",
			Prerelease: true,
			Archive:    betaArchive,
			Checksums:  fmt.Sprintf("%s  shampoore-linux-amd64.tar.gz\n", sha256Hex(betaArchive)),
		},
	})
}

func assertInstalledBinaries(t *testing.T, binDir, marker string) {
	t.Helper()
	for _, name := range updateBinaryNames {
		data, err := os.ReadFile(filepath.Join(binDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if want := marker + name; string(data) != want {
			t.Fatalf("%s = %q, want %q", name, data, want)
		}
	}
}

func TestInstallUpdateTargetBetaTag(t *testing.T) {
	binDir := t.TempDir()
	writeOldBinaries(t, binDir)
	srv := newBetaUpdateServer(t)
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateBinaryDir(binDir), WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	api.updater.downloadClient = srv.Client()
	silenceUpdaterRestart(api)

	rec := perform(api.Handler(), http.MethodPost, "/api/system/update", `{"tag":"2.2.0-beta.1"}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("POST beta update = %d: %s", rec.Code, rec.Body.String())
	}
	var result installResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.OK || result.LatestVersion != "v2.2.0-beta.1" || !result.Restarting {
		t.Fatalf("install result = %+v", result)
	}
	assertInstalledBinaries(t, binDir, "beta-")
}

func TestInstallUpdateWithoutTagPrefersStable(t *testing.T) {
	binDir := t.TempDir()
	writeOldBinaries(t, binDir)
	srv := newBetaUpdateServer(t)
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateBinaryDir(binDir), WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	api.updater.downloadClient = srv.Client()
	silenceUpdaterRestart(api)

	rec := perform(api.Handler(), http.MethodPost, "/api/system/update", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("POST update = %d: %s", rec.Code, rec.Body.String())
	}
	var result installResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.LatestVersion != "v2.1.0" {
		t.Fatalf("installed %q, want the stable v2.1.0", result.LatestVersion)
	}
	assertInstalledBinaries(t, binDir, "stable-")
}

func TestInstallUpdateTargetUnknownTag(t *testing.T) {
	srv := updateReleaseServer(t, []fakeRelease{{Tag: "v2.1.0", Published: "2025-01-02T03:04:05Z"}})
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	rec := perform(api.Handler(), http.MethodPost, "/api/system/update", `{"tag":"v9.9.9"}`, authRequest())
	if rec.Code != http.StatusNotFound {
		t.Fatalf("POST unknown tag = %d, want 404: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "release not found") {
		t.Fatalf("unexpected error: %s", rec.Body.String())
	}
}

func TestInstallUpdateRejectsOlderTarget(t *testing.T) {
	srv := updateReleaseServer(t, []fakeRelease{{Tag: "v1.0.0", Published: "2024-01-02T03:04:05Z"}})
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	rec := perform(api.Handler(), http.MethodPost, "/api/system/update", `{"version":"v1.0.0"}`, authRequest())
	if rec.Code != http.StatusConflict {
		t.Fatalf("POST older tag = %d, want 409: %s", rec.Code, rec.Body.String())
	}
}

func TestInstallUpdateWithoutTagRequiresExplicitBeta(t *testing.T) {
	betaArchive := buildTarGz(t, releaseBinaries("beta-"))
	srv := updateReleaseServer(t, []fakeRelease{{
		Tag:        "v2.1.0-beta.1",
		Published:  "2025-02-02T03:04:05Z",
		Prerelease: true,
		Archive:    betaArchive,
		Checksums:  fmt.Sprintf("%s  shampoore-linux-amd64.tar.gz\n", sha256Hex(betaArchive)),
	}})
	defer srv.Close()

	binDir := t.TempDir()
	old := writeOldBinaries(t, binDir)
	api, _ := newUpdaterTestAPI(t, WithUpdateBinaryDir(binDir), WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	rec := perform(api.Handler(), http.MethodPost, "/api/system/update", "", authRequest())
	if rec.Code != http.StatusConflict {
		t.Fatalf("POST beta without target = %d, want 409: %s", rec.Code, rec.Body.String())
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

func TestInstallUpdateTargetMissingAsset(t *testing.T) {
	srv := updateReleaseServer(t, []fakeRelease{{
		Tag:        "v2.2.0-beta.1",
		Published:  "2025-02-02T03:04:05Z",
		Prerelease: true,
		Assets:     []map[string]any{},
	}})
	defer srv.Close()

	api, _ := newUpdaterTestAPI(t, WithUpdateAPIBaseURL(srv.URL))
	api.updater.client = srv.Client()
	rec := perform(api.Handler(), http.MethodPost, "/api/system/update", `{"tag":"v2.2.0-beta.1"}`, authRequest())
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("POST tag without asset = %d, want 502: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "has no archive") {
		t.Fatalf("unexpected error: %s", rec.Body.String())
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
