package panel

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Esteviredzu/shampoore/internal/logging"
)

const DefaultUpdateRepo = "Esteviredzu/shampoore"

const (
	defaultUpdateAPIBase    = "https://api.github.com"
	updateCheckTimeout      = 10 * time.Second
	updateDownloadTimeout   = 10 * time.Minute
	updateCacheTTL          = 5 * time.Minute
	updateRestartDelay      = time.Second
	maxUpdateArchiveBytes   = int64(512 << 20)
	maxUpdateChecksumsBytes = int64(1 << 20)
	maxUpdateBinaryBytes    = int64(256 << 20)
	maxUpdateExtractedBytes = int64(768 << 20)
	maxUpdateAPIResponse    = int64(4 << 20)
)

var updateBinaryNames = []string{
	"shampoore-panel",
	"shampoore-server",
	"shampoore-relay",
	"shampoore-node-agent",
}

var errUpdateNotAvailable = errors.New("no update is available")

type UpdateInfo struct {
	CurrentVersion  string `json:"current_version"`
	LatestVersion   string `json:"latest_version"`
	UpdateAvailable bool   `json:"update_available"`
	ReleaseNotes    string `json:"release_notes"`
	PublishedAt     string `json:"published_at"`
	AssetURL        string `json:"asset_url"`
	AssetName       string `json:"asset_name,omitempty"`
	ChecksumURL     string `json:"checksum_url,omitempty"`
	HTMLURL         string `json:"html_url,omitempty"`
}

type installResult struct {
	OK             bool     `json:"ok"`
	CurrentVersion string   `json:"current_version"`
	LatestVersion  string   `json:"latest_version"`
	Updated        []string `json:"updated"`
	Restarting     bool     `json:"restarting"`
}

type githubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Size               int64  `json:"size"`
}

type githubRelease struct {
	TagName     string        `json:"tag_name"`
	Name        string        `json:"name"`
	Body        string        `json:"body"`
	HTMLURL     string        `json:"html_url"`
	PublishedAt time.Time     `json:"published_at"`
	Draft       bool          `json:"draft"`
	Prerelease  bool          `json:"prerelease"`
	Assets      []githubAsset `json:"assets"`
}

type releaseAsset struct {
	Name string
	URL  string
	Size int64
}

type releaseInfo struct {
	TagName     string
	Notes       string
	PublishedAt time.Time
	HTMLURL     string
	Asset       releaseAsset
	Checksums   releaseAsset
}

func CompareVersions(current, latest string) int {
	a, okA := parseSemver(current)
	b, okB := parseSemver(latest)
	if !okA || !okB {
		return sign(strings.Compare(normalizeVersion(current), normalizeVersion(latest)))
	}
	segments := len(a.core)
	if len(b.core) > segments {
		segments = len(b.core)
	}
	for i := 0; i < segments; i++ {
		var left, right int
		if i < len(a.core) {
			left = a.core[i]
		}
		if i < len(b.core) {
			right = b.core[i]
		}
		if left != right {
			return sign(left - right)
		}
	}
	return comparePrerelease(a.pre, b.pre)
}

type semver struct {
	core []int
	pre  []string
}

func parseSemver(raw string) (semver, bool) {
	s := normalizeVersion(raw)
	if i := strings.IndexByte(s, '+'); i >= 0 {
		s = s[:i]
	}
	var pre []string
	if i := strings.IndexByte(s, '-'); i >= 0 {
		prePart := strings.TrimSpace(s[i+1:])
		s = s[:i]
		if prePart != "" {
			pre = strings.Split(prePart, ".")
		}
	}
	if s == "" {
		return semver{}, false
	}
	parts := strings.Split(s, ".")
	core := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return semver{}, false
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return semver{}, false
		}
		core = append(core, n)
	}
	return semver{core: core, pre: pre}, true
}

func normalizeVersion(raw string) string {
	s := strings.TrimSpace(raw)
	for len(s) > 0 && (s[0] == 'v' || s[0] == 'V') {
		s = strings.TrimSpace(s[1:])
	}
	return s
}

func comparePrerelease(a, b []string) int {
	switch {
	case len(a) == 0 && len(b) == 0:
		return 0
	case len(a) == 0:

		return 1
	case len(b) == 0:
		return -1
	}
	for i := 0; i < len(a) && i < len(b); i++ {
		if cmp := comparePrereleaseIdentifier(a[i], b[i]); cmp != 0 {
			return cmp
		}
	}
	return sign(len(a) - len(b))
}

func comparePrereleaseIdentifier(a, b string) int {
	aNum, aErr := strconv.Atoi(a)
	bNum, bErr := strconv.Atoi(b)
	switch {
	case aErr == nil && bErr == nil:
		return sign(aNum - bNum)
	case aErr == nil:
		return -1
	case bErr == nil:
		return 1
	default:
		return sign(strings.Compare(a, b))
	}
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

func parseGitHubRelease(data []byte, goos, goarch string) (*releaseInfo, error) {
	var release githubRelease
	if err := json.Unmarshal(data, &release); err != nil {
		return nil, fmt.Errorf("decode GitHub release: %w", err)
	}
	tag := strings.TrimSpace(release.TagName)
	if tag == "" {
		return nil, errors.New("GitHub release has no tag_name")
	}
	return &releaseInfo{
		TagName:     tag,
		Notes:       strings.TrimSpace(release.Body),
		PublishedAt: release.PublishedAt,
		HTMLURL:     strings.TrimSpace(release.HTMLURL),
		Asset:       selectReleaseAsset(release.Assets, goos, goarch),
		Checksums:   selectChecksumAsset(release.Assets),
	}, nil
}

func selectReleaseAsset(assets []githubAsset, goos, goarch string) releaseAsset {
	expected := fmt.Sprintf("shampoore-%s-%s.tar.gz", strings.ToLower(goos), strings.ToLower(goarch))
	for _, asset := range assets {
		if strings.EqualFold(filepath.Base(asset.Name), expected) && strings.TrimSpace(asset.BrowserDownloadURL) != "" {
			return releaseAsset{Name: asset.Name, URL: asset.BrowserDownloadURL, Size: asset.Size}
		}
	}
	for _, asset := range assets {
		name := strings.ToLower(filepath.Base(asset.Name))
		if !strings.HasSuffix(name, ".tar.gz") || strings.TrimSpace(asset.BrowserDownloadURL) == "" {
			continue
		}
		if strings.Contains(name, strings.ToLower(goos)) && strings.Contains(name, strings.ToLower(goarch)) {
			return releaseAsset{Name: asset.Name, URL: asset.BrowserDownloadURL, Size: asset.Size}
		}
	}
	return releaseAsset{}
}

func selectChecksumAsset(assets []githubAsset) releaseAsset {
	for _, asset := range assets {
		if strings.EqualFold(filepath.Base(asset.Name), "SHA256SUMS") && strings.TrimSpace(asset.BrowserDownloadURL) != "" {
			return releaseAsset{Name: asset.Name, URL: asset.BrowserDownloadURL, Size: asset.Size}
		}
	}
	for _, asset := range assets {
		name := strings.ToUpper(filepath.Base(asset.Name))
		if strings.HasSuffix(name, "SHA256SUMS") && strings.TrimSpace(asset.BrowserDownloadURL) != "" {
			return releaseAsset{Name: asset.Name, URL: asset.BrowserDownloadURL, Size: asset.Size}
		}
	}
	return releaseAsset{}
}

func formatUpdateTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

type updateConfig struct {
	CurrentVersion string
	Repo           string
	APIBaseURL     string
	Token          string
	BinaryDir      string
	GOOS           string
	GOARCH         string
	HTTPClient     *http.Client
	DownloadClient *http.Client
	Restart        func() error
}

type updater struct {
	mu        sync.Mutex
	checkMu   sync.Mutex
	installMu sync.Mutex

	logger         *logging.Logger
	current        string
	repo           string
	apiBase        string
	token          string
	binDir         string
	goos           string
	goarch         string
	client         *http.Client
	downloadClient *http.Client
	restart        func() error
	restartOnce    sync.Once

	cached   *UpdateInfo
	cachedAt time.Time
}

func newUpdater(cfg updateConfig, logger *logging.Logger) *updater {
	u := &updater{
		logger:         logger,
		current:        strings.TrimSpace(cfg.CurrentVersion),
		repo:           strings.TrimSpace(cfg.Repo),
		apiBase:        strings.TrimSpace(cfg.APIBaseURL),
		token:          strings.TrimSpace(cfg.Token),
		binDir:         strings.TrimSpace(cfg.BinaryDir),
		goos:           strings.TrimSpace(cfg.GOOS),
		goarch:         strings.TrimSpace(cfg.GOARCH),
		client:         cfg.HTTPClient,
		downloadClient: cfg.DownloadClient,
		restart:        cfg.Restart,
	}
	if u.current == "" {
		u.current = "dev"
	}
	if u.repo == "" {
		u.repo = DefaultUpdateRepo
	}
	if u.apiBase == "" {
		u.apiBase = defaultUpdateAPIBase
	}
	if u.goos == "" {
		u.goos = runtime.GOOS
	}
	if u.goarch == "" {
		u.goarch = runtime.GOARCH
	}
	if u.binDir == "" {
		u.binDir = executableDir()
	}
	if u.client == nil {
		u.client = &http.Client{Timeout: updateCheckTimeout}
	}
	if u.downloadClient == nil {
		u.downloadClient = &http.Client{Timeout: updateDownloadTimeout}
	}
	if u.restart == nil {
		u.restart = restartPanelProcess
	}
	if u.logger == nil {
		u.logger = logging.New("panel", false)
	}
	return u
}

func executableDir() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return filepath.Dir(exe)
}

func restartPanelProcess() error {
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		return fmt.Errorf("find panel process: %w", err)
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal panel process: %w", err)
	}
	return nil
}

func (u *updater) now() time.Time {
	return time.Now()
}

func (u *updater) cachedInfo() *UpdateInfo {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.cached == nil || !u.now().Before(u.cachedAt.Add(updateCacheTTL)) {
		return nil
	}
	info := *u.cached
	return &info
}

func (u *updater) storeCache(info *UpdateInfo) {
	u.mu.Lock()
	defer u.mu.Unlock()
	clone := *info
	u.cached = &clone
	u.cachedAt = u.now()
}

func copyUpdateInfo(info *UpdateInfo) *UpdateInfo {
	if info == nil {
		return nil
	}
	clone := *info
	return &clone
}

func (u *updater) check(ctx context.Context, useCache bool) (*UpdateInfo, error) {
	u.checkMu.Lock()
	defer u.checkMu.Unlock()
	if useCache {
		if info := u.cachedInfo(); info != nil {
			return info, nil
		}
	}
	info, err := u.fetch(ctx)
	if err != nil {
		return nil, err
	}
	u.storeCache(info)
	return copyUpdateInfo(info), nil
}

func (u *updater) fetch(ctx context.Context) (*UpdateInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, updateCheckTimeout)
	defer cancel()
	url := strings.TrimRight(u.apiBase, "/") + "/repos/" + strings.Trim(u.repo, "/") + "/releases/latest"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build GitHub request: %w", err)
	}
	req.Header.Set("User-Agent", "shampoore-panel/"+u.current)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if u.token != "" {
		req.Header.Set("Authorization", "Bearer "+u.token)
	}
	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query GitHub releases: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = resp.Status
		}
		return nil, fmt.Errorf("GitHub releases: %s: %s", resp.Status, message)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxUpdateAPIResponse))
	if err != nil {
		return nil, fmt.Errorf("read GitHub release: %w", err)
	}
	release, err := parseGitHubRelease(data, u.goos, u.goarch)
	if err != nil {
		return nil, err
	}
	if CompareVersions(u.current, release.TagName) < 0 && release.Asset.URL == "" {
		u.logger.Warn("panel: release ", release.TagName, " has no archive for ", u.goos, "/", u.goarch)
	}
	return &UpdateInfo{
		CurrentVersion:  u.current,
		LatestVersion:   release.TagName,
		UpdateAvailable: CompareVersions(u.current, release.TagName) < 0 && release.Asset.URL != "",
		ReleaseNotes:    release.Notes,
		PublishedAt:     formatUpdateTime(release.PublishedAt),
		AssetURL:        release.Asset.URL,
		AssetName:       release.Asset.Name,
		ChecksumURL:     release.Checksums.URL,
		HTMLURL:         release.HTMLURL,
	}, nil
}

func (u *updater) scheduleRestart() {
	u.restartOnce.Do(func() {
		time.AfterFunc(updateRestartDelay, func() {
			u.mu.Lock()
			restart := u.restart
			u.mu.Unlock()
			u.logger.Info("panel: update installed; restarting")
			if restart == nil {
				return
			}
			if err := restart(); err != nil {
				u.logger.Error("panel: restart after update: ", err)
			}
		})
	})
}

func (u *updater) downloadFile(ctx context.Context, rawURL, dst string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "shampoore-panel/"+u.current)
	resp, err := u.downloadClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download %s: %s", filepath.Base(rawURL), resp.Status)
	}
	if resp.ContentLength > maxUpdateArchiveBytes {
		return "", fmt.Errorf("archive is too large (%d bytes)", resp.ContentLength)
	}
	file, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(resp.Body, maxUpdateArchiveBytes+1))
	syncErr := file.Sync()
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(dst)
		return "", copyErr
	}
	if syncErr != nil {
		_ = os.Remove(dst)
		return "", syncErr
	}
	if closeErr != nil {
		_ = os.Remove(dst)
		return "", closeErr
	}
	if written > maxUpdateArchiveBytes {
		_ = os.Remove(dst)
		return "", fmt.Errorf("archive is too large (> %d bytes)", maxUpdateArchiveBytes)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (u *updater) downloadBytes(ctx context.Context, rawURL string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "shampoore-panel/"+u.current)
	resp, err := u.downloadClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download %s: %s", filepath.Base(rawURL), resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%s is too large (> %d bytes)", filepath.Base(rawURL), limit)
	}
	return data, nil
}

func (u *updater) install(ctx context.Context) (*installResult, error) {
	info, err := u.check(ctx, true)
	if err != nil {
		return nil, err
	}
	if !info.UpdateAvailable || strings.TrimSpace(info.AssetURL) == "" {
		if CompareVersions(info.CurrentVersion, info.LatestVersion) >= 0 {
			return nil, fmt.Errorf("%w: current version %s, latest %s", errUpdateNotAvailable, info.CurrentVersion, info.LatestVersion)
		}
		return nil, fmt.Errorf("release %s has no archive for %s/%s", info.LatestVersion, u.goos, u.goarch)
	}
	dir, err := os.MkdirTemp("", "shampoore-update-*")
	if err != nil {
		return nil, fmt.Errorf("create update directory: %w", err)
	}
	defer os.RemoveAll(dir)

	archivePath := filepath.Join(dir, "release.tar.gz")
	if err := u.downloadArchive(ctx, info, archivePath); err != nil {
		return nil, err
	}
	binaries, err := extractUpdateBinariesFromFile(archivePath)
	if err != nil {
		return nil, err
	}
	updated, err := applyUpdateBinaries(u.binDir, binaries)
	if err != nil {
		return nil, err
	}
	return &installResult{
		OK:             true,
		CurrentVersion: info.CurrentVersion,
		LatestVersion:  info.LatestVersion,
		Updated:        updated,
		Restarting:     true,
	}, nil
}

func (u *updater) downloadArchive(ctx context.Context, info *UpdateInfo, dst string) error {
	ctx, cancel := context.WithTimeout(ctx, updateDownloadTimeout)
	defer cancel()
	sum, err := u.downloadFile(ctx, info.AssetURL, dst)
	if err != nil {
		return fmt.Errorf("download release: %w", err)
	}
	if strings.TrimSpace(info.ChecksumURL) == "" {
		return nil
	}
	data, err := u.downloadBytes(ctx, info.ChecksumURL, maxUpdateChecksumsBytes)
	if err != nil {
		return fmt.Errorf("download SHA256SUMS: %w", err)
	}
	sums, err := parseSHA256SUMS(data)
	if err != nil {
		return err
	}
	name := filepath.Base(strings.TrimSpace(info.AssetName))
	if name == "" || name == "." {
		name = filepath.Base(info.AssetURL)
	}
	want, ok := sums[name]
	if !ok {
		return fmt.Errorf("SHA256SUMS does not list %s", name)
	}
	if !strings.EqualFold(want, sum) {
		return fmt.Errorf("checksum mismatch for %s: got %s, want %s", name, sum, want)
	}
	return nil
}

func parseSHA256SUMS(data []byte) (map[string]string, error) {
	sums := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		sum := strings.ToLower(strings.TrimSpace(fields[0]))
		if !isHexString(sum) {
			continue
		}
		name := filepath.Base(strings.TrimPrefix(strings.TrimSpace(fields[1]), "*"))
		if name == "" || name == "." {
			continue
		}
		sums[name] = sum
	}
	if len(sums) == 0 {
		return nil, errors.New("SHA256SUMS is empty or malformed")
	}
	return sums, nil
}

func isHexString(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func extractUpdateBinariesFromFile(path string) (map[string][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open release archive: %w", err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("open release archive: %w", err)
	}
	defer gz.Close()
	return extractUpdateBinaries(gz)
}

func extractUpdateBinaries(reader io.Reader) (map[string][]byte, error) {
	allowed := make(map[string]struct{}, len(updateBinaryNames))
	for _, name := range updateBinaryNames {
		allowed[name] = struct{}{}
	}
	found := make(map[string][]byte, len(updateBinaryNames))
	tr := tar.NewReader(reader)
	var total int64
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read release archive: %w", err)
		}
		clean := filepath.Clean(header.Name)
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
			return nil, fmt.Errorf("unsafe path in release archive: %q", header.Name)
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			continue
		}
		name := filepath.Base(clean)
		if _, ok := allowed[name]; !ok {
			continue
		}
		if _, dup := found[name]; dup {
			return nil, fmt.Errorf("duplicate binary in release archive: %s", name)
		}
		if header.Size < 0 || header.Size > maxUpdateBinaryBytes {
			return nil, fmt.Errorf("binary %s has an unexpected size", name)
		}
		total += header.Size
		if total > maxUpdateExtractedBytes {
			return nil, errors.New("release archive expands beyond the allowed size")
		}
		data, err := io.ReadAll(io.LimitReader(tr, maxUpdateBinaryBytes+1))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		if int64(len(data)) > maxUpdateBinaryBytes {
			return nil, fmt.Errorf("binary %s is too large", name)
		}
		found[name] = data
	}
	missing := make([]string, 0, len(updateBinaryNames))
	for _, name := range updateBinaryNames {
		if _, ok := found[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("release archive is missing %s", strings.Join(missing, ", "))
	}
	return found, nil
}

func applyUpdateBinaries(binDir string, binaries map[string][]byte) ([]string, error) {
	if strings.TrimSpace(binDir) == "" {
		return nil, errors.New("panel binary directory is unknown")
	}
	info, err := os.Stat(binDir)
	if err != nil {
		return nil, fmt.Errorf("binary directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("binary directory %s is not a directory", binDir)
	}
	names := make([]string, 0, len(binaries))
	for name := range binaries {
		if !isUpdateBinaryName(name) {
			return nil, fmt.Errorf("refusing to install unknown binary %q", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)

	staged := make(map[string]string, len(names))
	cleanup := func() {
		for _, path := range staged {
			_ = os.Remove(path)
		}
	}
	for _, name := range names {
		target := filepath.Join(binDir, name)
		tmp := target + ".new"
		if err := writeExecutable(tmp, binaries[name]); err != nil {
			cleanup()
			return nil, fmt.Errorf("stage %s: %w", name, err)
		}
		staged[name] = tmp
	}

	updated := make([]string, 0, len(names))
	for _, name := range names {
		target := filepath.Join(binDir, name)
		tmp := staged[name]
		if err := os.Rename(tmp, target); err != nil {
			delete(staged, name)
			cleanup()
			return updated, fmt.Errorf("replace %s: %w", name, err)
		}
		delete(staged, name)
		updated = append(updated, name)
	}
	return updated, nil
}

func writeExecutable(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := file.Chmod(0o755); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

func isUpdateBinaryName(name string) bool {
	for _, allowed := range updateBinaryNames {
		if name == allowed {
			return true
		}
	}
	return false
}

func (a *API) systemVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"version": a.version,
		"os":      runtime.GOOS,
		"arch":    runtime.GOARCH,
	})
}

func (a *API) checkSystemUpdate(w http.ResponseWriter, r *http.Request) {
	if a.updater == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("updater is not configured"))
		return
	}
	info, err := a.updater.check(r.Context(), true)
	if err != nil {
		a.logger.Warn("panel: update check: ", err)
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (a *API) installSystemUpdate(w http.ResponseWriter, r *http.Request) {
	if a.updater == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("updater is not configured"))
		return
	}
	if !a.updater.installMu.TryLock() {
		writeErr(w, http.StatusConflict, errors.New("an update is already in progress"))
		return
	}
	defer a.updater.installMu.Unlock()

	result, err := a.updater.install(r.Context())
	if err != nil {
		if errors.Is(err, errUpdateNotAvailable) {
			writeErr(w, http.StatusConflict, err)
			return
		}
		a.logger.Warn("panel: update install: ", err)
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	a.logger.Info("panel: updated binaries: ", strings.Join(result.Updated, ", "))
	writeJSON(w, http.StatusOK, result)

	if a.sup != nil && a.sup.IsRunning() {
		if err := a.sup.Stop(); err != nil {
			a.logger.Warn("panel: stop server before restart: ", err)
		}
	}
	a.updater.scheduleRestart()
}
