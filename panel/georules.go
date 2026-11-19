package panel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

const (
	geoDownloadTimeout = 15 * time.Second
	maxGeoRuleBytes    = int64(32 << 20)
)

var (
	errGeoRuleInvalid = errors.New("invalid geo rule content")
	errGeoRuleFetch   = errors.New("geo rule download failed")
)

func geoRuleErrStatus(err error) int {
	if errors.Is(err, errGeoRuleInvalid) {
		return http.StatusBadRequest
	}
	return http.StatusBadGateway
}

func looksLikeHTML(text string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(text))
	if trimmed == "" {
		return false
	}
	for _, prefix := range []string{"<!doctype", "<html", "<head", "<body", "<table", "<pre", "<div"} {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	if idx := strings.Index(trimmed, "<html"); idx >= 0 && idx < 512 {
		return true
	}
	return false
}

func parseGeoRuleList(data []byte) ([]string, error) {
	text := strings.TrimPrefix(string(data), "\ufeff")
	if looksLikeHTML(text) {
		return nil, fmt.Errorf("%w: the URL returned an HTML page instead of an IP list", errGeoRuleInvalid)
	}
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	out := make([]string, 0, 1024)
	seen := make(map[string]struct{}, 1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") || strings.HasPrefix(line, ";") {
			continue
		}
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line == "" {
			continue
		}
		if strings.ContainsAny(line, "<>") {
			return nil, fmt.Errorf("%w: line %d looks like markup", errGeoRuleInvalid, lineNo)
		}
		token := strings.TrimSpace(strings.Fields(line)[0])
		normalized, ok := normalizeGeoToken(token)
		if !ok {
			return nil, fmt.Errorf("%w: line %d: %q is not an IP address or CIDR", errGeoRuleInvalid, lineNo, token)
		}
		if _, dup := seen[normalized]; dup {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", errGeoRuleInvalid, err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: the list contains no IP or CIDR entries", errGeoRuleInvalid)
	}
	return out, nil
}

func normalizeGeoToken(token string) (string, bool) {
	if strings.Contains(token, "/") {
		_, network, err := net.ParseCIDR(token)
		if err != nil {
			return "", false
		}
		return network.String(), true
	}
	ip := net.ParseIP(token)
	if ip == nil {
		return "", false
	}
	return ip.String(), true
}

func writeGeoRuleFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".geo-rule-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (a *API) geoRulePath(name string) (string, string, error) {
	if strings.TrimSpace(a.rulesDir) == "" {
		return "", "", fmt.Errorf("rules dir not set")
	}
	safe := sanitizeName(name)
	if safe == "" {
		return "", "", fmt.Errorf("invalid name")
	}
	path := filepath.Join(a.rulesDir, safe+".txt")
	cleanBase := filepath.Clean(a.rulesDir) + string(os.PathSeparator)
	cleanPath := filepath.Clean(path)
	if !strings.HasPrefix(cleanPath, cleanBase) {
		return "", "", fmt.Errorf("invalid name")
	}
	return safe, cleanPath, nil
}

func (a *API) downloadGeoRule(ctx context.Context, rawURL string) ([]string, error) {
	normalized, err := panelstore.NormalizeGeoRuleURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errGeoRuleInvalid, err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, geoDownloadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, normalized, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errGeoRuleFetch, err)
	}
	client := a.geoClient
	if client == nil {
		client = &http.Client{Timeout: geoDownloadTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errGeoRuleFetch, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w: %s returned %s", errGeoRuleFetch, normalized, resp.Status)
	}
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") {
		return nil, fmt.Errorf("%w: the URL returned an HTML page instead of an IP list", errGeoRuleInvalid)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxGeoRuleBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errGeoRuleFetch, err)
	}
	if int64(len(data)) > maxGeoRuleBytes {
		return nil, fmt.Errorf("%w: rule file is larger than %d bytes", errGeoRuleInvalid, maxGeoRuleBytes)
	}
	return parseGeoRuleList(data)
}

func geoRuleFileData(cidrs []string) []byte {
	return []byte(strings.Join(cidrs, "\n") + "\n")
}

type geoRuleImportRequest struct {
	Name                  string   `json:"name"`
	URL                   string   `json:"url"`
	UpdateIntervalMinutes int      `json:"update_interval_minutes"`
	ExitNodeIDs           *[]int64 `json:"exit_node_ids"`
}

func (a *API) listGeoRuleSources(w http.ResponseWriter, r *http.Request) {
	if a.store == nil {
		writeJSON(w, http.StatusOK, []panelstore.GeoRuleSource{})
		return
	}
	sources, err := a.store.ListGeoRuleSources(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sources)
}

func (a *API) importGeoRule(w http.ResponseWriter, r *http.Request) {
	if a.store == nil {
		writeErr(w, http.StatusServiceUnavailable, errNoStore())
		return
	}
	var req geoRuleImportRequest
	if err := decodeBody(w, r, &req, 1<<20); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	safe, path, err := a.geoRulePath(req.Name)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	normalizedURL, err := panelstore.NormalizeGeoRuleURL(req.URL)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.UpdateIntervalMinutes < 0 {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("update_interval_minutes must not be negative"))
		return
	}
	if req.ExitNodeIDs != nil {
		if err := a.store.ValidateExitNodeIDs(r.Context(), *req.ExitNodeIDs); err != nil {
			writeStoreErr(w, err)
			return
		}
	}
	cidrs, err := a.downloadGeoRule(r.Context(), normalizedURL)
	if err != nil {
		writeErr(w, geoRuleErrStatus(err), err)
		return
	}

	a.geoMu.Lock()
	defer a.geoMu.Unlock()
	if err := writeGeoRuleFileAtomic(path, geoRuleFileData(cidrs)); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	previousExitIDs := []int64{}
	_, sourceErr := a.store.GetGeoRuleSource(r.Context(), safe)
	if sourceErr != nil && !errors.Is(sourceErr, panelstore.ErrNotFound) {
		writeStoreErr(w, sourceErr)
		return
	}
	sourceExisted := sourceErr == nil
	if sourceExisted {
		previousExitIDs = a.geoRuleExitIDs(r.Context(), safe)
	}
	source, err := a.store.UpsertGeoRuleSource(r.Context(), panelstore.GeoRuleSource{
		Name:                  safe,
		URL:                   normalizedURL,
		UpdateIntervalMinutes: req.UpdateIntervalMinutes,
		LastUpdatedAt:         time.Now().UTC(),
	})
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if req.ExitNodeIDs != nil {
		ids, err := a.store.SetGeoRuleNodes(r.Context(), safe, *req.ExitNodeIDs)
		if err != nil {
			writeStoreErr(w, err)
			return
		}
		source.ExitNodeIDs = ids
	} else {
		ids, err := a.store.GeoRuleNodes(r.Context(), safe)
		if err != nil {
			writeStoreErr(w, err)
			return
		}
		source.ExitNodeIDs = ids
	}
	affected := source.ExitNodeIDs
	if req.ExitNodeIDs != nil && sourceExisted {
		affected = mergeGeoRuleExitIDs(previousExitIDs, source.ExitNodeIDs)
	}
	a.reloadGeoRules(r.Context(), affected)
	writeJSON(w, http.StatusCreated, map[string]any{
		"ok":     true,
		"source": source,
		"rules":  len(cidrs),
	})
}

func (a *API) refreshGeoRule(w http.ResponseWriter, r *http.Request) {
	if a.store == nil {
		writeErr(w, http.StatusServiceUnavailable, errNoStore())
		return
	}
	safe, _, err := a.geoRulePath(r.PathValue("name"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	source, err := a.store.GetGeoRuleSource(r.Context(), safe)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	a.geoMu.Lock()
	err = a.refreshGeoRuleLocked(r.Context(), source)
	a.geoMu.Unlock()
	if err != nil {
		writeErr(w, geoRuleErrStatus(err), err)
		return
	}
	updated, getErr := a.store.GetGeoRuleSource(r.Context(), safe)
	if getErr != nil {
		updated = source
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "source": updated})
}

func (a *API) refreshGeoRuleLocked(ctx context.Context, source panelstore.GeoRuleSource) error {
	_, path, err := a.geoRulePath(source.Name)
	if err != nil {
		a.recordGeoRuleError(ctx, source.Name, err)
		return err
	}
	cidrs, err := a.downloadGeoRule(ctx, source.URL)
	if err != nil {
		a.recordGeoRuleError(ctx, source.Name, err)
		return err
	}
	if err := writeGeoRuleFileAtomic(path, geoRuleFileData(cidrs)); err != nil {
		a.recordGeoRuleError(ctx, source.Name, err)
		return err
	}
	if err := a.store.MarkGeoRuleSourceUpdated(ctx, source.Name, time.Now().UTC()); err != nil {
		return err
	}
	a.reloadGeoRules(ctx, a.geoRuleExitIDs(ctx, source.Name))
	return nil
}

func (a *API) recordGeoRuleError(ctx context.Context, name string, cause error) {
	if a.store == nil {
		return
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := a.store.SetGeoRuleSourceError(saveCtx, name, cause.Error()); err != nil {
		if a.logger != nil {
			a.logger.Warn("panel: record geo rule error: ", err)
		}
	}
}

func (a *API) reloadGeoRules(ctx context.Context, exitNodeIDs []int64) {
	if a.filter != nil && strings.TrimSpace(a.rulesDir) != "" {
		if err := a.filter.LoadDir(a.rulesDir); err != nil && a.logger != nil {
			a.logger.Warn("panel: reload rules: ", err)
		}
	}
	if a.sup != nil && a.sup.IsRunning() {
		if err := a.sup.Stop(); err != nil && a.logger != nil {
			a.logger.Warn("panel: stop server for rules reload: ", err)
		} else if err := a.sup.Start(); err != nil && a.logger != nil {
			a.logger.Warn("panel: start server for rules reload: ", err)
		}
	}
	a.redeployExitNodes(exitNodeIDs, "geoip rules updated")
}

func (a *API) geoRuleExitIDs(ctx context.Context, name string) []int64 {
	if a.store == nil {
		return nil
	}
	ids, err := a.store.GeoRuleNodes(ctx, name)
	if err != nil {
		if a.logger != nil {
			a.logger.Warn("panel: read geo rule nodes: ", err)
		}
		return nil
	}
	return ids
}

func (a *API) redeployGeoRule(ctx context.Context, name, reason string) {
	a.redeployExitNodes(a.geoRuleExitIDs(ctx, name), reason)
}

func (a *API) redeployExitNodes(ids []int64, reason string) {
	if a.provisioner == nil || a.store == nil {
		return
	}
	if len(ids) == 0 {
		a.redeployDeployedExits(reason)
		return
	}
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		node, err := a.store.GetNode(context.Background(), id)
		if err != nil || !node.IsExit {
			continue
		}
		if node.Status != panelstore.NodeStatusDeployed && node.Status != panelstore.NodeStatusOnline {
			continue
		}
		a.scheduleDeploy(id, reason)
	}
}

func mergeGeoRuleExitIDs(previous, next []int64) []int64 {
	if len(previous) == 0 || len(next) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(previous)+len(next))
	out := make([]int64, 0, len(previous)+len(next))
	for _, ids := range [][]int64{previous, next} {
		for _, id := range ids {
			if id <= 0 {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

func (a *API) updateGeoRuleNodes(w http.ResponseWriter, r *http.Request) {
	if a.store == nil {
		writeErr(w, http.StatusServiceUnavailable, errNoStore())
		return
	}
	safe, _, err := a.geoRulePath(r.PathValue("name"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	exists := false
	for _, ext := range []string{".txt", ".json"} {
		_, statErr := os.Stat(filepath.Join(a.rulesDir, safe+ext))
		if statErr == nil {
			exists = true
			break
		}
		if !errors.Is(statErr, os.ErrNotExist) {
			writeErr(w, http.StatusInternalServerError, statErr)
			return
		}
	}
	if !exists {
		writeErr(w, http.StatusNotFound, fmt.Errorf("geo rule %s: %w", safe, panelstore.ErrNotFound))
		return
	}
	var req struct {
		ExitNodeIDs []int64 `json:"exit_node_ids"`
	}
	if err := decodeBody(w, r, &req, 1<<20); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	previous, err := a.store.GeoRuleNodes(r.Context(), safe)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	ids, err := a.store.SetGeoRuleNodes(r.Context(), safe, req.ExitNodeIDs)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	a.redeployExitNodes(mergeGeoRuleExitIDs(previous, ids), "geoip rule exit nodes changed")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "exit_node_ids": ids})
}

func (a *API) StartGeoRuleUpdater(ctx context.Context) {
	if a.store == nil || strings.TrimSpace(a.rulesDir) == "" {
		return
	}
	every := a.geoUpdateEvery
	if every <= 0 {
		every = time.Minute
	}
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.refreshDueGeoRules(ctx)
			}
		}
	}()
}

func (a *API) refreshDueGeoRules(ctx context.Context) {
	if a.store == nil {
		return
	}
	sources, err := a.store.DueGeoRuleSources(ctx, time.Now().UTC())
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if a.logger != nil {
			a.logger.Warn("panel: list due geo rules: ", err)
		}
		return
	}
	for _, source := range sources {
		if ctx.Err() != nil {
			return
		}
		a.geoMu.Lock()
		refreshErr := a.refreshGeoRuleLocked(ctx, source)
		a.geoMu.Unlock()
		if refreshErr != nil && a.logger != nil {
			a.logger.Warn("panel: refresh geo rule "+source.Name+": ", refreshErr)
		}
	}
}
