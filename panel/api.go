package panel

import (
	"archive/tar"
	"compress/gzip"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/control"
	"github.com/Esteviredzu/shampoore/internal/geofilter"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/server/manager"
)

//go:embed static
var staticFS embed.FS

type API struct {
	client     *control.Client
	sup        ServerManager
	logger     *logging.Logger
	token      string
	configsDir string
	filter     *geofilter.Filter
	rulesDir   string
	limiter    *loginLimiter
}

func New(client *control.Client, sup ServerManager, logger *logging.Logger, token, configsDir string, filter *geofilter.Filter, rulesDir string) *API {
	return &API{
		client:     client,
		sup:        sup,
		logger:     logger,
		token:      strings.TrimSpace(token),
		configsDir: configsDir,
		filter:     filter,
		rulesDir:   rulesDir,
		limiter:    newLoginLimiter(),
	}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/login", a.login)
	mux.HandleFunc("POST /api/logout", a.logout)

	mux.HandleFunc("GET /api/server/status", a.serverStatus)
	mux.HandleFunc("POST /api/server/start", a.serverStart)
	mux.HandleFunc("POST /api/server/stop", a.serverStop)
	mux.HandleFunc("GET /api/server/logs", a.serverLogs)

	mux.HandleFunc("GET /api/stats", a.stats)
	mux.HandleFunc("GET /api/stats/stream", a.statsStream)
	mux.HandleFunc("GET /api/system/stats", a.systemStats)

	mux.HandleFunc("GET /api/templates", a.templates)
	mux.HandleFunc("GET /api/configs", a.list)
	mux.HandleFunc("POST /api/configs", a.add)
	mux.HandleFunc("POST /api/configs/validate", a.validate)
	mux.HandleFunc("GET /api/configs/{name}", a.get)
	mux.HandleFunc("GET /api/configs/{name}/export", a.exportConfig)
	mux.HandleFunc("PUT /api/configs/{name}", a.update)
	mux.HandleFunc("DELETE /api/configs/{name}", a.remove)
	mux.HandleFunc("POST /api/configs/{name}/enable", a.enable)
	mux.HandleFunc("POST /api/configs/{name}/disable", a.disable)
	mux.HandleFunc("POST /api/configs/{name}/restart", a.restart)
	mux.HandleFunc("GET /api/configs/{name}/logs", a.logs)

	mux.HandleFunc("GET /api/backup", a.backup)

	mux.HandleFunc("GET /api/rules", a.listRules)
	mux.HandleFunc("POST /api/rules/{name}/enable", a.enableRule)
	mux.HandleFunc("POST /api/rules/{name}/disable", a.disableRule)
	mux.HandleFunc("POST /api/rules", a.addRule)
	mux.HandleFunc("DELETE /api/rules/{name}", a.deleteRule)
	mux.HandleFunc("POST /api/rules/apply", a.applyRules)

	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		a.logger.Warn("panel: static dir missing from embed: ", err)
		sub = staticFS
	}
	loginHTML, loginErr := fs.ReadFile(sub, "login.html")
	if loginErr != nil {
		a.logger.Warn("panel: login page missing from embed: ", loginErr)
	}
	mux.HandleFunc("GET /login", func(w http.ResponseWriter, r *http.Request) {
		if loginErr != nil {
			writeErr(w, http.StatusInternalServerError, fmt.Errorf("login page unavailable: %w", loginErr))
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if _, err := w.Write(loginHTML); err != nil {
			log.Printf("panel: write login page: %v", err)
		}
	})
	mux.Handle("/", http.FileServer(http.FS(sub)))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")

		if a.token == "" {
			writeErr(w, http.StatusInternalServerError, fmt.Errorf("panel token is not configured"))
			return
		}

		path := r.URL.Path
		if strings.HasPrefix(path, "/api/") || path == "/" || strings.HasSuffix(path, ".html") {
			w.Header().Set("Cache-Control", "no-store")
		}
		if path == "/login" || path == "/login.html" || path == "/api/login" {
			mux.ServeHTTP(w, r)
			return
		}

		if a.authorized(r) {
			mux.ServeHTTP(w, r)
			return
		}

		if _, ok := a.credentials(r); ok {
			ip := clientIP(r)
			if blocked, retry := a.limiter.fail(ip); blocked {
				w.Header().Set("Retry-After", retryAfterSeconds(retry))
				writeErr(w, http.StatusTooManyRequests, fmt.Errorf("too many failed authentication attempts"))
				return
			}
		}

		if strings.HasPrefix(path, "/api/") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="shampoore-panel"`)
			writeErr(w, http.StatusUnauthorized, fmt.Errorf("unauthorized"))
			return
		}
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		writeErr(w, http.StatusUnauthorized, fmt.Errorf("unauthorized"))
	})
}

func (a *API) login(w http.ResponseWriter, r *http.Request) {
	if a.token == "" {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("panel token is not configured"))
		return
	}
	ip := clientIP(r)
	if allowed, retry := a.limiter.allow(ip); !allowed {
		w.Header().Set("Retry-After", retryAfterSeconds(retry))
		writeErr(w, http.StatusTooManyRequests, fmt.Errorf("too many failed authentication attempts"))
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var req struct {
		Token string `json:"token"`
	}
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid login request: %w", err))
			return
		}
		req.Token = r.PostFormValue("token")
	} else {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid login request: %w", err))
			return
		}
	}

	req.Token = strings.TrimSpace(req.Token)
	if !tokenEqual(req.Token, a.token) {
		if blocked, retry := a.limiter.fail(ip); blocked {
			w.Header().Set("Retry-After", retryAfterSeconds(retry))
			writeErr(w, http.StatusTooManyRequests, fmt.Errorf("too many failed authentication attempts"))
			return
		}
		a.writeAuthFailure(w)
		return
	}

	a.limiter.reset(ip)
	a.setAuthCookie(w, r)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *API) writeAuthFailure(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="shampoore-panel"`)
	writeErr(w, http.StatusUnauthorized, fmt.Errorf("invalid token"))
}

func (a *API) logout(w http.ResponseWriter, _ *http.Request) {
	a.clearAuthCookie(w)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *API) listRules(w http.ResponseWriter, _ *http.Request) {
	if a.filter == nil {
		writeJSON(w, 200, []geofilter.RuleSet{})
		return
	}
	writeJSON(w, 200, a.filter.Rules())
}

func (a *API) enableRule(w http.ResponseWriter, r *http.Request) {
	if a.filter == nil {
		writeErr(w, 400, fmt.Errorf("filter not initialized"))
		return
	}
	name := r.PathValue("name")
	if err := a.filter.SetEnabled(name, true); err != nil {
		writeErr(w, 400, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *API) disableRule(w http.ResponseWriter, r *http.Request) {
	if a.filter == nil {
		writeErr(w, 400, fmt.Errorf("filter not initialized"))
		return
	}
	name := r.PathValue("name")
	if err := a.filter.SetEnabled(name, false); err != nil {
		writeErr(w, 400, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *API) applyRules(w http.ResponseWriter, _ *http.Request) {
	if !a.sup.IsRunning() {
		writeJSON(w, 200, map[string]any{"ok": true, "restarted": false})
		return
	}
	if err := a.sup.Stop(); err != nil {
		writeErr(w, 500, err)
		return
	}
	if err := a.sup.Start(); err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "restarted": true})
}

func (a *API) systemStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, getSystemStats())
}

func (a *API) backup(w http.ResponseWriter, r *http.Request) {
	entries, err := os.ReadDir(a.configsDir)
	if err != nil {
		writeErr(w, 500, err)
		return
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="shampoore-backup-%s.tar.gz"`,
			time.Now().Format("2006-01-02-150405")))

	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	defer func() {
		if err := tw.Close(); err != nil {
			a.logger.Warn("panel: backup tar close: ", err)
		}
		if err := gz.Close(); err != nil {
			a.logger.Warn("panel: backup gzip close: ", err)
		}
	}()

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		path := filepath.Join(a.configsDir, e.Name())
		info, err := e.Info()
		if err != nil {
			a.logger.Warn("panel: backup stat ", e.Name(), ": ", err)
			continue
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			a.logger.Warn("panel: backup header ", e.Name(), ": ", err)
			continue
		}
		hdr.Name = e.Name()
		if err := tw.WriteHeader(hdr); err != nil {
			a.logger.Warn("panel: backup write header: ", err)
			return
		}
		f, err := os.Open(path)
		if err != nil {
			a.logger.Warn("panel: backup open ", e.Name(), ": ", err)
			continue
		}
		if _, err := io.Copy(tw, f); err != nil {
			a.logger.Warn("panel: backup copy ", e.Name(), ": ", err)
		}
		if err := f.Close(); err != nil {
			a.logger.Warn("panel: backup close ", e.Name(), ": ", err)
		}
	}
}

func (a *API) findConfigPath(name string) (string, error) {
	entries, err := os.ReadDir(a.configsDir)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(a.configsDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		settings, err := manager.ParseConfigContent(string(data))
		if err != nil || settings == nil {
			continue
		}
		n := settings.Name
		if n == "" {
			n = strings.TrimSuffix(e.Name(), ".json")
		}
		if n == name {
			return path, nil
		}
	}
	return "", fmt.Errorf("config file not found for name: %s", name)
}

func (a *API) listConfigsFromDisk() ([]manager.EntryView, error) {
	st, err := manager.LoadState(a.configsDir)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(a.configsDir)
	if err != nil {
		return nil, err
	}
	var out []manager.EntryView
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(a.configsDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		settings, err := manager.ParseConfigContent(string(data))
		if err != nil || settings == nil {
			continue
		}
		if err := settings.ApplyDefaults(); err != nil {
			a.logger.Warn("panel: skip config ", e.Name(), ": ", err)
			continue
		}
		name := settings.Name
		if name == "" {
			name = strings.TrimSuffix(e.Name(), ".json")
		}
		info, _ := e.Info()
		updatedAt := time.Now()
		if info != nil {
			updatedAt = info.ModTime()
		}
		enabled := false
		if st != nil {
			enabled = st.IsEnabled(name)
		}
		out = append(out, manager.EntryView{
			Name:      name,
			Enabled:   enabled,
			Kind:      settings.Type,
			Settings:  settings,
			Status:    manager.StatusStopped,
			UpdatedAt: updatedAt,
			FilePath:  path,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (a *API) serverStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"running": a.sup.IsRunning()})
}

func (a *API) serverStart(w http.ResponseWriter, _ *http.Request) {
	if err := a.sup.Start(); err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *API) serverStop(w http.ResponseWriter, _ *http.Request) {
	if err := a.sup.Stop(); err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *API) serverLogs(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}
	lines := a.sup.Logs(limit)
	writeJSON(w, 200, map[string]any{"lines": lines})
}

func (a *API) stats(w http.ResponseWriter, _ *http.Request) {
	if !a.sup.IsRunning() {
		writeJSON(w, 200, map[string]any{
			"running": false,
			"entries": []any{},
			"ts":      time.Now().UnixMilli(),
		})
		return
	}
	entries, err := a.client.Stats()
	if err != nil {
		writeControlErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{
		"running": true,
		"entries": entries,
		"ts":      time.Now().UnixMilli(),
	})
}

func (a *API) statsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "sse unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	ctx := r.Context()

	send := func() {
		var payload any
		if a.sup.IsRunning() {
			entries, err := a.client.Stats()
			if err == nil {
				payload = map[string]any{
					"running": true,
					"entries": entries,
					"ts":      time.Now().UnixMilli(),
				}
			} else {
				payload = map[string]any{
					"running": false,
					"error":   err.Error(),
					"ts":      time.Now().UnixMilli(),
				}
			}
		} else {
			payload = map[string]any{
				"running": false,
				"entries": []any{},
				"ts":      time.Now().UnixMilli(),
			}
		}
		data, err := json.Marshal(payload)
		if err != nil {
			a.logger.Warn("panel: stats stream marshal: ", err)
			return
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return
		}
		flusher.Flush()
	}

	send()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			send()
		}
	}
}

func (a *API) templates(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, LoadTemplates())
}

func (a *API) list(w http.ResponseWriter, _ *http.Request) {
	if a.sup.IsRunning() {
		v, err := a.client.List()
		if err == nil {
			writeJSON(w, 200, v)
			return
		}
	}
	v, err := a.listConfigsFromDisk()
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, v)
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if a.sup.IsRunning() {
		v, err := a.client.Get(name)
		if err == nil {
			writeJSON(w, 200, v)
			return
		}
	}
	path, err := a.findConfigPath(name)
	if err != nil {
		writeErr(w, 404, err)
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	settings, err := manager.ParseConfigContent(string(data))
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	if settings == nil {
		writeErr(w, 500, fmt.Errorf("invalid config"))
		return
	}
	if err := settings.ApplyDefaults(); err != nil {
		writeErr(w, 500, err)
		return
	}
	st, err := manager.LoadState(a.configsDir)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	enabled := st.IsEnabled(name)
	writeJSON(w, 200, manager.EntryView{
		Name:     name,
		Enabled:  enabled,
		Kind:     settings.Type,
		Settings: settings,
		Status:   manager.StatusStopped,
		FilePath: path,
	})
}

func (a *API) exportConfig(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var settings *app.Settings

	if a.sup.IsRunning() {
		v, err := a.client.Get(name)
		if err != nil {
			writeControlErr(w, err)
			return
		}
		settings = v.Settings
	} else {
		path, err := a.findConfigPath(name)
		if err != nil {
			writeErr(w, 404, err)
			return
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			writeErr(w, 500, readErr)
			return
		}
		settings, err = manager.ParseConfigContent(string(data))
		if err != nil {
			writeErr(w, 500, err)
			return
		}
	}

	if settings == nil {
		writeErr(w, 400, fmt.Errorf("config has no settings"))
		return
	}

	url, err := app.EncodeURL(settings)
	if err != nil {
		writeErr(w, 500, fmt.Errorf("encode url: %w", err))
		return
	}
	writeJSON(w, 200, map[string]any{"url": url})
}

func (a *API) add(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		Template string `json:"template,omitempty"`
		Content  string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, err)
		return
	}
	if strings.TrimSpace(req.Content) == "" {
		writeErr(w, 400, fmt.Errorf("content is required"))
		return
	}

	if a.sup.IsRunning() {
		if err := a.client.Add(req.Name, req.Template, req.Content); err != nil {
			writeControlErr(w, err)
			return
		}
	} else {
		settings, err := manager.ParseConfigContent(req.Content)
		if err != nil {
			writeErr(w, 400, err)
			return
		}
		if err := manager.ValidateSettings(settings); err != nil {
			writeErr(w, 400, err)
			return
		}
		if req.Name == "" {
			req.Name = settings.Name
		}
		if req.Name == "" {
			writeErr(w, 400, fmt.Errorf("name is required"))
			return
		}
		settings.Name = req.Name

		safe := sanitizeName(req.Name)
		path := filepath.Join(a.configsDir, safe+".json")

		cleanBase := filepath.Clean(a.configsDir) + string(os.PathSeparator)
		cleanPath := filepath.Clean(path)
		if !strings.HasPrefix(cleanPath, cleanBase) {
			writeErr(w, 400, fmt.Errorf("invalid name"))
			return
		}

		data, err := json.MarshalIndent(settings, "", "  ")
		if err != nil {
			writeErr(w, 500, err)
			return
		}
		if err := writeFile0600(cleanPath, data); err != nil {
			writeErr(w, 500, err)
			return
		}
		st, err := manager.LoadState(a.configsDir)
		if err != nil {
			writeErr(w, 500, err)
			return
		}
		if err := st.Set(req.Name, true); err != nil {
			writeErr(w, 500, err)
			return
		}
	}
	writeJSON(w, 201, map[string]any{"ok": true})
}

func (a *API) validate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, err)
		return
	}
	s, err := manager.ParseConfigContent(req.Content)
	if err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if err := manager.ValidateSettings(s); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{
		"ok":     true,
		"name":   s.Name,
		"type":   s.Type,
		"is_url": strings.HasPrefix(strings.TrimSpace(req.Content), "shampoore://"),
	})
}

func (a *API) update(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var entry manager.ConfigEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		writeErr(w, 400, err)
		return
	}

	if a.sup.IsRunning() {
		if err := a.client.Update(name, entry); err != nil {
			writeControlErr(w, err)
			return
		}
	} else {
		path, err := a.findConfigPath(name)
		if err != nil {
			writeErr(w, 404, err)
			return
		}
		if entry.Settings == nil {
			writeErr(w, 400, fmt.Errorf("settings required"))
			return
		}

		newName := entry.Settings.Name
		if newName == "" {
			newName = name
		}
		entry.Settings.Name = newName
		if err := entry.Settings.ApplyDefaults(); err != nil {
			writeErr(w, 400, err)
			return
		}
		if err := manager.ValidateSettings(entry.Settings); err != nil {
			writeErr(w, 400, err)
			return
		}

		data, err := json.MarshalIndent(entry.Settings, "", "  ")
		if err != nil {
			writeErr(w, 500, err)
			return
		}

		if newName != name {
			oldPath := path
			newPath := filepath.Join(a.configsDir, sanitizeName(newName)+".json")
			if err := writeFile0600(newPath, data); err != nil {
				writeErr(w, 500, err)
				return
			}
			if err := os.Remove(oldPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				writeErr(w, 500, err)
				return
			}

			st, err := manager.LoadState(a.configsDir)
			if err != nil {
				writeErr(w, 500, err)
				return
			}
			wasEnabled := st.IsEnabled(name)
			if err := st.Remove(name); err != nil {
				writeErr(w, 500, err)
				return
			}
			if wasEnabled {
				if err := st.Set(newName, true); err != nil {
					writeErr(w, 500, err)
					return
				}
			}
		} else {
			if err := writeFile0600(path, data); err != nil {
				writeErr(w, 500, err)
				return
			}
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *API) remove(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if a.sup.IsRunning() {
		if err := a.client.Remove(name); err != nil {
			writeControlErr(w, err)
			return
		}
	} else {
		path, err := a.findConfigPath(name)
		if err != nil {
			writeErr(w, 404, err)
			return
		}
		if err := os.Remove(path); err != nil {
			writeErr(w, 500, err)
			return
		}
		st, err := manager.LoadState(a.configsDir)
		if err != nil {
			writeErr(w, 500, err)
			return
		}
		if err := st.Remove(name); err != nil {
			writeErr(w, 500, err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *API) enable(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if a.sup.IsRunning() {
		if err := a.client.SetEnabled(name, true); err != nil {
			writeControlErr(w, err)
			return
		}
	} else {
		st, err := manager.LoadState(a.configsDir)
		if err != nil {
			writeErr(w, 500, err)
			return
		}
		if err := st.Set(name, true); err != nil {
			writeErr(w, 500, err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *API) disable(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if a.sup.IsRunning() {
		if err := a.client.SetEnabled(name, false); err != nil {
			writeControlErr(w, err)
			return
		}
	} else {
		st, err := manager.LoadState(a.configsDir)
		if err != nil {
			writeErr(w, 500, err)
			return
		}
		if err := st.Remove(name); err != nil {
			writeErr(w, 500, err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *API) restart(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if a.sup.IsRunning() {
		if err := a.client.Restart(name); err != nil {
			writeControlErr(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "started": false})
		return
	}
	if err := a.sup.Start(); err != nil {
		writeErr(w, 500, fmt.Errorf("start server: %w", err))
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "started": true})
}

func (a *API) logs(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}

	if a.sup.IsRunning() {
		lines, err := a.client.Logs(r.PathValue("name"), limit)
		if err == nil {
			writeJSON(w, 200, map[string]any{"lines": lines})
			return
		}
	}

	writeJSON(w, 200, map[string]any{"lines": []string{"Server is offline, no logs available."}})
}

func sanitizeName(name string) string {
	name = strings.Map(func(r rune) rune {
		if r < 32 || r == '/' || r == '\\' || r == ':' || r == '*' ||
			r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
			return '_'
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." {
		return "config"
	}
	return name
}

func writeControlErr(w http.ResponseWriter, err error) {
	if strings.Contains(err.Error(), "connect") {
		writeErr(w, 503, fmt.Errorf("server offline: %w", err))
		return
	}
	writeErr(w, 400, err)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("panel: write response: %v", err)
	}
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]any{"error": err.Error()})
}

func (a *API) addRule(w http.ResponseWriter, r *http.Request) {
	if a.rulesDir == "" {
		writeErr(w, 400, fmt.Errorf("rules dir not set"))
		return
	}
	var req struct {
		Name  string   `json:"name"`
		CIDRs []string `json:"cidrs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, err)
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		writeErr(w, 400, fmt.Errorf("name is required"))
		return
	}
	safe := sanitizeName(req.Name)
	path := filepath.Join(a.rulesDir, safe+".txt")

	cleanBase := filepath.Clean(a.rulesDir) + string(os.PathSeparator)
	cleanPath := filepath.Clean(path)
	if !strings.HasPrefix(cleanPath, cleanBase) {
		writeErr(w, 400, fmt.Errorf("invalid name"))
		return
	}

	contentStr := strings.Join(req.CIDRs, "\n")
	if err := writeFile0600(cleanPath, []byte(contentStr)); err != nil {
		writeErr(w, 500, err)
		return
	}
	if a.filter != nil {
		if err := a.filter.LoadDir(a.rulesDir); err != nil {
			writeErr(w, 500, err)
			return
		}
	}
	writeJSON(w, 201, map[string]any{"ok": true})
}

func (a *API) deleteRule(w http.ResponseWriter, r *http.Request) {
	if a.rulesDir == "" {
		writeErr(w, 400, fmt.Errorf("rules dir not set"))
		return
	}
	name := r.PathValue("name")
	safe := sanitizeName(name)

	for _, ext := range []string{".txt", ".json"} {
		path := filepath.Join(a.rulesDir, safe+ext)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			writeErr(w, 500, err)
			return
		}
	}
	if a.filter != nil {
		if err := a.filter.LoadDir(a.rulesDir); err != nil {
			writeErr(w, 500, err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}
