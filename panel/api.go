package panel

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/control"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/server/manager"
)

//go:embed static
var staticFS embed.FS

type API struct {
	client      *control.Client
	sup         ServerManager
	logger      *logging.Logger
	token       string
	examplesDir string
	configsDir  string
}

func New(client *control.Client, sup ServerManager, logger *logging.Logger, token, examplesDir, configsDir string) *API {
	return &API{
		client:      client,
		sup:         sup,
		logger:      logger,
		token:       token,
		examplesDir: examplesDir,
		configsDir:  configsDir,
	}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/server/status", a.serverStatus)
	mux.HandleFunc("POST /api/server/start", a.serverStart)
	mux.HandleFunc("POST /api/server/stop", a.serverStop)
	mux.HandleFunc("GET /api/server/logs", a.serverLogs)

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

	sub, _ := fs.Sub(staticFS, "static")
	mux.Handle("/", http.FileServer(http.FS(sub)))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.token != "" && strings.HasPrefix(r.URL.Path, "/api/") {
			if r.Header.Get("X-Panel-Token") != a.token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
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
	st, _ := manager.LoadState(a.configsDir)
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
		settings.ApplyDefaults()
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

func (a *API) templates(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, LoadTemplates(a.examplesDir))
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
	data, _ := os.ReadFile(path)
	settings, err := manager.ParseConfigContent(string(data))
	if err != nil || settings == nil {
		writeErr(w, 500, fmt.Errorf("invalid config"))
		return
	}
	settings.ApplyDefaults()
	st, _ := manager.LoadState(a.configsDir)
	enabled := false
	if st != nil {
		enabled = st.IsEnabled(name)
	}
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
		data, _ := os.ReadFile(path)
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
		data, _ := json.MarshalIndent(settings, "", "  ")
		if err := os.WriteFile(path, data, 0644); err != nil {
			writeErr(w, 500, err)
			return
		}
		st, _ := manager.LoadState(a.configsDir)
		if st != nil {
			_ = st.Set(req.Name, true)
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
		entry.Settings.ApplyDefaults()
		entry.Settings.Name = name
		data, _ := json.MarshalIndent(entry.Settings, "", "  ")
		if err := os.WriteFile(path, data, 0644); err != nil {
			writeErr(w, 500, err)
			return
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
		st, _ := manager.LoadState(a.configsDir)
		if st != nil {
			_ = st.Remove(name)
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
	}
	writeJSON(w, 200, map[string]any{"ok": true})
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
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]any{"error": err.Error()})
}
