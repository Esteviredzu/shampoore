package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/control"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/server/manager"
)

type PanelClient interface {
	Sync(ctx context.Context, nodeID int64, traffic []panelstore.TrafficDelta) ([]auth.UserSpec, error)
}

type ControlClient interface {
	Usage() ([]manager.UsageEntry, error)
	List() ([]manager.EntryView, error)
	Update(name string, entry manager.ConfigEntry) error
}

type Config struct {
	PanelURL      string
	NodeID        int64
	NodeSecret    string
	ControlSocket string
	ConfigsDir    string
	StateFile     string
	Interval      time.Duration
}

type Agent struct {
	cfg     Config
	panel   PanelClient
	control ControlClient

	mu    sync.Mutex
	state stateFile
}

type baseline struct {
	Upload   int64 `json:"upload"`
	Download int64 `json:"download"`
}

type stateFile struct {
	Entries map[string]baseline `json:"entries"`
}

func New(cfg Config) *Agent {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.StateFile == "" {
		cfg.StateFile = "/opt/shampoore/agent-state.json"
	}
	return &Agent{
		cfg:     cfg,
		panel:   NewHTTPPanel(cfg.PanelURL, cfg.NodeSecret),
		control: control.NewClient(cfg.ControlSocket),
		state:   stateFile{Entries: map[string]baseline{}},
	}
}

func NewWithClients(cfg Config, panel PanelClient, ctrl ControlClient) *Agent {
	a := New(cfg)
	if panel != nil {
		a.panel = panel
	}
	if ctrl != nil {
		a.control = ctrl
	}
	return a
}

func stateKey(config, userID string) string { return config + "\x00" + userID }

func (a *Agent) Run(ctx context.Context) error {
	a.loadState()
	for {
		if err := a.SyncOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("node-agent: sync: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(a.cfg.Interval):
		}
	}
}

func (a *Agent) SyncOnce(ctx context.Context) error {
	if a.panel == nil || a.control == nil {
		return fmt.Errorf("nodeagent: clients are not configured")
	}
	usage, err := a.control.Usage()
	if err != nil {
		return fmt.Errorf("nodeagent: usage: %w", err)
	}

	a.mu.Lock()
	next := make(map[string]baseline, len(usage))
	deltas := make(map[string]*panelstore.TrafficDelta)
	for _, item := range usage {
		key := stateKey(item.Config, item.UserID)
		prev := a.state.Entries[key]
		up := item.UploadBytes - prev.Upload
		down := item.DownloadBytes - prev.Download
		if up < 0 {
			up = item.UploadBytes
		}
		if down < 0 {
			down = item.DownloadBytes
		}
		next[key] = baseline{Upload: item.UploadBytes, Download: item.DownloadBytes}
		if up == 0 && down == 0 {
			continue
		}
		d := deltas[item.UserID]
		if d == nil {
			d = &panelstore.TrafficDelta{UserID: item.UserID}
			deltas[item.UserID] = d
		}
		d.UploadBytes += up
		d.DownloadBytes += down
	}
	a.mu.Unlock()

	traffic := make([]panelstore.TrafficDelta, 0, len(deltas))
	for _, d := range deltas {
		traffic = append(traffic, *d)
	}
	users, err := a.panel.Sync(ctx, a.cfg.NodeID, traffic)
	if err != nil {
		return fmt.Errorf("nodeagent: panel sync: %w", err)
	}

	updated, err := a.applyUsers(users)
	if err != nil {
		return fmt.Errorf("nodeagent: apply users: %w", err)
	}

	a.mu.Lock()
	a.state.Entries = next
	for _, config := range updated {
		prefix := config + "\x00"
		for key := range a.state.Entries {
			if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
				delete(a.state.Entries, key)
			}
		}
	}
	snapshot := stateFile{Entries: make(map[string]baseline, len(a.state.Entries))}
	for k, v := range a.state.Entries {
		snapshot.Entries[k] = v
	}
	a.mu.Unlock()

	return a.saveState(snapshot)
}

func (a *Agent) applyUsers(users []auth.UserSpec) ([]string, error) {
	entries, err := a.control.List()
	if err != nil {
		return nil, err
	}
	var updated []string
	for _, entry := range entries {
		if entry.Settings == nil {
			continue
		}
		changed, err := patchUsers(entry.Settings, users)
		if err != nil {
			return updated, fmt.Errorf("config %q: %w", entry.Name, err)
		}
		if !changed {
			continue
		}
		if err := a.control.Update(entry.Name, manager.ConfigEntry{
			Name:     entry.Name,
			Enabled:  entry.Enabled,
			Template: entry.Template,
			Settings: entry.Settings,
		}); err != nil {
			return updated, fmt.Errorf("config %q: %w", entry.Name, err)
		}
		updated = append(updated, entry.Name)
	}
	return updated, nil
}

func patchUsers(settings *app.Settings, users []auth.UserSpec) (bool, error) {
	if settings == nil || len(settings.Transport) == 0 {
		return false, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(settings.Transport, &raw); err != nil {
		return false, err
	}
	var multiuser bool
	if v, ok := raw["multiuser"]; ok {
		if err := json.Unmarshal(v, &multiuser); err != nil {
			return false, err
		}
	}
	if !multiuser {
		return false, nil
	}
	newUsers, err := json.Marshal(users)
	if err != nil {
		return false, err
	}
	old, hadUsers := raw["users"]
	if hadUsers && bytes.Equal(bytes.TrimSpace(old), newUsers) {
		return false, nil
	}
	if !hadUsers && len(users) == 0 {
		return false, nil
	}
	raw["users"] = newUsers
	updated, err := json.Marshal(raw)
	if err != nil {
		return false, err
	}
	settings.Transport = updated
	return true, nil
}

func (a *Agent) loadState() {
	if a.cfg.StateFile == "" {
		return
	}
	data, err := os.ReadFile(a.cfg.StateFile)
	if err != nil {
		return
	}
	var st stateFile
	if err := json.Unmarshal(data, &st); err != nil || st.Entries == nil {
		return
	}
	a.mu.Lock()
	a.state = st
	a.mu.Unlock()
}

func (a *Agent) saveState(st stateFile) error {
	if a.cfg.StateFile == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(a.cfg.StateFile), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := a.cfg.StateFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, a.cfg.StateFile)
}
