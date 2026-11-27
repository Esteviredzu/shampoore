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
	Sync(ctx context.Context, nodeID int64, traffic []panelstore.TrafficDelta, metrics *panelstore.NodeMetrics, activeUserIDs []string) ([]auth.UserSpec, error)
}

type ControlClient interface {
	Usage() ([]manager.UsageEntry, error)
	List() ([]manager.EntryView, error)
	Update(name string, entry manager.ConfigEntry) error
	SyncUsers(name string, entry manager.ConfigEntry) (bool, error)
	ActiveUserIDs() ([]string, error)
}

type Config struct {
	PanelURL      string
	NodeID        int64
	NodeSecret    string
	ControlSocket string
	ConfigsDir    string
	StateFile     string
	Interval      time.Duration
	Metrics       MetricsSource
}

type Agent struct {
	cfg     Config
	panel   PanelClient
	control ControlClient
	metrics MetricsSource

	mu             sync.Mutex
	state          stateFile
	lastMetricsErr time.Time
	lastControlErr time.Time
}

type baseline struct {
	Upload   int64 `json:"upload"`
	Download int64 `json:"download"`
}

type stateFile struct {
	Entries map[string]baseline `json:"entries"`
}

const defaultSyncInterval = 2 * time.Second

func New(cfg Config) *Agent {
	if cfg.Interval < defaultSyncInterval {
		cfg.Interval = defaultSyncInterval
	}
	if cfg.StateFile == "" {
		cfg.StateFile = "/opt/shampoore/agent-state.json"
	}
	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewHostMetrics()
	}
	return &Agent{
		cfg:     cfg,
		panel:   NewHTTPPanel(cfg.PanelURL, cfg.NodeSecret),
		control: control.NewClient(cfg.ControlSocket),
		metrics: metrics,
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
	if a.panel == nil {
		return fmt.Errorf("nodeagent: panel client is not configured")
	}

	usage, usageOK := a.collectUsage()
	next, traffic := a.trafficDeltas(usage, usageOK)
	metrics := a.sampleMetrics()
	active := a.activeUserIDs()
	users, err := a.panel.Sync(ctx, a.cfg.NodeID, traffic, metrics, active)
	if err != nil {
		return fmt.Errorf("nodeagent: panel sync: %w", err)
	}

	restarted, applyErr := a.applyUsers(users)
	if applyErr != nil {
		a.logControlFailure("apply users", applyErr)
	}

	if !usageOK {

		return nil
	}

	a.mu.Lock()
	a.state.Entries = next
	for _, config := range restarted {
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

func (a *Agent) collectUsage() ([]manager.UsageEntry, bool) {
	if a.control == nil {
		return nil, false
	}
	usage, err := a.control.Usage()
	if err != nil {
		a.logControlFailure("usage", err)
		return nil, false
	}
	return usage, true
}

func (a *Agent) logControlFailure(op string, err error) {
	a.mu.Lock()
	shouldLog := a.lastControlErr.IsZero() || time.Since(a.lastControlErr) > time.Minute
	if shouldLog {
		a.lastControlErr = time.Now()
	}
	a.mu.Unlock()
	if shouldLog {
		log.Printf("node-agent: control %s: %v", op, err)
	}
}

func (a *Agent) trafficDeltas(usage []manager.UsageEntry, usageOK bool) (map[string]baseline, []panelstore.TrafficDelta) {
	if !usageOK {
		return nil, []panelstore.TrafficDelta{}
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
	return next, traffic
}

func (a *Agent) activeUserIDs() []string {
	if a.control == nil {
		return nil
	}
	ids, err := a.control.ActiveUserIDs()
	if err != nil {
		a.logControlFailure("active users", err)
		return nil
	}
	return ids
}

func (a *Agent) applyUsers(users []auth.UserSpec) ([]string, error) {
	entries, err := a.control.List()
	if err != nil {
		return nil, err
	}
	var restarted []string
	for _, entry := range entries {
		if entry.Settings == nil {
			continue
		}
		before := append(json.RawMessage(nil), entry.Settings.Transport...)
		changed, err := patchUsers(entry.Settings, users)
		if err != nil {
			return restarted, fmt.Errorf("config %q: %w", entry.Name, err)
		}
		if !changed {
			continue
		}
		cfgEntry := manager.ConfigEntry{
			Name:     entry.Name,
			Enabled:  entry.Enabled,
			Template: entry.Template,
			Settings: entry.Settings,
		}
		if usersOnlyChange(before, entry.Settings.Transport) {
			didRestart, syncErr := a.control.SyncUsers(entry.Name, cfgEntry)
			if syncErr == nil {
				if didRestart {
					restarted = append(restarted, entry.Name)
				}
				continue
			}
		}
		if err := a.control.Update(entry.Name, cfgEntry); err != nil {
			return restarted, fmt.Errorf("config %q: %w", entry.Name, err)
		}
		restarted = append(restarted, entry.Name)
	}
	return restarted, nil
}

func usersOnlyChange(before, after json.RawMessage) bool {
	a, err := app.TransportWithoutUsers(before)
	if err != nil {
		return false
	}
	b, err := app.TransportWithoutUsers(after)
	if err != nil {
		return false
	}
	return bytes.Equal(a, b)
}

func patchUsers(settings *app.Settings, users []auth.UserSpec) (bool, error) {
	if settings == nil || len(settings.Transport) == 0 {
		return false, nil
	}
	if descriptor, ok := app.DescriptorFor(settings.Type); ok && descriptor.Caps.Multiuser != nil {
		updated, changed, err := descriptor.Caps.Multiuser.PatchUsers(settings.Transport, users)
		if err != nil || !changed {
			return changed, err
		}
		settings.Transport = updated
		return true, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(settings.Transport, &raw); err != nil {
		return false, err
	}
	changed, err := patchUsersMap(raw, users)
	if err != nil || !changed {
		return changed, err
	}
	updated, err := json.Marshal(raw)
	if err != nil {
		return false, err
	}
	settings.Transport = updated
	return true, nil
}

func patchUsersMap(raw map[string]json.RawMessage, users []auth.UserSpec) (bool, error) {
	if childrenRaw, ok := raw["transports"]; ok {
		var children []json.RawMessage
		if err := json.Unmarshal(childrenRaw, &children); err != nil {
			return false, err
		}
		changed := false
		for i, child := range children {
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(child, &obj); err != nil {
				return false, err
			}
			trRaw, ok := obj["transport"]
			if !ok {
				continue
			}
			var trObj map[string]json.RawMessage
			if err := json.Unmarshal(trRaw, &trObj); err != nil {
				return false, err
			}
			childChanged, err := patchUsersMap(trObj, users)
			if err != nil {
				return false, err
			}
			if !childChanged {
				continue
			}
			updatedTr, err := json.Marshal(trObj)
			if err != nil {
				return false, err
			}
			obj["transport"] = updatedTr
			updatedChild, err := json.Marshal(obj)
			if err != nil {
				return false, err
			}
			children[i] = updatedChild
			changed = true
		}
		if !changed {
			return false, nil
		}
		updatedChildren, err := json.Marshal(children)
		if err != nil {
			return false, err
		}
		raw["transports"] = updatedChildren
		return true, nil
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
