package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/server/logbuf"
	"github.com/Esteviredzu/shampoore/internal/socks5"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type Manager struct {
	dir    string
	logger *logging.Logger
	state  *State

	startAll bool

	mu      sync.Mutex
	entries map[string]*ManagedEntry

	ctx    context.Context
	cancel context.CancelFunc
}

type ManagedEntry struct {
	Name     string
	FilePath string

	settingsMu sync.Mutex
	settings   *app.Settings

	logRing *logbuf.Ring

	mu         sync.Mutex
	status     Status
	lastError  string
	startedAt  time.Time
	updatedAt  time.Time
	generation uint64

	rt      *app.Runtime
	mux     multiplexing.StreamMultiplexer
	handler *socks5.RemoteHandler
	cancel  context.CancelFunc

	statsMu    sync.Mutex
	statsAgg   *transport.StatsAggregator
	cachedStat EntryStats
}

func New(dir string, logger *logging.Logger) *Manager {
	return &Manager{
		dir:     dir,
		logger:  logger,
		entries: map[string]*ManagedEntry{},
		state:   &State{data: map[string]bool{}},
	}
}

func (m *Manager) SetStartAll(v bool) {
	m.startAll = v
}

func (m *Manager) HasDir() bool { return m.dir != "" }

func (m *Manager) Load(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)
	go m.statsLoop()
	if m.dir == "" {
		return nil
	}
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return err
	}
	st, err := LoadState(m.dir)
	if err != nil {
		return err
	}
	m.state = st

	ents, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}
	for _, ent := range ents {
		if ent.IsDir() || strings.HasPrefix(ent.Name(), ".") || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		if err := m.loadFile(filepath.Join(m.dir, ent.Name())); err != nil {
			m.logger.Warn("manager: load ", ent.Name(), ": ", err)
		}
	}
	return nil
}

func (m *Manager) LoadFiles(ctx context.Context, files []string) error {
	m.ctx, m.cancel = context.WithCancel(ctx)
	go m.statsLoop()
	if m.dir != "" {
		if err := os.MkdirAll(m.dir, 0o755); err != nil {
			return err
		}
	}
	st, err := LoadState(m.dir)
	if err != nil {
		return err
	}
	m.state = st

	for _, path := range files {
		if err := m.loadFile(path); err != nil {
			return fmt.Errorf("load %s: %w", path, err)
		}
	}
	return nil
}

func (m *Manager) loadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	settings, err := ParseConfigContent(string(data))
	if err != nil {
		return err
	}
	if settings == nil {
		return errors.New("no settings")
	}

	settings.ApplyDefaults()
	if settings.Name == "" {
		settings.Name = strings.TrimSuffix(filepath.Base(path), ".json")
	}
	if !m.startAll {
		var probe map[string]json.RawMessage
		if json.Unmarshal(data, &probe) == nil {
			if enabledRaw, ok := probe["enabled"]; ok {
				var b bool
				if json.Unmarshal(enabledRaw, &b) == nil && b {
					if !m.state.IsEnabled(settings.Name) {
						_ = m.state.Set(settings.Name, true)
					}
				}
			}
		}
	}

	if err := writeSettingsFile(path, settings); err != nil {
		m.logger.Warn("manager: migrate ", path, ": ", err)
	}

	me := &ManagedEntry{
		Name:      settings.Name,
		FilePath:  path,
		settings:  settings,
		logRing:   logbuf.New(500),
		status:    StatusStopped,
		updatedAt: time.Now(),
	}

	m.mu.Lock()
	old, had := m.entries[settings.Name]
	m.entries[settings.Name] = me
	m.mu.Unlock()

	if had {
		m.stopEntry(old)
	}

	if m.startAll || m.state.IsEnabled(settings.Name) {
		m.startEntry(me)
	}
	return nil
}

func (m *Manager) startEntry(me *ManagedEntry) {
	me.mu.Lock()
	if me.status == StatusRunning || me.status == StatusStarting {
		me.mu.Unlock()
		return
	}
	me.status = StatusStarting
	me.lastError = ""
	me.generation++
	gen := me.generation
	me.mu.Unlock()

	go func() {
		err := m.startEntrySync(me, gen)
		me.mu.Lock()
		if me.generation != gen {
			me.mu.Unlock()
			return
		}
		if err != nil {
			me.status = StatusError
			me.lastError = err.Error()
			me.mu.Unlock()
			m.logger.Warn("manager: ", me.Name, ": ", err)
			return
		}
		me.status = StatusRunning
		me.startedAt = time.Now()
		me.mu.Unlock()
		m.logger.Info("manager: ", me.Name, " running")
	}()
}

func (m *Manager) startEntrySync(me *ManagedEntry, gen uint64) error {
	me.settingsMu.Lock()
	settings := me.settings
	me.settingsMu.Unlock()

	logger := logging.New("cfg:"+me.Name, true)
	logger.AddSink(me.logRing)

	ctx, cancel := context.WithCancel(m.ctx)

	rt, err := app.BuildRuntime(ctx, settings, "server", logger)
	if err != nil {
		cancel()
		return fmt.Errorf("build runtime: %w", err)
	}

	var mux multiplexing.StreamMultiplexer
	if rt.StreamMux != nil {
		mux = rt.StreamMux
		logger.Info("manager: ", me.Name, " using stream mux")
	} else {
		muxCfg := tunnel.Config{
			FlushThreshold: rt.MuxConfig.FlushThreshold,
			FlushInterval:  time.Duration(rt.MuxConfig.FlushMs) * time.Millisecond,
		}
		mux = tunnel.New(rt.Transport, logger, muxCfg)
	}

	if err := mux.Start(ctx); err != nil {
		cancel()
		_ = rt.Stop(context.Background())
		return fmt.Errorf("mux start: %w", err)
	}
	if err := rt.Start(ctx); err != nil {
		_ = mux.Stop(context.Background())
		cancel()
		return fmt.Errorf("runtime start: %w", err)
	}

	handler := socks5.NewRemoteHandler(ctx, mux, logger, 8000, app.IPv4Only)
	go func() {
		if err := handler.Run(); err != nil {
			logger.Warn("remote handler stopped: ", err)
		}
	}()

	me.mu.Lock()
	if me.generation != gen {
		me.mu.Unlock()
		handler.Stop()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = mux.Stop(stopCtx)
		_ = rt.Stop(stopCtx)
		stopCancel()
		cancel()
		return nil
	}
	me.rt = rt
	me.mux = mux
	me.handler = handler
	me.cancel = cancel
	me.statsAgg = transport.NewStatsAggregator()
	me.mu.Unlock()
	return nil
}

func (m *Manager) stopEntry(me *ManagedEntry) {
	me.mu.Lock()
	me.generation++
	cancel := me.cancel
	handler := me.handler
	mux := me.mux
	rt := me.rt
	me.cancel = nil
	me.handler = nil
	me.mux = nil
	me.rt = nil
	me.statsAgg = nil
	me.status = StatusStopped
	me.startedAt = time.Time{}
	me.mu.Unlock()

	me.statsMu.Lock()
	me.cachedStat = EntryStats{Name: me.Name, Status: StatusStopped}
	me.statsMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if handler != nil {
		handler.Stop()
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if mux != nil {
		_ = mux.Stop(stopCtx)
	}
	if rt != nil {
		_ = rt.Stop(stopCtx)
	}
}

func (m *Manager) List() []EntryView {
	m.mu.Lock()
	list := make([]*ManagedEntry, 0, len(m.entries))
	for _, e := range m.entries {
		list = append(list, e)
	}
	m.mu.Unlock()

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	out := make([]EntryView, len(list))
	for i, e := range list {
		out[i] = e.view(m.state, m.startAll)
	}
	return out
}

func (m *Manager) Get(name string) (EntryView, bool) {
	m.mu.Lock()
	e, ok := m.entries[name]
	m.mu.Unlock()
	if !ok {
		return EntryView{}, false
	}
	return e.view(m.state, m.startAll), true
}

func (e *ManagedEntry) view(st *State, startAll bool) EntryView {
	e.settingsMu.Lock()
	settings := e.settings
	e.settingsMu.Unlock()

	e.mu.Lock()
	status := e.status
	errStr := e.lastError
	started := e.startedAt
	updated := e.updatedAt
	e.mu.Unlock()

	var startedPtr *time.Time
	if !started.IsZero() {
		startedPtr = &started
	}
	var kind string
	if settings != nil {
		kind = settings.Type
	}
	enabled := startAll
	if !startAll && st != nil {
		enabled = st.IsEnabled(e.Name)
	}
	return EntryView{
		Name: e.Name, Enabled: enabled, Kind: kind,
		Settings: settings, Status: status, Error: errStr,
		StartedAt: startedPtr, UpdatedAt: updated, FilePath: e.FilePath,
	}
}

func (m *Manager) Stats() []EntryStats {
	m.mu.Lock()
	list := make([]*ManagedEntry, 0, len(m.entries))
	for _, e := range m.entries {
		list = append(list, e)
	}
	m.mu.Unlock()

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	out := make([]EntryStats, len(list))
	for i, e := range list {
		out[i] = e.Stats()
	}
	return out
}

func (me *ManagedEntry) Stats() EntryStats {
	me.statsMu.Lock()
	cached := me.cachedStat
	me.statsMu.Unlock()
	if cached.Name == "" {
		return me.refreshStats()
	}
	return cached
}

func (me *ManagedEntry) refreshStats() EntryStats {
	me.mu.Lock()
	mux := me.mux
	agg := me.statsAgg
	status := me.status
	me.mu.Unlock()

	if mux == nil || agg == nil {
		out := EntryStats{Name: me.Name, Status: status}
		me.statsMu.Lock()
		me.cachedStat = out
		me.statsMu.Unlock()
		return out
	}

	tel, ok := mux.(transport.Telemetry)
	if !ok {
		out := EntryStats{Name: me.Name, Status: status}
		me.statsMu.Lock()
		me.cachedStat = out
		me.statsMu.Unlock()
		return out
	}

	stats := agg.Update(tel.WalkLayers())
	out := EntryStats{
		Name:     me.Name,
		Status:   status,
		Overall:  stats.Overall,
		Rates:    stats.Rates,
		Subflows: stats.Subflows,
	}
	me.statsMu.Lock()
	me.cachedStat = out
	me.statsMu.Unlock()
	return out
}

func (m *Manager) statsLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.logStats()
		}
	}
}

func (m *Manager) logStats() {
	m.mu.Lock()
	list := make([]*ManagedEntry, 0, len(m.entries))
	for _, e := range m.entries {
		list = append(list, e)
	}
	m.mu.Unlock()

	for _, e := range list {
		st := e.refreshStats()
		if st.Status != StatusRunning {
			continue
		}
		m.logger.Info("[stats] ", st.Name,
			" app_tx=", formatRate(st.Rates.UsefulSentBPS),
			" app_rx=", formatRate(st.Rates.UsefulRecvBPS),
			" wire_tx=", formatRate(st.Rates.WireSentBPS),
			" wire_rx=", formatRate(st.Rates.WireRecvBPS),
			" eff=", formatPct(st.Rates.Efficiency))
		for _, sf := range st.Subflows {
			m.logger.Info("[stats]   ", sf.Name,
				" share=", formatPct(sf.Share),
				" wire_tx=", formatRate(sf.Rates.WireSentBPS),
				" useful_tx=", formatRate(sf.Rates.UsefulSentBPS),
				" eff=", formatPct(sf.Efficiency))
		}
	}
}

func formatRate(bps float64) string {
	if bps < 0 {
		bps = 0
	}
	return formatSize(int64(bps)) + "/s"
}

func formatSize(n int64) string {
	if n <= 0 {
		return "0B"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func formatPct(f float64) string {
	if f < 0 {
		f = 0
	}
	return fmt.Sprintf("%.1f%%", f*100)
}

func (m *Manager) Add(name, template, content string) error {
	if m.dir == "" {
		return errors.New("manager: directory mode required")
	}

	settings, err := ParseConfigContent(content)
	if err != nil {
		return err
	}
	if err := ValidateSettings(settings); err != nil {
		return err
	}

	if name == "" {
		name = settings.Name
	}
	if name == "" {
		return errors.New("name is required")
	}
	if !validName(name) {
		return errors.New("invalid name (allowed: a-zA-Z0-9_-)")
	}

	settings.Name = name

	path := filepath.Join(m.dir, sanitizeName(name)+".json")
	if err := writeSettingsFile(path, settings); err != nil {
		return err
	}
	_ = m.state.Set(name, true)
	return m.loadFile(path)
}

func (m *Manager) Update(name string, entry ConfigEntry) error {
	m.mu.Lock()
	me, ok := m.entries[name]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("config %q not found", name)
	}
	if me.FilePath == "" {
		return errors.New("cannot update: no file path")
	}
	if entry.Settings == nil {
		return errors.New("settings required")
	}

	newName := entry.Settings.Name
	if newName == "" {
		newName = name
	}
	entry.Settings.Name = newName

	if err := entry.Settings.ApplyDefaults(); err != nil {
		return err
	}

	m.stopEntry(me)

	if newName != name {
		newPath := filepath.Join(filepath.Dir(me.FilePath), sanitizeName(newName)+".json")
		os.Remove(me.FilePath)
		me.FilePath = newPath
		me.Name = newName

		m.mu.Lock()
		delete(m.entries, name)
		m.entries[newName] = me
		m.mu.Unlock()

		wasEnabled := m.state.IsEnabled(name)
		m.state.Remove(name)
		if wasEnabled {
			m.state.Set(newName, true)
		}
	}

	if err := writeSettingsFile(me.FilePath, entry.Settings); err != nil {
		return err
	}

	me.settingsMu.Lock()
	me.settings = entry.Settings
	me.settingsMu.Unlock()
	me.mu.Lock()
	me.updatedAt = time.Now()
	me.mu.Unlock()

	if m.state.IsEnabled(newName) || m.startAll {
		m.startEntry(me)
	}
	return nil
}

func (m *Manager) Remove(name string) error {
	m.mu.Lock()
	me, ok := m.entries[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("config %q not found", name)
	}
	delete(m.entries, name)
	m.mu.Unlock()

	m.stopEntry(me)
	if me.FilePath != "" {
		_ = os.Remove(me.FilePath)
	}
	_ = m.state.Remove(name)
	return nil
}

func (m *Manager) SetEnabled(name string, enabled bool) error {
	m.mu.Lock()
	me, ok := m.entries[name]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("config %q not found", name)
	}

	if m.state.IsEnabled(name) == enabled {
		return nil
	}

	if err := m.state.Set(name, enabled); err != nil {
		return err
	}

	me.mu.Lock()
	me.updatedAt = time.Now()
	me.mu.Unlock()

	if enabled {
		m.startEntry(me)
	} else {
		m.stopEntry(me)
	}
	return nil
}

func (m *Manager) Restart(name string) error {
	m.mu.Lock()
	me, ok := m.entries[name]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("config %q not found", name)
	}
	m.stopEntry(me)

	if me.FilePath != "" {
		if data, err := os.ReadFile(me.FilePath); err == nil {
			if settings, err := ParseConfigContent(string(data)); err == nil && settings != nil {
				settings.ApplyDefaults()
				if settings.Name == "" {
					settings.Name = name
				}
				me.settingsMu.Lock()
				me.settings = settings
				me.settingsMu.Unlock()
				me.mu.Lock()
				me.updatedAt = time.Now()
				me.mu.Unlock()
			}
		}
	}

	if m.startAll || m.state.IsEnabled(name) {
		m.startEntry(me)
	}
	return nil
}

func (m *Manager) Logs(name string, limit int) ([]string, bool) {
	m.mu.Lock()
	me, ok := m.entries[name]
	m.mu.Unlock()
	if !ok {
		return nil, false
	}
	return me.logRing.Lines(limit), true
}

func (m *Manager) Close() {
	m.mu.Lock()
	list := make([]*ManagedEntry, 0, len(m.entries))
	for _, e := range m.entries {
		list = append(list, e)
	}
	m.mu.Unlock()

	for _, e := range list {
		m.stopEntry(e)
	}
	if m.cancel != nil {
		m.cancel()
	}
}

func writeSettingsFile(path string, s *app.Settings) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func validName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
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
