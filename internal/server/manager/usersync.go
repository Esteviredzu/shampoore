package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/userlimit"
)

func (m *Manager) ActiveUserIDs() []string {
	if m == nil || m.limits == nil {
		return nil
	}
	return m.limits.ActiveUsers(userlimit.ActiveWindow)
}

func (m *Manager) syncUserLimits() {
	if m == nil || m.limits == nil {
		return
	}
	m.mu.Lock()
	entries := make([]*ManagedEntry, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	m.mu.Unlock()

	limits := make(map[string]int64)
	for _, me := range entries {
		me.settingsMu.Lock()
		settings := me.settings
		me.settingsMu.Unlock()
		if settings == nil {
			continue
		}
		for _, spec := range app.UsersFromTransport(settings.Transport) {
			bps := spec.MaxSpeedKBps * 1024
			if bps < 0 {
				bps = 0
			}
			if prev, ok := limits[spec.UserID]; !ok || bps > prev {
				limits[spec.UserID] = bps
			}
		}
	}
	m.limits.ReplaceLimits(limits)
}

func (m *Manager) SyncUsers(name string, entry ConfigEntry) (bool, error) {
	m.mu.Lock()
	me, ok := m.entries[name]
	m.mu.Unlock()
	if !ok {
		return false, fmt.Errorf("config %q not found", name)
	}
	if entry.Settings == nil {
		return false, fmt.Errorf("settings required")
	}
	newName := entry.Settings.Name
	if newName == "" {
		newName = name
	}
	entry.Settings.Name = newName
	if newName != name {
		return true, m.Update(name, entry)
	}
	if err := entry.Settings.ApplyDefaults(); err != nil {
		return false, err
	}
	if err := ValidateSettings(entry.Settings); err != nil {
		return false, err
	}

	me.settingsMu.Lock()
	old := me.settings
	me.settingsMu.Unlock()
	if old == nil || !sameTransportIgnoringUsers(old.Transport, entry.Settings.Transport) {
		return true, m.Update(name, entry)
	}

	me.mu.Lock()
	rt := me.rt
	filePath := me.FilePath
	me.mu.Unlock()
	if filePath != "" {
		if err := writeSettingsFile(filePath, entry.Settings); err != nil {
			return false, err
		}
	}
	me.settingsMu.Lock()
	me.settings = entry.Settings
	me.settingsMu.Unlock()
	me.mu.Lock()
	me.updatedAt = time.Now()
	me.mu.Unlock()

	if rt != nil {
		specs := app.UsersFromTransport(entry.Settings.Transport)
		for _, reg := range rt.Registries() {
			if err := reg.Replace(specs); err != nil {
				return false, fmt.Errorf("apply users to %q: %w", name, err)
			}
		}
	}
	m.syncUserLimits()
	return false, nil
}

func sameTransportIgnoringUsers(before, after json.RawMessage) bool {
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
