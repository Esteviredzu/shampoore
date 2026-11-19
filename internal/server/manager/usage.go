package manager

import "sort"

// UsageEntry is a per-config, per-user traffic snapshot.
type UsageEntry struct {
	Config        string `json:"config"`
	UserID        string `json:"user_id"`
	UploadBytes   int64  `json:"upload_bytes"`
	DownloadBytes int64  `json:"download_bytes"`
}

// Usage returns cumulative traffic counters from every running multiuser config.
func (m *Manager) Usage() []UsageEntry {
	m.mu.Lock()
	entries := make([]*ManagedEntry, 0, len(m.entries))
	names := make(map[*ManagedEntry]string, len(m.entries))
	for name, e := range m.entries {
		entries = append(entries, e)
		names[e] = name
	}
	m.mu.Unlock()

	out := make([]UsageEntry, 0)
	for _, e := range entries {
		e.mu.Lock()
		rt := e.rt
		name := names[e]
		e.mu.Unlock()
		if rt == nil || rt.UserRegistry == nil {
			continue
		}
		for _, info := range rt.UserRegistry.Snapshot() {
			out = append(out, UsageEntry{
				Config:        name,
				UserID:        info.UserID,
				UploadBytes:   info.Usage.UploadBytes,
				DownloadBytes: info.Usage.DownloadBytes,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Config == out[j].Config {
			return out[i].UserID < out[j].UserID
		}
		return out[i].Config < out[j].Config
	})
	return out
}
