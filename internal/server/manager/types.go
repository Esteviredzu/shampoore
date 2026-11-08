package manager

import (
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

type ConfigEntry struct {
	Name     string        `json:"name"`
	Enabled  bool          `json:"enabled"`
	Template string        `json:"template,omitempty"`
	Settings *app.Settings `json:"settings"`
}

type Status string

const (
	StatusStopped  Status = "stopped"
	StatusStarting Status = "starting"
	StatusRunning  Status = "running"
	StatusError    Status = "error"
)

type EntryView struct {
	Name      string        `json:"name"`
	Enabled   bool          `json:"enabled"`
	Template  string        `json:"template,omitempty"`
	Kind      string        `json:"kind"`
	Settings  *app.Settings `json:"settings"`
	Status    Status        `json:"status"`
	Error     string        `json:"error,omitempty"`
	StartedAt *time.Time    `json:"started_at,omitempty"`
	UpdatedAt time.Time     `json:"updated_at"`
	FilePath  string        `json:"file_path,omitempty"`
}

type EntryStats struct {
	Name     string                   `json:"name"`
	Status   Status                   `json:"status"`
	Overall  transport.Snapshot       `json:"overall"`
	Rates    transport.Rates          `json:"rates"`
	Subflows []transport.SubflowStats `json:"subflows,omitempty"`
}