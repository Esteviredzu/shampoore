package control

import (
	"encoding/json"

	"github.com/Esteviredzu/shampoore-core/internal/server/manager"
)

type Request struct {
	Cmd      string               `json:"cmd"`
	Name     string               `json:"name,omitempty"`
	Template string               `json:"template,omitempty"`
	Content  string               `json:"content,omitempty"`
	Entry    *manager.ConfigEntry `json:"entry,omitempty"`
	Limit    int                  `json:"limit,omitempty"`
}

type Response struct {
	OK    bool            `json:"ok"`
	Error string          `json:"error,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
}
