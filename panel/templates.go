package panel

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Esteviredzu/shampoore-core/internal/app"
)

type Template struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
	Kind        string `json:"kind"`
	Example     string `json:"example"`
}

func LoadTemplates(dir string) []Template {
	var out []Template
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	descs := make(map[string]app.Descriptor)
	for _, d := range app.AllDescriptors() {
		descs[d.Kind] = d
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json.example") {
			continue
		}

		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}

		var probe struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &probe); err != nil {
			continue
		}

		id := strings.TrimSuffix(e.Name(), ".json.example")
		displayName := probe.Type
		desc := ""

		if d, ok := descs[probe.Type]; ok {
			displayName = d.DisplayName
			desc = d.Description
		}

		out = append(out, Template{
			ID:          id,
			DisplayName: displayName,
			Description: desc,
			Kind:        probe.Type,
			Example:     string(data),
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}