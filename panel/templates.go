package panel

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/app"
)

type Template struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
	Kind        string `json:"kind"`
	Example     string `json:"example"`
}

func LoadTemplates(dir string) []Template {
	out := make([]Template, 0)
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("[panel] templates: cannot read %q: %v", dir, err)
		return out
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
			log.Printf("[panel] templates: read %s: %v", e.Name(), err)
			continue
		}

		var probe struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &probe); err != nil {
			log.Printf("[panel] templates: parse %s: %v", e.Name(), err)
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
