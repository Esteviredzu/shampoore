package panel

import (
	"encoding/json"
	"sort"

	"github.com/Esteviredzu/shampoore/internal/app"
)

type Template struct {
	ID          string      `json:"id"`
	DisplayName string      `json:"display_name"`
	Description string      `json:"description"`
	Kind        string      `json:"kind"`
	Example     string      `json:"example"`
	Fields      []app.Field `json:"fields,omitempty"`
}

func LoadTemplates() []Template {
	out := make([]Template, 0)
	for _, d := range app.AllDescriptors() {
		ex := buildExample(d)
		out = append(out, Template{
			ID:          d.Kind,
			DisplayName: d.DisplayName,
			Description: d.Description,
			Kind:        d.Kind,
			Example:     ex,
			Fields:      d.Fields,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

func buildExample(d app.Descriptor) string {
	root := map[string]interface{}{
		"type": d.Kind,
	}

	if d.Kind == "multi" {
		root["transport"] = map[string]interface{}{
			"strategy": "flowmux",
			"transports": []interface{}{
				map[string]interface{}{
					"type":      "yandex",
					"transport": map[string]interface{}{"hash": "your-board-hash"},
				},
			},
		}
	} else {
		tr := make(map[string]interface{})
		for _, f := range d.Fields {
			var val interface{}
			switch f.Type {
			case app.FieldTypeText:
				val = f.Placeholder
				if val == "" {
					val = "value"
				}
			case app.FieldTypePassword:
				val = "secret"
			case app.FieldTypeNumber:
				val = 0
			case app.FieldTypeBool:
				val = false
			}
			tr[f.Key] = val
		}
		root["transport"] = tr
	}

	b, _ := json.MarshalIndent(root, "", "  ")
	return string(b)
}
