package panel

import (
	"encoding/json"
	"sort"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/capability"
)

type TemplateCapabilities struct {
	Deployable   bool                    `json:"deployable"`
	DefaultRelay bool                    `json:"default_relay,omitempty"`
	ClientConfig bool                    `json:"client_config"`
	ServerConfig bool                    `json:"server_config"`
	Multiuser    bool                    `json:"multiuser"`
	Decoy        bool                    `json:"decoy"`
	Multi        bool                    `json:"multi"`
	Strategies   []app.MultiStrategy     `json:"strategies,omitempty"`
	Secrets      []capability.SecretSpec `json:"secrets,omitempty"`
}

type Template struct {
	ID           string               `json:"id"`
	DisplayName  string               `json:"display_name"`
	Description  string               `json:"description"`
	Kind         string               `json:"kind"`
	Example      string               `json:"example"`
	Fields       []app.Field          `json:"fields,omitempty"`
	Capabilities TemplateCapabilities `json:"capabilities"`
}

func LoadTemplates() []Template {
	out := make([]Template, 0)
	for _, d := range app.AllDescriptors() {
		ex := buildExample(d)
		out = append(out, Template{
			ID:           d.Kind,
			DisplayName:  d.DisplayName,
			Description:  d.Description,
			Kind:         d.Kind,
			Example:      ex,
			Fields:       d.Fields,
			Capabilities: templateCapabilities(d),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

func templateCapabilities(d app.Descriptor) TemplateCapabilities {
	caps := TemplateCapabilities{
		Deployable:   d.Caps.Relay != nil,
		DefaultRelay: d.DefaultRelay,
		ClientConfig: d.Caps.Client != nil,
		ServerConfig: d.Caps.Server != nil,
		Multiuser:    d.Caps.Multiuser != nil,
		Decoy:        d.SupportsDecoy(),
		Multi:        d.Caps.Multi != nil,
	}
	if d.Caps.Multi != nil {
		caps.Strategies = d.Caps.Multi.Strategies()
	}
	if d.Caps.Secrets != nil {
		caps.Secrets = d.Caps.Secrets.SecretSpecs()
	}
	return caps
}

func buildExample(d app.Descriptor) string {
	root := map[string]interface{}{
		"type": d.Kind,
	}

	if d.Caps.Multi != nil {
		strategy := ""
		if strategies := d.Caps.Multi.Strategies(); len(strategies) > 0 {
			strategy = strategies[0].ID
		}
		root["transport"] = map[string]interface{}{
			"strategy":   strategy,
			"transports": []interface{}{},
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
