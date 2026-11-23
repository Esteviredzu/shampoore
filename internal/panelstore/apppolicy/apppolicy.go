package apppolicy

import (
	"sort"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/capability"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type Policy struct{}

var _ panelstore.TransportPolicy = Policy{}

func (Policy) Known(kind string) bool {
	_, ok := app.DescriptorFor(kind)
	return ok
}

func (Policy) IsMulti(kind string) bool {
	d, ok := app.DescriptorFor(kind)
	return ok && d.Caps.Multi != nil
}

func (Policy) MultiKinds() []string {
	var out []string
	for _, d := range app.AllDescriptors() {
		if d.Caps.Multi != nil {
			out = append(out, d.Kind)
		}
	}
	sort.Strings(out)
	return out
}

func (Policy) MultiStrategies(kind string) []string {
	d, ok := app.DescriptorFor(kind)
	if !ok || d.Caps.Multi == nil {
		return nil
	}
	strategies := d.Caps.Multi.Strategies()
	out := make([]string, 0, len(strategies))
	for _, strategy := range strategies {
		if strategy.ID != "" {
			out = append(out, strategy.ID)
		}
	}
	return out
}

func (Policy) AllowsDecoy(kind string) bool {
	d, ok := app.DescriptorFor(kind)
	return ok && d.SupportsDecoy()
}

func (Policy) Secrets() []capability.SecretSpec {
	specs := panelstore.BaseSecretSpecs()
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		seen[spec.Key] = struct{}{}
	}
	for _, d := range app.AllDescriptors() {
		if d.Caps.Secrets == nil {
			continue
		}
		for _, spec := range d.Caps.Secrets.SecretSpecs() {
			if spec.Key == "" {
				continue
			}
			if _, ok := seen[spec.Key]; ok {
				continue
			}
			seen[spec.Key] = struct{}{}
			specs = append(specs, spec)
		}
	}
	return specs
}

func (p Policy) EditableSecret(key string) bool {
	for _, spec := range p.Secrets() {
		if spec.Key == key {
			return spec.Editable
		}
	}
	return false
}
