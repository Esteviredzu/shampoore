package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/Esteviredzu/shampoore/internal/logging"
)

type FieldType string

const (
	FieldTypeText     FieldType = "text"
	FieldTypePassword FieldType = "password"
	FieldTypeNumber   FieldType = "number"
	FieldTypeBool     FieldType = "bool"
)

type Field struct {
	Key         string    `json:"key"`
	Label       string    `json:"label"`
	Placeholder string    `json:"placeholder,omitempty"`
	Hint        string    `json:"hint,omitempty"`
	Type        FieldType `json:"type"`
	Required    bool      `json:"required,omitempty"`
}

type Builder func(
	ctx context.Context,
	s *Settings,
	role string,
	logger logging.ContextLogger,
	cfg Values,
) (*BuildResult, error)

type RouteParams struct {
	RouteID    int64
	RouteName  string
	UserID     string
	ExitNodeID string

	Domain string

	Secrets map[string]string

	OnDemand bool

	TokenURL    string
	TokenSecret string
}

func (p RouteParams) Secret(key string) string {
	if p.Secrets == nil {
		return ""
	}
	return p.Secrets[key]
}

func MarshalTransportOptions(cfg any, forceEmptyUsers bool) (json.RawMessage, error) {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	if !forceEmptyUsers {
		return raw, nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	obj["users"] = []any{}
	return json.Marshal(obj)
}

type RouteConfigBuilder func(RouteParams) (Settings, error)

type Descriptor struct {
	Kind        string
	DisplayName string
	Description string

	Fields []Field

	DecodeURL func(q url.Values) (json.RawMessage, error)
	EncodeURL func(raw json.RawMessage, q url.Values) error

	Validate func(raw json.RawMessage) error

	Defaults Values

	Build Builder

	Aliases []string

	BuildSubscriptionConfig RouteConfigBuilder

	BuildServerConfig RouteConfigBuilder

	RelayUpstream string

	RelaySecrets []string
}

var (
	descriptorsMu sync.RWMutex
	descriptors   = map[string]Descriptor{}
	descriptorIDs = map[string]string{}
)

func RegisterDescriptor(d Descriptor) error {
	if d.Kind == "" {
		return errors.New("app: descriptor kind is empty")
	}
	if d.Build == nil {
		return fmt.Errorf("app: descriptor %q: Build is nil", d.Kind)
	}
	if d.Defaults == nil {
		d.Defaults = NewValues()
	}

	descriptorsMu.Lock()
	defer descriptorsMu.Unlock()

	if _, exists := descriptors[d.Kind]; exists {
		return fmt.Errorf("app: descriptor %q already registered", d.Kind)
	}
	if owner, exists := descriptorIDs[d.Kind]; exists && owner != d.Kind {
		return fmt.Errorf("app: descriptor id %q is already an alias of %q", d.Kind, owner)
	}
	aliases := make([]string, 0, len(d.Aliases))
	for _, raw := range d.Aliases {
		alias := strings.TrimSpace(raw)
		if alias == "" || alias == d.Kind {
			continue
		}
		if _, exists := descriptors[alias]; exists {
			return fmt.Errorf("app: descriptor alias %q conflicts with a kind", alias)
		}
		if owner, exists := descriptorIDs[alias]; exists && owner != d.Kind {
			return fmt.Errorf("app: descriptor alias %q already registered by %q", alias, owner)
		}
		aliases = append(aliases, alias)
	}
	for _, alias := range aliases {
		descriptorIDs[alias] = d.Kind
	}
	descriptors[d.Kind] = d
	return nil
}

func DescriptorFor(kind string) (Descriptor, bool) {
	descriptorsMu.RLock()
	defer descriptorsMu.RUnlock()
	if d, ok := descriptors[kind]; ok {
		return d, true
	}
	if id, ok := descriptorIDs[kind]; ok {
		if d, ok := descriptors[id]; ok {
			return d, true
		}
	}
	return Descriptor{}, false
}

func AllDescriptors() []Descriptor {
	descriptorsMu.RLock()
	defer descriptorsMu.RUnlock()

	out := make([]Descriptor, 0, len(descriptors))
	for _, d := range descriptors {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Kind < out[j].Kind
	})
	return out
}
