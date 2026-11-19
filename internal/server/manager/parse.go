package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/app"
)

func ParseConfigContent(content string) (*app.Settings, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil, fmt.Errorf("empty config")
	}

	if strings.HasPrefix(content, app.URLScheme+"://") {
		s, err := app.ParseURL(content)
		if err != nil {
			return nil, fmt.Errorf("shampoore url: %w", err)
		}
		return s, nil
	}

	var probe map[string]json.RawMessage
	if err := json.Unmarshal([]byte(content), &probe); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	if raw, ok := probe["settings"]; ok {
		var s app.Settings
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("invalid settings: %w", err)
		}
		if nameRaw, ok := probe["name"]; ok {
			var n string
			if err := json.Unmarshal(nameRaw, &n); err != nil {
				return nil, fmt.Errorf(`invalid top-level "name": %w`, err)
			}
			if s.Name == "" && n != "" {
				s.Name = n
			}
		}
		if err := s.ApplyDefaults(); err != nil {
			return nil, err
		}
		s.IPv4Only = app.IPv4Only
		return &s, nil
	}

	var s app.Settings
	if err := json.Unmarshal([]byte(content), &s); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if err := s.ApplyDefaults(); err != nil {
		return nil, err
	}
	s.IPv4Only = app.IPv4Only
	return &s, nil
}

func ValidateSettings(s *app.Settings) error {
	if s == nil {
		return fmt.Errorf("settings is required")
	}
	kind := strings.TrimSpace(s.Type)
	if kind == "" {
		return fmt.Errorf("settings.type is required")
	}
	d, ok := app.DescriptorFor(kind)
	if !ok {
		return fmt.Errorf("unknown transport type %q", kind)
	}
	if d.Validate != nil {
		if err := d.Validate(s.Transport); err != nil {
			return fmt.Errorf("transport %q: %w", kind, err)
		}
		return nil
	}
	return validateRequiredFields(kind, s.Transport, d)
}

func validateRequiredFields(kind string, raw json.RawMessage, d app.Descriptor) error {
	if len(d.Fields) == 0 {
		return nil
	}
	if !isJSONObject(raw) {
		return fmt.Errorf("transport %q: settings.transport must be a JSON object", kind)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return fmt.Errorf("transport %q: settings.transport: %w", kind, err)
	}
	for _, f := range d.Fields {
		if !f.Required {
			continue
		}
		value, ok := fields[f.Key]
		if !ok {
			return fmt.Errorf("transport %q: field %q is required", kind, f.Key)
		}
		if err := validateFieldValue(f, value); err != nil {
			return fmt.Errorf("transport %q: field %q: %w", kind, f.Key, err)
		}
	}
	return nil
}

func validateFieldValue(f app.Field, raw json.RawMessage) error {
	value := bytes.TrimSpace(raw)
	if len(value) == 0 || bytes.Equal(value, []byte("null")) {
		return fmt.Errorf("must not be null")
	}
	switch f.Type {
	case app.FieldTypeText, app.FieldTypePassword:
		var s string
		if err := json.Unmarshal(value, &s); err != nil {
			return fmt.Errorf("must be a string")
		}
		if strings.TrimSpace(s) == "" {
			return fmt.Errorf("must not be empty")
		}
	case app.FieldTypeNumber:
		var n float64
		if err := json.Unmarshal(value, &n); err != nil {
			return fmt.Errorf("must be a number")
		}
	case app.FieldTypeBool:
		var b bool
		if err := json.Unmarshal(value, &b); err != nil {
			return fmt.Errorf("must be a boolean")
		}
	}
	return nil
}

func isJSONObject(raw []byte) bool {
	value := bytes.TrimSpace(raw)
	return len(value) > 0 && value[0] == '{'
}
