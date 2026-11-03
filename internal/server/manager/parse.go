package manager

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Esteviredzu/shampoore-core/internal/app"
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
		if nameRaw, ok := probe["name"]; ok && s.Name == "" {
			var n string
			if json.Unmarshal(nameRaw, &n) == nil && n != "" {
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
	if s.Type == "" {
		return fmt.Errorf("settings.type is required")
	}
	if _, ok := app.DescriptorFor(s.Type); !ok {
		return fmt.Errorf("unknown transport type %q", s.Type)
	}
	return nil
}
