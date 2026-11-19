package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Mode string

const (
	ModeSOCKS5 Mode = "socks5"
	ModeTUN    Mode = "tun"
)

type Settings struct {
	Name string `json:"name,omitempty"`

	UserID     string `json:"user_id,omitempty"`
	ExitNodeID string `json:"exit_node_id,omitempty"`

	Type      string          `json:"type,omitempty"`
	Transport json.RawMessage `json:"transport,omitempty"`

	EncryptionPassword string `json:"encryption_password,omitempty"`

	Advanced Values `json:"advanced,omitempty"`

	IPv4Only bool `json:"-"`
}

func (s *Settings) TransportKind() string          { return s.Type }
func (s *Settings) TransportData() json.RawMessage { return s.Transport }

func (s *Settings) SetTransportData(kind string, data json.RawMessage) {
	s.Type = kind
	s.Transport = data
}

func (s *Settings) EffectiveAdvanced() Values {
	return defaultAdvanced().Merge(s.Advanced)
}

var settingsFields = map[string]struct{}{
	"name":                {},
	"user_id":             {},
	"exit_node_id":        {},
	"type":                {},
	"transport":           {},
	"encryption_password": {},
	"advanced":            {},
	"transport_kind":      {},

	"enabled": {},
}

func (s *Settings) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("app: settings must be a JSON object")
	}
	if err := checkDuplicateKeys(trimmed); err != nil {
		return err
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &raw); err != nil {
		return fmt.Errorf("app: settings must be a JSON object: %w", err)
	}

	for _, old := range []string{"mode", "socks5", "tun", "platform"} {
		if _, ok := raw[old]; ok {
			return fmt.Errorf(
				"app: field %q is no longer supported in config; "+
					"use \"advanced\" (e.g. tun_mtu, socks5_listen) "+
					"and CLI flag -m for mode", old)
		}
	}

	var legacyKind string
	if v, ok := raw["transport_kind"]; ok {
		if err := decodeStrictField(v, &legacyKind); err != nil {
			return fieldError("transport_kind", err)
		}
		if strings.TrimSpace(legacyKind) == "" {
			return fmt.Errorf(`app: field "transport_kind" must not be empty`)
		}
	}

	var declaredType string
	if v, ok := raw["type"]; ok {
		if err := decodeStrictField(v, &declaredType); err != nil {
			return fieldError("type", err)
		}
		if strings.TrimSpace(declaredType) == "" {
			return fmt.Errorf(`app: field "type" must not be empty`)
		}
	}
	if declaredType != "" && legacyKind != "" && declaredType != legacyKind {
		return fmt.Errorf("app: conflicting settings types %q and %q", declaredType, legacyKind)
	}

	for key := range raw {
		if _, ok := settingsFields[key]; ok {
			continue
		}
		if _, ok := DescriptorFor(key); ok {
			continue
		}
		return fmt.Errorf("app: unknown field %q in settings", key)
	}

	expectedKind := declaredType
	if expectedKind == "" {
		expectedKind = legacyKind
	}
	if expectedKind != "" {
		for key := range raw {
			if _, ok := settingsFields[key]; ok {
				continue
			}
			if key != expectedKind {
				return fmt.Errorf("app: transport block %q does not match settings type %q", key, expectedKind)
			}
		}
	}

	type plain Settings
	tmp := plain(*s)

	if v, ok := raw["name"]; ok {
		if err := decodeStrictField(v, &tmp.Name); err != nil {
			return fieldError("name", err)
		}
	}
	if v, ok := raw["user_id"]; ok {
		if err := decodeStrictField(v, &tmp.UserID); err != nil {
			return fieldError("user_id", err)
		}
	}
	if v, ok := raw["exit_node_id"]; ok {
		if err := decodeStrictField(v, &tmp.ExitNodeID); err != nil {
			return fieldError("exit_node_id", err)
		}
	}
	if v, ok := raw["encryption_password"]; ok {
		if err := decodeStrictField(v, &tmp.EncryptionPassword); err != nil {
			return fieldError("encryption_password", err)
		}
	}
	if v, ok := raw["enabled"]; ok {
		var enabled bool
		if err := decodeStrictField(v, &enabled); err != nil {
			return fieldError("enabled", err)
		}
	}
	if v, ok := raw["advanced"]; ok {
		if !isJSONObject(v) {
			return fmt.Errorf(`app: field "advanced" must be a JSON object`)
		}
		var adv Values
		if err := json.Unmarshal(v, &adv); err != nil {
			return fieldError("advanced", err)
		}
		if err := adv.ValidateTypes(); err != nil {
			return fmt.Errorf("app: settings.advanced: %w", err)
		}
		tmp.Advanced = adv
	}

	tmp.Type = declaredType
	if tmp.Type == "" {
		tmp.Type = legacyKind
	}

	if v, ok := raw["transport"]; ok {
		if !isJSONObject(v) {
			return fmt.Errorf(`app: field "transport" must be a JSON object`)
		}
		tmp.Transport = append(json.RawMessage(nil), v...)
	}
	if tmp.Type != "" && tmp.Transport == nil {
		if v, ok := raw[tmp.Type]; ok {
			if !isJSONObject(v) {
				return fmt.Errorf("app: transport block %q must be a JSON object", tmp.Type)
			}
			tmp.Transport = append(json.RawMessage(nil), v...)
		}
	}

	if tmp.Type != "" {
		if d, ok := DescriptorFor(tmp.Type); ok {
			if err := tmp.Advanced.ValidateAgainstDefaults(d.Defaults); err != nil {
				return fmt.Errorf("app: settings.advanced: %w", err)
			}
		}
	}

	*s = Settings(tmp)
	return nil
}

func (s *Settings) ApplyDefaults() error {
	if strings.TrimSpace(s.Type) == "" {
		return fmt.Errorf(`app: settings "type" is required`)
	}
	if s.Advanced == nil {
		s.Advanced = NewValues()
	}
	return nil
}

func Load(path string) (*Settings, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	s := &Settings{IPv4Only: IPv4Only}
	if err := json.Unmarshal(data, s); err != nil {
		return nil, err
	}
	if err := s.ApplyDefaults(); err != nil {
		return nil, err
	}
	return s, nil
}

func decodeStrictField(raw json.RawMessage, dst any) error {
	value := bytes.TrimSpace(raw)
	if len(value) == 0 || bytes.Equal(value, []byte("null")) {
		return fmt.Errorf("must not be null")
	}
	if err := json.Unmarshal(value, dst); err != nil {
		return err
	}
	return nil
}

func fieldError(field string, err error) error {
	return fmt.Errorf("app: field %q: %w", field, err)
}

func isJSONObject(raw []byte) bool {
	value := bytes.TrimSpace(raw)
	return len(value) > 0 && value[0] == '{'
}

func checkDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("app: settings: %w", err)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return nil
	}
	seen := make(map[string]struct{})
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("app: settings: %w", err)
		}
		key, ok := tok.(string)
		if !ok {
			return fmt.Errorf("app: settings: invalid object key")
		}
		if _, dup := seen[key]; dup {
			return fmt.Errorf("app: duplicate field %q in settings", key)
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return fmt.Errorf("app: settings: %w", err)
		}
	}
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("app: settings: %w", err)
	}
	return nil
}
