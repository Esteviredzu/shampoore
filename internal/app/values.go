package app

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type Values map[string]any

func NewValues() Values {
	return Values{}
}

func (v Values) Clone() Values {
	out := make(Values, len(v))
	for k, val := range v {
		out[k] = val
	}
	return out
}

func (v Values) Merge(override Values) Values {
	out := v.Clone()
	for k, val := range override {
		out[k] = val
	}
	return out
}

func (v Values) Int(key string, def int) int {
	n, ok, err := v.IntStrict(key)
	if err != nil || !ok {
		return def
	}
	return n
}

func (v Values) IntStrict(key string) (int, bool, error) {
	raw, ok := v[key]
	if !ok || raw == nil {
		return 0, false, nil
	}
	switch n := raw.(type) {
	case int:
		return n, true, nil
	case int64:
		return int(n), true, nil
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) || n != math.Trunc(n) {
			return 0, true, fmt.Errorf("value %q: expected integer, got %v", key, n)
		}
		return int(n), true, nil
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, true, fmt.Errorf("value %q: expected integer: %w", key, err)
		}
		return int(i), true, nil
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(n))
		if err != nil {
			return 0, true, fmt.Errorf("value %q: expected integer, got %q", key, n)
		}
		return i, true, nil
	default:
		return 0, true, fmt.Errorf("value %q: expected integer, got %T", key, raw)
	}
}

func (v Values) Bool(key string, def bool) bool {
	b, ok, err := v.BoolStrict(key)
	if err != nil || !ok {
		return def
	}
	return b
}

func (v Values) BoolStrict(key string) (bool, bool, error) {
	raw, ok := v[key]
	if !ok || raw == nil {
		return false, false, nil
	}
	switch b := raw.(type) {
	case bool:
		return b, true, nil
	case string:
		switch strings.ToLower(strings.TrimSpace(b)) {
		case "true", "1", "yes", "on":
			return true, true, nil
		case "false", "0", "no", "off":
			return false, true, nil
		default:
			return false, true, fmt.Errorf("value %q: expected boolean, got %q", key, b)
		}
	default:
		return false, true, fmt.Errorf("value %q: expected boolean, got %T", key, raw)
	}
}

func (v Values) String(key string, def string) string {
	s, ok, err := v.StringStrict(key)
	if err != nil || !ok {
		return def
	}
	return s
}

func (v Values) StringStrict(key string) (string, bool, error) {
	raw, ok := v[key]
	if !ok || raw == nil {
		return "", false, nil
	}
	s, ok := raw.(string)
	if !ok {
		return "", true, fmt.Errorf("value %q: expected string, got %T", key, raw)
	}
	return s, true, nil
}

func (v Values) StringSlice(key string, def []string) []string {
	raw, ok := v[key]
	if !ok || raw == nil {
		return def
	}
	switch s := raw.(type) {
	case []string:
		return s
	case []any:
		out := make([]string, 0, len(s))
		for _, item := range s {
			if str, ok := item.(string); ok {
				out = append(out, str)
			}
		}
		return out
	case string:
		if s == "" {
			return def
		}
		parts := strings.Split(s, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	default:
		return def
	}
}

func (v Values) ValidateTypes() error {
	for key, raw := range v {
		if key == "" {
			return fmt.Errorf("empty key is not allowed")
		}
		if raw == nil {
			return fmt.Errorf("value %q: null is not allowed", key)
		}
		switch x := raw.(type) {
		case string, bool, int, int64, float64, json.Number:
		case []string:
		case []any:
			for i, item := range x {
				if _, ok := item.(string); !ok {
					return fmt.Errorf("value %q[%d]: expected string, got %T", key, i, item)
				}
			}
		default:
			return fmt.Errorf("value %q: unsupported type %T", key, raw)
		}
	}
	return nil
}

func (v Values) ValidateAgainstDefaults(defaults Values) error {
	for key := range v {
		def, ok := defaults[key]
		if !ok || def == nil {
			continue
		}
		switch def.(type) {
		case int, int64, float64, json.Number:
			if _, _, err := v.IntStrict(key); err != nil {
				return err
			}
		case bool:
			if _, _, err := v.BoolStrict(key); err != nil {
				return err
			}
		case string:
			if _, _, err := v.StringStrict(key); err != nil {
				return err
			}
		}
	}
	return nil
}
