package app

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const (
	URLScheme      = "shampoore"
	advancedPrefix = "a."
)

func ParseSource(src string) (*Settings, error) {
	src = strings.TrimSpace(src)
	if src == "" {
		return nil, fmt.Errorf("app: empty source")
	}
	if strings.HasPrefix(src, URLScheme+"://") {
		return ParseURL(src)
	}
	if strings.HasPrefix(src, "{") {
		var s Settings
		if err := json.Unmarshal([]byte(src), &s); err != nil {
			return nil, fmt.Errorf("app: parse json: %w", err)
		}
		if err := s.ApplyDefaults(); err != nil {
			return nil, err
		}
		s.IPv4Only = IPv4Only
		return &s, nil
	}
	return Load(src)
}

func ParseURL(raw string) (*Settings, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("app: parse url: %w", err)
	}
	if u.Scheme != URLScheme {
		return nil, fmt.Errorf("app: unexpected scheme %q", u.Scheme)
	}

	q := u.Query()

	kind := q.Get("type")
	if kind == "" {
		kind = u.Host
	}
	if kind == "" {
		return nil, fmt.Errorf("app: shampoore url missing type")
	}

	d, ok := DescriptorFor(kind)
	if !ok {
		return nil, fmt.Errorf("app: unknown transport type %q", kind)
	}
	if d.DecodeURL == nil {
		return nil, fmt.Errorf("app: transport %q does not support URL parsing", kind)
	}

	s := defaultSettings()
	if v := q.Get("pw"); v != "" {
		s.EncryptionPassword = v
	}
	if v := q.Get("user_id"); v != "" {
		s.UserID = v
	}
	if v := q.Get("exit_node_id"); v != "" {
		s.ExitNodeID = v
	}
	if u.Fragment != "" {
		s.Name = u.Fragment
	}

	for key, vals := range q {
		if !strings.HasPrefix(key, advancedPrefix) || len(vals) == 0 {
			continue
		}
		realKey := strings.TrimPrefix(key, advancedPrefix)
		if realKey == "" {
			continue
		}
		s.Advanced[realKey] = vals[0]
	}

	data, err := d.DecodeURL(q)
	if err != nil {
		return nil, fmt.Errorf("app: decode transport %q: %w", kind, err)
	}
	s.SetTransportData(kind, data)
	if err := s.Advanced.ValidateAgainstDefaults(d.Defaults); err != nil {
		return nil, fmt.Errorf("app: advanced: %w", err)
	}
	if err := s.ApplyDefaults(); err != nil {
		return nil, err
	}

	return s, nil
}

func EncodeURL(s *Settings) (string, error) {
	kind := s.Type
	if kind == "" {
		return "", fmt.Errorf("app: transport type not set")
	}

	d, ok := DescriptorFor(kind)
	if !ok {
		return "", fmt.Errorf("app: unknown transport type %q", kind)
	}
	if d.EncodeURL == nil {
		return "", fmt.Errorf("app: transport %q does not support URL encoding", kind)
	}

	q := url.Values{}
	q.Set("type", kind)
	if s.EncryptionPassword != "" {
		q.Set("pw", s.EncryptionPassword)
	}
	if s.UserID != "" {
		q.Set("user_id", s.UserID)
	}
	if s.ExitNodeID != "" {
		q.Set("exit_node_id", s.ExitNodeID)
	}

	if err := d.EncodeURL(s.Transport, q); err != nil {
		return "", fmt.Errorf("app: encode transport %q: %w", kind, err)
	}

	for k, v := range s.Advanced {
		if k == "" || v == nil {
			continue
		}
		q.Set(advancedPrefix+k, EncodeValue(v))
	}

	out := URLScheme + "://?" + q.Encode()
	if s.Name != "" {
		out += "#" + url.PathEscape(s.Name)
	}
	return out, nil
}

func EncodeValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case []string:
		return strings.Join(x, ",")
	case []any:
		parts := make([]string, 0, len(x))
		for _, item := range x {
			parts = append(parts, EncodeValue(item))
		}
		return strings.Join(parts, ",")
	default:
		return fmt.Sprint(v)
	}
}
