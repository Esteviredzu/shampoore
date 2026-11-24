package yandex

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

func decodeURL(q url.Values) (json.RawMessage, error) {
	hash := q.Get("hash")
	if hash == "" {
		return nil, errors.New("yandex: missing hash")
	}
	if !isValidHash(hash) {
		return nil, errors.New("yandex: invalid hash")
	}
	out := map[string]any{"hash": hash}
	if profile := strings.TrimSpace(q.Get("utls_profile")); profile != "" {
		out["utls_profile"] = profile
	}
	return json.Marshal(out)
}

func encodeURL(raw json.RawMessage, q url.Values) error {
	cfg := DefaultConfig()
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
	}
	if cfg.Hash != "" {
		q.Set("hash", cfg.Hash)
	}
	if cfg.UTLSProfile != "" {
		q.Set("utls_profile", cfg.UTLSProfile)
	}
	return nil
}
