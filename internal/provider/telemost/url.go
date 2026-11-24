package telemost

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

func DecodeURL(q url.Values) (json.RawMessage, error) {
	raw := q.Get("conference_link")
	if raw == "" {
		return nil, errors.New("telemost: missing conference_link")
	}
	id := extractConferenceID(raw)
	if id == "" {
		return nil, errors.New("telemost: invalid conference_link")
	}
	out := map[string]any{
		"conference_link": "https://telemost.yandex.ru/j/" + id,
	}
	if profile := strings.TrimSpace(q.Get("utls_profile")); profile != "" {
		out["utls_profile"] = profile
	}
	return json.Marshal(out)
}

func EncodeURL(raw json.RawMessage, q url.Values) error {
	cfg := DefaultConfig()
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
	}
	if id := extractConferenceID(cfg.EffectiveConferenceLink()); id != "" {
		q.Set("conference_link", id)
	}
	if cfg.UTLSProfile != "" {
		q.Set("utls_profile", cfg.UTLSProfile)
	}
	return nil
}
