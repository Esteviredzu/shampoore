package telemost

import (
	"encoding/json"
	"errors"
	"net/url"
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
	return nil
}
