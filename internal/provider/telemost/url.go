package telemost

import (
	"encoding/json"
	"errors"
	"net/url"
)

func DecodeURL(q url.Values) (json.RawMessage, error) {
	conf := q.Get("conf")
	if conf == "" {
		return nil, errors.New("telemost: missing conf")
	}
	id := extractConferenceID(conf)
	if id == "" {
		return nil, errors.New("telemost: invalid conf")
	}
	out := map[string]any{"conference_id": id}
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
		q.Set("conf", id)
	}
	return nil
}