package livekit

import (
	"encoding/json"
	"errors"
	"net/url"
)

func DecodeURL(q url.Values) (json.RawMessage, error) {
	signal := q.Get("signal_url")
	if signal == "" {
		return nil, errors.New("livekit: missing signal_url")
	}
	key := q.Get("api_key")
	if key == "" {
		return nil, errors.New("livekit: missing api_key")
	}
	secret := q.Get("api_secret")
	if secret == "" {
		return nil, errors.New("livekit: missing api_secret")
	}
	room := q.Get("room")
	if room == "" {
		return nil, errors.New("livekit: missing room")
	}

	out := map[string]any{
		"signal_url": signal,
		"api_key":    key,
		"api_secret": secret,
		"room":       room,
	}
	if topic := q.Get("data_topic"); topic != "" {
		out["data_topic"] = topic
	}
	if identity := q.Get("identity"); identity != "" {
		out["identity"] = identity
	}
	return json.Marshal(out)
}

func EncodeURL(raw json.RawMessage, q url.Values) error {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
	}
	if cfg.SignalURL != "" {
		q.Set("signal_url", cfg.SignalURL)
	}
	if cfg.APIKey != "" {
		q.Set("api_key", cfg.APIKey)
	}
	if cfg.APISecret != "" {
		q.Set("api_secret", cfg.APISecret)
	}
	if cfg.Room != "" {
		q.Set("room", cfg.Room)
	}
	if cfg.DataTopic != "" {
		q.Set("data_topic", cfg.DataTopic)
	}
	if cfg.Identity != "" {
		q.Set("identity", cfg.Identity)
	}
	return nil
}
