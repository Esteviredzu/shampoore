package livekit

import (
	"encoding/json"
	"errors"
	"net/url"
)

// DecodeURL читает плоские параметры:
//
//	signal=wss://...   (обязательно)
//	key=...            (обязательно)
//	secret=...         (обязательно)
//	room=...           (обязательно)
//	topic=...          (опционально, default "shampoore")
//	identity=...       (опционально)
func DecodeURL(q url.Values) (json.RawMessage, error) {
	signal := q.Get("signal")
	if signal == "" {
		return nil, errors.New("livekit: missing signal")
	}
	key := q.Get("key")
	if key == "" {
		return nil, errors.New("livekit: missing key")
	}
	secret := q.Get("secret")
	if secret == "" {
		return nil, errors.New("livekit: missing secret")
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
	if topic := q.Get("topic"); topic != "" {
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
		q.Set("signal", cfg.SignalURL)
	}
	if cfg.APIKey != "" {
		q.Set("key", cfg.APIKey)
	}
	if cfg.APISecret != "" {
		q.Set("secret", cfg.APISecret)
	}
	if cfg.Room != "" {
		q.Set("room", cfg.Room)
	}
	if cfg.DataTopic != "" {
		q.Set("topic", cfg.DataTopic)
	}
	if cfg.Identity != "" {
		q.Set("identity", cfg.Identity)
	}
	return nil
}