package livekit

import (
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
)

func DecodeURL(q url.Values) (json.RawMessage, error) {
	signal := q.Get("signal_url")
	key := q.Get("api_key")
	secret := q.Get("api_secret")
	room := q.Get("room")
	multiuser, _ := strconv.ParseBool(q.Get("multiuser"))
	token := q.Get("token")
	if signal == "" {
		return nil, errors.New("livekit: missing signal_url")
	}
	if key == "" || secret == "" {
		if !multiuser && token == "" {
			return nil, errors.New("livekit: missing api_key or api_secret")
		}
	}
	if room == "" && !multiuser && token == "" {
		return nil, errors.New("livekit: missing room")
	}

	out := map[string]any{
		"signal_url": signal,
	}
	if key != "" {
		out["api_key"] = key
	}
	if secret != "" {
		out["api_secret"] = secret
	}
	if room != "" {
		out["room"] = room
	}
	if topic := q.Get("data_topic"); topic != "" {
		out["data_topic"] = topic
	}
	if identity := q.Get("identity"); identity != "" {
		out["identity"] = identity
	}
	if userID := q.Get("user_id"); userID != "" {
		out["user_id"] = userID
	}
	if exitID := q.Get("exit_node_id"); exitID != "" {
		out["exit_node_id"] = exitID
	}
	if multiuser {
		out["multiuser"] = true
	}
	if prefix := q.Get("room_prefix"); prefix != "" {
		out["room_prefix"] = prefix
	}
	if profile := strings.TrimSpace(q.Get("utls_profile")); profile != "" {
		out["utls_profile"] = profile
	}
	if token != "" {
		out["token"] = token
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
	if cfg.UserID != "" {
		q.Set("user_id", cfg.UserID)
	}
	if cfg.ExitNodeID != "" {
		q.Set("exit_node_id", cfg.ExitNodeID)
	}
	if cfg.Token != "" {
		q.Set("token", cfg.Token)
	}
	if cfg.Multiuser {
		q.Set("multiuser", "1")
	}
	if cfg.RoomPrefix != "" {
		q.Set("room_prefix", cfg.RoomPrefix)
	}
	if cfg.UTLSProfile != "" {
		q.Set("utls_profile", cfg.UTLSProfile)
	}
	return nil
}
