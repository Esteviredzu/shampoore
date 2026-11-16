package raw_ws

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"
)

var defaults = app.Values{
	"mux_flush_ms":        10,
	"mux_flush_threshold": 12000,
}

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "raw_ws",
		DisplayName: "Raw WebSocket",
		Description: "Binary WebSocket tunnel through a relay server",
		Fields: []app.Field{
			{
				Key:         "server_url",
				Label:       "Server URL",
				Type:        app.FieldTypeText,
				Required:    true,
				Placeholder: "wss://draw.example.com",
			},
			{
				Key:         "room",
				Label:       "Room ID",
				Type:        app.FieldTypeText,
				Required:    true,
				Placeholder: "tunnel-1",
			},
			{
				Key:      "password",
				Label:    "Room Password",
				Type:     app.FieldTypePassword,
				Required: true,
			},
		},
		Defaults:  defaults,
		DecodeURL: decodeURL,
		EncodeURL: encodeURL,
		Build:     build,
	})
	if err != nil {
		panic(err)
	}
}

type Config struct {
	ServerURL string `json:"server_url"`
	Room      string `json:"room"`
	Password  string `json:"password"`
}

func decodeURL(q url.Values) (json.RawMessage, error) {
	serverURL := q.Get("server_url")
	room := q.Get("room")
	password := q.Get("password")
	if serverURL == "" || room == "" || password == "" {
		return nil, errors.New("raw_ws: server_url, room and password are required")
	}
	return json.Marshal(Config{ServerURL: serverURL, Room: room, Password: password})
}

func encodeURL(raw json.RawMessage, q url.Values) error {
	cfg := Config{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
	}
	if cfg.ServerURL != "" {
		q.Set("server_url", cfg.ServerURL)
	}
	if cfg.Room != "" {
		q.Set("room", cfg.Room)
	}
	if cfg.Password != "" {
		q.Set("password", cfg.Password)
	}
	return nil
}

func build(
	ctx context.Context,
	s *app.Settings,
	role string,
	logger logging.ContextLogger,
	eff app.Values,
) (*app.BuildResult, error) {
	var cfg Config
	if data := s.TransportData(); len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}

	if cfg.ServerURL == "" || cfg.Room == "" || cfg.Password == "" {
		return nil, errors.New("raw_ws: server_url, room and password are required")
	}

	wsURL, err := buildWSURL(cfg.ServerURL, cfg.Room)
	if err != nil {
		return nil, err
	}

	t := newTransport(cfg, logger)
	logger.Info("raw_ws: ready (url=", wsURL, " max_msg=", t.MaxMessageSize(), ")")

	bypass := []string{}
	if u, err := url.Parse(cfg.ServerURL); err == nil && u.Host != "" {
		bypass = append(bypass, u.Host)
	}

	return &app.BuildResult{
		Transport:   t,
		BypassHosts: bypass,
	}, nil
}
