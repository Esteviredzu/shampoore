package raw_ws

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/netdial"
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
			{
				Key:      "batch",
				Label:    "Batching",
				Type:     app.FieldTypeBool,
				Required: false,
				Hint:     "Combine small messages into larger frames",
			},
			{
				Key:      "padding",
				Label:    "Padding",
				Type:     app.FieldTypeBool,
				Required: false,
				Hint:     "Add random padding to hide message sizes",
			},
			{
				Key:      "allow_insecure_ws",
				Label:    "Allow insecure ws://",
				Type:     app.FieldTypeBool,
				Required: false,
				Hint:     "Allow plaintext ws:// URLs (encryption stays optional)",
			},
		},
		Defaults:  defaults,
		DecodeURL: decodeURL,
		EncodeURL: encodeURL,
		Validate:  validate,
		Build:     build,
	})
	if err != nil {
		panic(err)
	}
}

type Config struct {
	ServerURL       string `json:"server_url"`
	Room            string `json:"room"`
	Password        string `json:"password"`
	Batch           bool   `json:"batch,omitempty"`
	Padding         bool   `json:"padding,omitempty"`
	AllowInsecureWS bool   `json:"allow_insecure_ws,omitempty"`
}

func decodeURL(q url.Values) (json.RawMessage, error) {
	serverURL := q.Get("server_url")
	room := q.Get("room")
	password := q.Get("password")
	if serverURL == "" || room == "" || password == "" {
		return nil, errors.New("raw_ws: server_url, room and password are required")
	}
	batch, _ := strconv.ParseBool(q.Get("batch"))
	padding, _ := strconv.ParseBool(q.Get("padding"))
	allowInsecure, _ := strconv.ParseBool(q.Get("allow_insecure_ws"))
	return json.Marshal(Config{
		ServerURL:       serverURL,
		Room:            room,
		Password:        password,
		Batch:           batch,
		Padding:         padding,
		AllowInsecureWS: allowInsecure,
	})
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
	if cfg.Batch {
		q.Set("batch", "1")
	}
	if cfg.Padding {
		q.Set("padding", "1")
	}
	if cfg.AllowInsecureWS {
		q.Set("allow_insecure_ws", "1")
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
	if err := validateSecureServerURL(cfg.ServerURL, cfg.AllowInsecureWS); err != nil {
		return nil, err
	}

	wsURL, err := buildWSURL(cfg.ServerURL, cfg.Room)
	if err != nil {
		return nil, err
	}

	t := newTransport(cfg, logger)
	logger.Info("raw_ws: ready (url=", wsURL, " max_msg=", t.MaxMessageSize(),
		" batch=", cfg.Batch, " padding=", cfg.Padding, ")")

	bypass := []string{}
	if u, err := url.Parse(cfg.ServerURL); err == nil && u.Host != "" {
		bypass = append(bypass, u.Hostname())
		bypass = append(bypass, netdial.ResolveIPv4(ctx, u.Hostname())...)
	}

	return &app.BuildResult{
		Transport:   t,
		BypassHosts: bypass,
	}, nil
}

func validate(raw json.RawMessage) error {
	cfg := Config{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
	}
	if strings.TrimSpace(cfg.ServerURL) == "" || strings.TrimSpace(cfg.Room) == "" || strings.TrimSpace(cfg.Password) == "" {
		return errors.New("raw_ws: server_url, room and password are required")
	}
	return validateSecureServerURL(cfg.ServerURL, cfg.AllowInsecureWS)
}
