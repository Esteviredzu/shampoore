package raw_ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/capability"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/net/utlsdial"
	"github.com/Esteviredzu/shampoore/internal/netdial"
	"github.com/Esteviredzu/shampoore/internal/wsshape"
)

const defaultMaxRateKBps = 20480

var defaults = app.Values{
	"mux_flush_ms":              10,
	"mux_flush_threshold":       12000,
	"shape_batch_window_ms":     5,
	"shape_batch_jitter_ms":     10,
	"shape_batch_max_bytes":     32768,
	"shape_pad_min_total_bytes": 64,
	"shape_pad_max_extra_bytes": 320,
	"shape_ping_interval_ms":    5000,
	"shape_ping_timeout_ms":     5000,
	"shape_read_idle_ms":        15000,
	"shape_max_rate_kbps":       0,
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
			{
				Key:         "utls_profile",
				Label:       "uTLS Profile",
				Type:        app.FieldTypeText,
				Required:    false,
				Advanced:    true,
				Options:     utlsdial.SupportedProfiles(),
				Placeholder: "chrome",
				Hint:        "chrome (default), random, firefox, safari, ios, edge",
			},
			{
				Key:      "user_id",
				Label:    "User ID",
				Type:     app.FieldTypeText,
				Required: false,
				Hint:     "Multiuser mode: joins the room <room>-<user_id>",
			},
			{
				Key:      "multiuser",
				Label:    "Multiuser server",
				Type:     app.FieldTypeBool,
				Required: false,
				Hint:     "Server only: run one isolated room session per active user",
			},
			{
				Key:      "shape_batch_window_ms",
				Label:    "Batch window (ms)",
				Type:     app.FieldTypeNumber,
				Required: false,
				Advanced: true,
				Hint:     "Flush window for batched frames (default 5 ms, mirrors ws-shape.json batch.window_ms)",
			},
			{
				Key:      "shape_batch_jitter_ms",
				Label:    "Batch jitter (ms)",
				Type:     app.FieldTypeNumber,
				Required: false,
				Advanced: true,
				Hint:     "Random extra delay added to the batch window (default 10 ms)",
			},
			{
				Key:      "shape_batch_max_bytes",
				Label:    "Batch max bytes",
				Type:     app.FieldTypeNumber,
				Required: false,
				Advanced: true,
				Hint:     "Flush the batch once this many payload bytes are queued (default 32768)",
			},
			{
				Key:      "shape_pad_min_total_bytes",
				Label:    "Padding target (bytes)",
				Type:     app.FieldTypeNumber,
				Required: false,
				Advanced: true,
				Hint:     "Small frames are padded up to this size plus jitter (default 64)",
			},
			{
				Key:      "shape_pad_max_extra_bytes",
				Label:    "Padding jitter (bytes)",
				Type:     app.FieldTypeNumber,
				Required: false,
				Advanced: true,
				Hint:     "Random padding range above the target (default 320)",
			},
			{
				Key:      "shape_ping_interval_ms",
				Label:    "Ping interval (ms)",
				Type:     app.FieldTypeNumber,
				Required: false,
				Advanced: true,
				Hint:     "WebSocket heartbeat interval (default 5000 ms, use the same value in ws-shape.json)",
			},
			{
				Key:      "shape_ping_timeout_ms",
				Label:    "Ping timeout (ms)",
				Type:     app.FieldTypeNumber,
				Required: false,
				Advanced: true,
				Hint:     "Control-frame write deadline for pings (default 5000 ms)",
			},
			{
				Key:      "shape_read_idle_ms",
				Label:    "Read idle (ms)",
				Type:     app.FieldTypeNumber,
				Required: false,
				Advanced: true,
				Hint:     "Close the socket when nothing (including pongs) arrives for this long (default 15000 ms)",
			},
			{
				Key:      "shape_max_rate_kbps",
				Label:    "Max rate (KB/s)",
				Type:     app.FieldTypeNumber,
				Required: false,
				Advanced: true,
				Hint:     "Uplink bandwidth ceiling; 0 disables it (default 0, new routes use 20480)",
			},
		},
		Defaults:     defaults,
		DefaultRelay: true,
		Caps: app.Capabilities{
			Relay:     relayProvider{},
			Multiuser: app.MultiuserConfig{},
			Secrets: app.SecretProviderFunc(func() []capability.SecretSpec {
				return []capability.SecretSpec{{
					Key:       "raw_ws_password",
					Label:     "Raw WS Password",
					Generator: capability.SecretGenerator{Kind: "hex", Bytes: 32},
					Editable:  true,
				}}
			}),
		},
		DecodeURL: decodeURL,
		EncodeURL: encodeURL,
		Validate:  validate,
		Build:     build,

		Aliases:                 []string{"raw_ws"},
		BuildSubscriptionConfig: buildSubscriptionConfig,
		BuildServerConfig:       buildServerConfig,
		RelayUpstream:           "relay:8787",
		RelaySecrets:            []string{"raw_ws_password"},
	})
	if err != nil {
		panic(err)
	}
}

type ShapeConfig = wsshape.Config

type Config struct {
	ServerURL       string          `json:"server_url"`
	Room            string          `json:"room"`
	Password        string          `json:"password"`
	Batch           bool            `json:"batch,omitempty"`
	Padding         bool            `json:"padding,omitempty"`
	AllowInsecureWS bool            `json:"allow_insecure_ws,omitempty"`
	UTLSProfile     string          `json:"utls_profile,omitempty"`
	UserID          string          `json:"user_id,omitempty"`
	Multiuser       bool            `json:"multiuser,omitempty"`
	Shape           ShapeConfig     `json:"shape,omitempty"`
	Users           []auth.UserSpec `json:"users,omitempty"`
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
	multiuser, _ := strconv.ParseBool(q.Get("multiuser"))
	return json.Marshal(Config{
		ServerURL:       serverURL,
		Room:            room,
		Password:        password,
		Batch:           batch,
		Padding:         padding,
		AllowInsecureWS: allowInsecure,
		UTLSProfile:     strings.TrimSpace(q.Get("utls_profile")),
		UserID:          q.Get("user_id"),
		Multiuser:       multiuser,
		Shape:           decodeShapeQuery(q),
	})
}

func decodeShapeQuery(q url.Values) wsshape.Config {
	var shape wsshape.Config
	readInt := func(key string, dst *int) {
		raw := strings.TrimSpace(q.Get(key))
		if raw == "" {
			return
		}
		if v, err := strconv.Atoi(raw); err == nil {
			*dst = v
		}
	}
	readInt("shape_batch_window_ms", &shape.Batch.WindowMs)
	readInt("shape_batch_jitter_ms", &shape.Batch.JitterMs)
	readInt("shape_batch_max_bytes", &shape.Batch.MaxBytes)
	readInt("shape_pad_min_total_bytes", &shape.Padding.MinTotalBytes)
	readInt("shape_pad_max_extra_bytes", &shape.Padding.MaxExtraBytes)
	readInt("shape_ping_interval_ms", &shape.Pulse.PingIntervalMs)
	readInt("shape_ping_timeout_ms", &shape.Pulse.PingTimeoutMs)
	readInt("shape_read_idle_ms", &shape.Pulse.ReadIdleMs)
	readInt("shape_max_rate_kbps", &shape.Rate.MaxRateKBps)
	if shape.Padding.MinTotalBytes > 0 || shape.Padding.MaxExtraBytes > 0 {
		shape.Padding.SetEnabled(true)
	}
	return shape
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
	if cfg.UTLSProfile != "" {
		q.Set("utls_profile", cfg.UTLSProfile)
	}
	if cfg.UserID != "" {
		q.Set("user_id", cfg.UserID)
	}
	if cfg.Multiuser {
		q.Set("multiuser", "1")
	}
	encodeShapeQuery(cfg.Shape, q)
	return nil
}

func encodeShapeQuery(shape wsshape.Config, q url.Values) {
	setInt := func(key string, v int) {
		if v > 0 {
			q.Set(key, strconv.Itoa(v))
		}
	}
	setInt("shape_batch_window_ms", shape.Batch.WindowMs)
	setInt("shape_batch_jitter_ms", shape.Batch.JitterMs)
	setInt("shape_batch_max_bytes", shape.Batch.MaxBytes)
	setInt("shape_pad_min_total_bytes", shape.Padding.MinTotalBytes)
	setInt("shape_pad_max_extra_bytes", shape.Padding.MaxExtraBytes)
	setInt("shape_ping_interval_ms", shape.Pulse.PingIntervalMs)
	setInt("shape_ping_timeout_ms", shape.Pulse.PingTimeoutMs)
	setInt("shape_read_idle_ms", shape.Pulse.ReadIdleMs)
	setInt("shape_max_rate_kbps", shape.Rate.MaxRateKBps)
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
	applyShapeOverrides(&cfg, s.Advanced)

	if cfg.ServerURL == "" || cfg.Room == "" || cfg.Password == "" {
		return nil, errors.New("raw_ws: server_url, room and password are required")
	}
	if err := validateSecureServerURL(cfg.ServerURL, cfg.AllowInsecureWS); err != nil {
		return nil, err
	}
	if cfg.UserID == "" {
		cfg.UserID = s.UserID
	}
	if cfg.UserID != "" {
		normalized, err := auth.NormalizeUserID(cfg.UserID)
		if err != nil {
			return nil, fmt.Errorf("raw_ws: %w", err)
		}
		cfg.UserID = normalized
	}

	wsURL, err := buildWSURL(cfg.ServerURL, roomForUser(cfg.Room, cfg.UserID))
	if err != nil {
		return nil, err
	}

	bypass := []string{}
	if u, err := url.Parse(cfg.ServerURL); err == nil && u.Host != "" {
		bypass = append(bypass, u.Hostname())
		bypass = append(bypass, netdial.ResolveIPv4(ctx, u.Hostname())...)
	}

	if role == "server" && cfg.Multiuser {
		registry, err := buildUserRegistry(cfg.Users)
		if err != nil {
			return nil, err
		}
		srv := newMultiServer(cfg, logger, registry, app.Values{
			"mux_flush_ms":        eff.Int("mux_flush_ms", 20),
			"mux_flush_threshold": eff.Int("mux_flush_threshold", 12000),
		})
		srv.SetExitNodeID(s.ExitNodeID)
		logger.Info("raw_ws: multiuser server ready (room=", cfg.Room,
			" users=", len(cfg.Users), ")")
		return &app.BuildResult{
			SessionServer: srv,
			UserRegistry:  registry,
			BypassHosts:   bypass,
		}, nil
	}

	t := newTransport(cfg, logger)
	logger.Info("raw_ws: ready (url=", wsURL, " max_msg=", t.MaxMessageSize(),
		" batch=", cfg.Batch, " padding=", cfg.Padding, " user=", cfg.UserID, ")")

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

func applyShapeOverrides(cfg *Config, adv app.Values) {
	shape := cfg.Shape
	if adv != nil {
		setInt := func(key string, dst *int) {
			if v, ok, err := adv.IntStrict(key); err == nil && ok {
				*dst = v
			}
		}
		setInt("shape_batch_window_ms", &shape.Batch.WindowMs)
		setInt("shape_batch_jitter_ms", &shape.Batch.JitterMs)
		setInt("shape_batch_max_bytes", &shape.Batch.MaxBytes)
		setInt("shape_pad_min_total_bytes", &shape.Padding.MinTotalBytes)
		setInt("shape_pad_max_extra_bytes", &shape.Padding.MaxExtraBytes)
		setInt("shape_ping_interval_ms", &shape.Pulse.PingIntervalMs)
		setInt("shape_ping_timeout_ms", &shape.Pulse.PingTimeoutMs)
		setInt("shape_read_idle_ms", &shape.Pulse.ReadIdleMs)
		setInt("shape_max_rate_kbps", &shape.Rate.MaxRateKBps)
		if shape.Padding.MinTotalBytes > 0 || shape.Padding.MaxExtraBytes > 0 {
			shape.Padding.SetEnabled(true)
		}
	}
	cfg.Shape = wsshape.Normalize(shape)
}

func defaultShape() ShapeConfig {
	return wsshape.Config{Rate: wsshape.Rate{MaxRateKBps: defaultMaxRateKBps}}
}

func routeRoom(p app.RouteParams) string {
	if p.RouteID > 0 {
		return "route-" + strconv.FormatInt(p.RouteID, 10)
	}
	if name := strings.TrimSpace(p.RouteName); name != "" {
		return name
	}
	return "route"
}

func buildSubscriptionConfig(p app.RouteParams) (app.Settings, error) {
	cfg := Config{
		ServerURL: app.NormalizeWSURL(p.Domain),
		Room:      routeRoom(p),
		Password:  p.Secret("raw_ws_password"),
		Batch:     true,
		Padding:   true,
		UserID:    p.UserID,
		Shape:     defaultShape(),
	}
	if strings.TrimSpace(cfg.Password) == "" {
		return app.Settings{}, errors.New("raw_ws: missing raw_ws_password")
	}
	if strings.HasPrefix(cfg.ServerURL, "ws://") {
		cfg.AllowInsecureWS = true
	}
	return rawWSSettings(cfg, p, false)
}

func buildServerConfig(p app.RouteParams) (app.Settings, error) {
	cfg := Config{
		ServerURL: app.NormalizeWSURL(p.Domain),
		Room:      routeRoom(p),
		Password:  p.Secret("raw_ws_password"),
		Batch:     true,
		Padding:   true,
		Multiuser: true,
		Users:     make([]auth.UserSpec, 0),
		Shape:     defaultShape(),
	}
	if strings.TrimSpace(cfg.Password) == "" {
		return app.Settings{}, errors.New("raw_ws: missing raw_ws_password")
	}
	if strings.HasPrefix(cfg.ServerURL, "ws://") {
		cfg.AllowInsecureWS = true
	}
	return rawWSSettings(cfg, p, true)
}

func rawWSSettings(cfg Config, p app.RouteParams, server bool) (app.Settings, error) {
	raw, err := app.MarshalTransportOptions(cfg, server)
	if err != nil {
		return app.Settings{}, err
	}
	out := app.Settings{
		Name:      p.RouteName,
		Type:      "raw_ws",
		Transport: raw,
	}
	if !server {
		out.UserID = p.UserID
	}
	out.ExitNodeID = p.ExitNodeID
	out.EncryptionPassword = p.Secret("encryption_password")
	return out, nil
}
