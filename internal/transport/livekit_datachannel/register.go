package livekit_datachannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/capability"
	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/net/utlsdial"
	"github.com/Esteviredzu/shampoore/internal/netdial"
	lk "github.com/Esteviredzu/shampoore/internal/provider/livekit"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

var defaults = app.Values{
	"mux_flush_ms":        5,
	"mux_flush_threshold": 12000,
}

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "livekit_datachannel",
		DisplayName: "LiveKit",
		Description: "LiveKit Reliable DataChannel",
		Fields: []app.Field{
			{
				Key:         "signal_url",
				Label:       "Signal URL",
				Type:        app.FieldTypeText,
				Required:    true,
				Placeholder: "wss://...",
			},
			{
				Key:      "api_key",
				Label:    "API Key",
				Type:     app.FieldTypeText,
				Required: true,
			},
			{
				Key:      "api_secret",
				Label:    "API Secret",
				Type:     app.FieldTypePassword,
				Required: true,
			},
			{
				Key:      "room",
				Label:    "Room",
				Type:     app.FieldTypeText,
				Required: true,
			},
			{
				Key:         "data_topic",
				Label:       "Data Topic",
				Type:        app.FieldTypeText,
				Placeholder: "shampoore",
			},
			{
				Key:      "user_id",
				Label:    "User ID",
				Type:     app.FieldTypeText,
				Required: false,
				Hint:     "Multiuser: joins the personal room <room_prefix>-<user_id>",
			},
			{
				Key:      "multiuser",
				Label:    "Multiuser server",
				Type:     app.FieldTypeBool,
				Required: false,
				Hint:     "Server only: run one isolated room per active user",
			},
			{
				Key:         "room_prefix",
				Label:       "Room Prefix",
				Type:        app.FieldTypeText,
				Required:    false,
				Placeholder: "shampoore",
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
		},
		Defaults: defaults,
		Caps: app.Capabilities{
			Relay:     relayProvider{},
			Multiuser: app.MultiuserConfig{},
			Secrets: app.SecretProviderFunc(func() []capability.SecretSpec {
				return []capability.SecretSpec{
					{
						Key:       "livekit_api_key",
						Label:     "LiveKit API Key",
						Generator: capability.SecretGenerator{Kind: "hex", Bytes: 8},
						Editable:  true,
					},
					{
						Key:       "livekit_api_secret",
						Label:     "LiveKit API Secret",
						Generator: capability.SecretGenerator{Kind: "base64", Bytes: 32},
						Editable:  true,
					},
				}
			}),
		},
		DecodeURL: lk.DecodeURL,
		EncodeURL: lk.EncodeURL,
		Validate:  validate,
		Build:     build,

		Aliases:                 []string{"livekit"},
		BuildSubscriptionConfig: buildSubscriptionConfig,
		BuildServerConfig:       buildServerConfig,
		RelayUpstream:           "livekit:7880",
		RelaySecrets:            []string{"livekit_api_key", "livekit_api_secret"},
	})
	if err != nil {
		panic(err)
	}
}

func build(
	ctx context.Context,
	s *app.Settings,
	role string,
	logger logging.ContextLogger,
	eff app.Values,
) (*app.BuildResult, error) {
	var cfg lk.Config
	if data := s.TransportData(); len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	if strings.TrimSpace(cfg.UserID) == "" {
		cfg.UserID = strings.TrimSpace(s.UserID)
	}
	if strings.TrimSpace(cfg.ExitNodeID) == "" {
		cfg.ExitNodeID = strings.TrimSpace(s.ExitNodeID)
	}

	if strings.TrimSpace(s.UserID) == "" && cfg.UserID != "" {
		s.UserID = cfg.UserID
	}
	if strings.TrimSpace(s.ExitNodeID) == "" && cfg.ExitNodeID != "" {
		s.ExitNodeID = cfg.ExitNodeID
	}
	if role != "server" {

		normalizeClientIdentity(&cfg)
	}
	if err := cfg.ValidateForRole(role); err != nil {
		return nil, err
	}

	if role == "server" && cfg.Multiuser {
		registry, err := lk.BuildUserRegistry(cfg.Users)
		if err != nil {
			return nil, err
		}
		muxCfg := tunnel.Config{
			FlushThreshold: eff.Int("mux_flush_threshold", 12000),
			FlushInterval:  time.Duration(eff.Int("mux_flush_ms", 5)) * time.Millisecond,
			Registry:       registry,
		}
		srv := lk.NewMultiServer(cfg, logger, registry, muxCfg, s.EncryptionPassword)
		srv.SetExitNodeID(s.ExitNodeID)
		logger.Info("livekit_datachannel: multiuser server ready (signal_url=", cfg.SignalURL,
			" room_prefix=", cfg.RoomPrefix, " users=", len(cfg.Users), ")")
		return &app.BuildResult{
			SessionServer: srv,
			UserRegistry:  registry,
			BypassHosts:   bypassHosts(ctx, cfg.SignalURL),
		}, nil
	}

	logger.Info("livekit_datachannel: signal_url=", cfg.SignalURL,
		" room=", cfg.Room, " user=", cfg.UserID)

	provider, err := lk.NewProvider(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}

	var pipe transport.Pipe = provider.Pipe()
	if pipe == nil {
		_ = provider.Stop(ctx)
		return nil, fmt.Errorf("livekit_datachannel: pipe not created")
	}
	if s.EncryptionPassword != "" {
		wrapped, err := security.WrapPipeWithRole(pipe, s.EncryptionPassword, role, logger)
		if err != nil {
			_ = provider.Stop(ctx)
			return nil, fmt.Errorf("livekit_datachannel: wrap encryption: %w", err)
		}
		pipe = wrapped
		logger.Info("livekit_datachannel: XChaCha20-Poly1305 encryption enabled")
	}

	t := lk.NewTransport(pipe)

	logger.Info("livekit_datachannel: ready (max_msg=", t.MaxMessageSize(),
		" flush_ms=", eff.Int("mux_flush_ms", 5),
		" flush_threshold=", eff.Int("mux_flush_threshold", 12000), ")")

	return &app.BuildResult{
		Transport:    t,
		Provider:     provider,
		BypassHosts:  provider.BypassHosts(),
		ConnectedIPs: provider.ConnectedIPsFunc(),
	}, nil
}

func validate(raw json.RawMessage) error {
	cfg := lk.Config{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
	}
	return cfg.Validate()
}

func bypassHosts(ctx context.Context, signalURL string) []string {
	u, err := url.Parse(signalURL)
	if err != nil || u.Hostname() == "" {
		return nil
	}
	host := u.Hostname()
	out := []string{host}
	out = append(out, netdial.ResolveIPv4(ctx, host)...)
	return out
}

func normalizeClientIdentity(cfg *lk.Config) {
	if cfg == nil {
		return
	}
	if strings.TrimSpace(cfg.Identity) == "" && strings.TrimSpace(cfg.UserID) != "" {
		cfg.Identity = cfg.UserID
	}
	identity := strings.TrimSpace(cfg.Identity)
	if identity == "" {
		return
	}
	if normalized, err := auth.NormalizeUserID(identity); err == nil {
		cfg.Identity = normalized
		return
	}
	cfg.Identity = identity
}

func routeRoomPrefix(p app.RouteParams) string {
	if p.RouteID > 0 {
		return "route-" + strconv.FormatInt(p.RouteID, 10)
	}
	if name := strings.TrimSpace(p.RouteName); name != "" {
		return name
	}
	return "route"
}

func buildSubscriptionConfig(p app.RouteParams) (app.Settings, error) {
	signalURL := app.NormalizeWSURL(p.Domain)
	if strings.TrimSpace(signalURL) == "" || signalURL == "wss://" {
		return app.Settings{}, errors.New("livekit: signal domain is required")
	}
	cfg := lk.Config{
		SignalURL:  signalURL,
		APIKey:     p.Secret("livekit_api_key"),
		APISecret:  p.Secret("livekit_api_secret"),
		DataTopic:  "shampoore",
		RoomPrefix: routeRoomPrefix(p),
		UserID:     p.UserID,
		ExitNodeID: p.ExitNodeID,
	}
	userID := strings.TrimSpace(p.UserID)
	if userID == "" {
		return app.Settings{}, errors.New("livekit: user_id is required for multiuser subscription")
	}
	cfg.Room = lk.RoomForUser(cfg.RoomPrefix, userID)
	if err := cfg.ValidateForRole("client"); err != nil {
		return app.Settings{}, err
	}
	return livekitSettings(cfg, p, false)
}

func buildServerConfig(p app.RouteParams) (app.Settings, error) {
	signalURL := app.NormalizeWSURL(p.Domain)
	if strings.TrimSpace(signalURL) == "" || signalURL == "wss://" {
		return app.Settings{}, errors.New("livekit: signal domain is required")
	}
	cfg := lk.Config{
		SignalURL:  signalURL,
		APIKey:     p.Secret("livekit_api_key"),
		APISecret:  p.Secret("livekit_api_secret"),
		Multiuser:  true,
		Users:      make([]auth.UserSpec, 0),
		DataTopic:  "shampoore",
		RoomPrefix: routeRoomPrefix(p),
	}
	if err := cfg.ValidateForRole("server"); err != nil {
		return app.Settings{}, err
	}
	return livekitSettings(cfg, p, true)
}

func livekitSettings(cfg lk.Config, p app.RouteParams, server bool) (app.Settings, error) {
	raw, err := app.MarshalTransportOptions(cfg, server)
	if err != nil {
		return app.Settings{}, err
	}

	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return app.Settings{}, err
	}
	for key, value := range obj {
		if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
			delete(obj, key)
		}
	}
	raw, err = json.Marshal(obj)
	if err != nil {
		return app.Settings{}, err
	}
	out := app.Settings{
		Name:      p.RouteName,
		Type:      "livekit_datachannel",
		Transport: raw,
	}
	if !server {
		out.UserID = p.UserID
	}
	out.ExitNodeID = p.ExitNodeID
	out.EncryptionPassword = p.Secret("encryption_password")
	return out, nil
}
