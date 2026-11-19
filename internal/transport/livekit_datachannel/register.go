package livekit_datachannel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
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
				Hint:     "Multiuser / on-demand: identifies the user when requesting a session",
			},
			{
				Key:      "multiuser",
				Label:    "Multiuser server",
				Type:     app.FieldTypeBool,
				Required: false,
				Hint:     "Server only: create LiveKit rooms on demand",
			},
			{
				Key:         "room_prefix",
				Label:       "Room Prefix",
				Type:        app.FieldTypeText,
				Required:    false,
				Placeholder: "shampoore",
			},
			{
				Key:         "token_listen",
				Label:       "Token Listen",
				Type:        app.FieldTypeText,
				Required:    false,
				Placeholder: "127.0.0.1:8099",
				Hint:        "Server: HTTP address for POST /session (also accepted as the client endpoint)",
			},
			{
				Key:         "token_url",
				Label:       "Token URL",
				Type:        app.FieldTypeText,
				Required:    false,
				Placeholder: "https://server.example/session",
			},
			{
				Key:      "token_secret",
				Label:    "Token Secret",
				Type:     app.FieldTypePassword,
				Required: false,
				Hint:     "Optional X-Shampoore-Secret value for the session endpoint",
			},
		},
		Defaults:  defaults,
		DecodeURL: lk.DecodeURL,
		EncodeURL: lk.EncodeURL,
		Validate:  validate,
		Build:     build,
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
		if strings.TrimSpace(cfg.TokenURL) == "" {
			cfg.TokenURL = lk.NormalizeTokenURL(cfg.TokenListen)
		}
		if strings.TrimSpace(cfg.Identity) == "" && strings.TrimSpace(cfg.UserID) != "" {
			cfg.Identity = cfg.UserID
		}
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
		logger.Info("livekit_datachannel: on-demand multiuser server ready (signal_url=",
			cfg.SignalURL, " token_listen=", cfg.TokenListen,
			" users=", len(cfg.Users), ")")
		return &app.BuildResult{
			SessionServer: srv,
			UserRegistry:  registry,
			BypassHosts:   bypassHosts(ctx, cfg.SignalURL),
		}, nil
	}

	logger.Info("livekit_datachannel: signal_url=", cfg.SignalURL,
		" room=", cfg.Room, " token_url=", cfg.TokenURL, " user=", cfg.UserID)

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
