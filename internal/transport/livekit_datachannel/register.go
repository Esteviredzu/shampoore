package livekit_datachannel

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	logger.Info("livekit_datachannel: signal_url=", cfg.SignalURL, " room=", cfg.Room)

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

	t := newDirectTransport(pipe)

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
