package yandex_telemost_raw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/core/reliability/kcp"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/mode/raw"
	"github.com/Esteviredzu/shampoore-core/internal/provider/telemost"
)

var defaults = app.Values{
	"mux_flush_ms":        10,
	"mux_flush_threshold": 12000,

	"kcp_conv":     0,
	"kcp_mtu":      1200,
	"kcp_window":   128,
	"kcp_nodelay":  1,
	"kcp_interval": 5,
	"kcp_resend":   2,
	"kcp_nc":       true,

	"fec_enabled":       false,
	"fec_data_shards":   10,
	"fec_parity_shards": 3,

	"raw_frame_size": 9000,
	"raw_fps":        60,
	"raw_copies":     2,
	"raw_gop":        30,
}

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "yandex_telemost_raw",
		DisplayName: "Yandex Telemost (raw VP8)",
		Defaults:    defaults,
		DecodeURL:   telemost.DecodeURL,
		EncodeURL:   telemost.EncodeURL,
		Build:       build,
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
	cfg := telemost.DefaultConfig()
	if data := s.TransportData(); len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	boot, err := telemost.Bootstrap(ctx, &cfg)
	if err != nil {
		return nil, err
	}

	providerPipe := telemost.NewPipe(&cfg, boot, logger)
	if err := providerPipe.Start(ctx); err != nil {
		return nil, fmt.Errorf("yandex_telemost_raw: signaling start: %w", err)
	}
	tunnel := providerPipe.Tunnel()
	if tunnel == nil {
		_ = providerPipe.Stop(ctx)
		return nil, errors.New("yandex_telemost_raw: tunnel not created")
	}

	rawCfg := raw.Config{
		FrameSize: eff.Int("raw_frame_size", 9000),
		FPS:       eff.Int("raw_fps", 60),
		Copies:    eff.Int("raw_copies", 2),
		GOP:       eff.Int("raw_gop", 30),
	}
	rawPipe := raw.New(tunnel, rawCfg, logger)
	if err := rawPipe.Start(ctx); err != nil {
		_ = providerPipe.Stop(ctx)
		return nil, err
	}

	conv := uint32(eff.Int("kcp_conv", 0))
	if conv == 0 {
		conv = crc32.ChecksumIEEE([]byte(cfg.EffectiveConferenceLink()))
		if conv == 0 {
			conv = 1
		}
	}

	kcpCfg := kcp.Config{
		Conv:     conv,
		MTU:      eff.Int("kcp_mtu", 1200),
		Window:   eff.Int("kcp_window", 128),
		NoDelay:  eff.Int("kcp_nodelay", 1),
		Interval: eff.Int("kcp_interval", 5),
		Resend:   eff.Int("kcp_resend", 2),
		NC:       eff.Bool("kcp_nc", true),

		FECEnabled:      eff.Bool("fec_enabled", false),
		FECDataShards:   eff.Int("fec_data_shards", 10),
		FECParityShards: eff.Int("fec_parity_shards", 3),

		EncryptionPassword: s.EncryptionPassword,
	}
	inner, err := kcp.NewSession(rawPipe, logger, kcpCfg)
	if err != nil {
		_ = rawPipe.Stop(ctx)
		_ = providerPipe.Stop(ctx)
		return nil, err
	}

	providerPipe.SetTransportResetFunc(func() {
		logger.Info("yandex_telemost_raw: remote reconnect detected, resetting KCP session")
		if resetter, ok := interface{}(inner).(interface{ Reset() error }); ok {
			if err := resetter.Reset(); err != nil {
				logger.Warn("yandex_telemost_raw: KCP reset failed: ", err)
			}
		}
	})

	if err := inner.Start(ctx); err != nil {
		_ = rawPipe.Stop(ctx)
		_ = providerPipe.Stop(ctx)
		return nil, err
	}

	logger.Info("yandex_telemost_raw: ready (mtu=", kcpCfg.MTU, " conv=", conv,
		" window=", kcpCfg.Window, " copies=", rawCfg.Copies,
		" frame_size=", rawCfg.FrameSize, ")")

	return &app.BuildResult{
		Transport:    inner,
		Provider:     providerPipe,
		BypassHosts:  providerPipe.BypassHosts(),
		ConnectedIPs: providerPipe.ConnectedIPs,
	}, nil
}
