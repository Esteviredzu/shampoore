package galene_raw

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/core/reliability/kcp"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/mode/raw"
	"github.com/Esteviredzu/shampoore-core/internal/provider/galene"
)

var defaults = app.Values{
	"mux_flush_ms":        4,
	"mux_flush_threshold": 10000,

	"kcp_conv":     0,
	"kcp_mtu":      1350,
	"kcp_window":   2048,
	"kcp_nodelay":  1,
	"kcp_interval": 5,
	"kcp_resend":   2,
	"kcp_nc":       true,

	"raw_frame_size": 12000,
	"raw_fps":        90,
	"raw_copies":     1,
	"raw_gop":        30,
}

var insecureTLS = false

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "galene_raw",
		DisplayName: "Galene (VP8 raw)",
		Defaults:    defaults,
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
	var cfg galene.Config
	if data := s.TransportData(); len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	logger.Info("galene_raw: group_url=", cfg.GroupURL, " group=", cfg.Group, " user=", cfg.Username)

	provider, err := galene.NewProvider(ctx, cfg, logger, insecureTLS)
	if err != nil {
		return nil, err
	}
	tunnel := provider.Tunnel()
	if tunnel == nil {
		_ = provider.Stop(ctx)
		return nil, fmt.Errorf("galene_raw: tunnel is nil")
	}

	rawCfg := raw.Config{
		FrameSize: eff.Int("raw_frame_size", 0),
		FPS:       eff.Int("raw_fps", 0),
		Copies:    eff.Int("raw_copies", 0),
		GOP:       eff.Int("raw_gop", 0),
	}
	rawPipe := raw.New(tunnel, rawCfg, logger)
	if err := rawPipe.Start(ctx); err != nil {
		_ = provider.Stop(ctx)
		return nil, err
	}

	conv := uint32(eff.Int("kcp_conv", 0))
	if conv == 0 {
		conv = crc32.ChecksumIEEE([]byte(cfg.GroupURL))
		if conv == 0 {
			conv = 1
		}
	}

	kcpCfg := kcp.Config{
		Conv:               conv,
		MTU:                eff.Int("kcp_mtu", 0),
		Window:             eff.Int("kcp_window", 0),
		NoDelay:            eff.Int("kcp_nodelay", 0),
		Interval:           eff.Int("kcp_interval", 0),
		Resend:             eff.Int("kcp_resend", 0),
		NC:                 eff.Bool("kcp_nc", false),
		EncryptionPassword: s.EncryptionPassword,
	}
	inner, err := kcp.NewSession(rawPipe, logger, kcpCfg)
	if err != nil {
		_ = rawPipe.Stop(ctx)
		_ = provider.Stop(ctx)
		return nil, err
	}

	if err := inner.Start(ctx); err != nil {
		_ = rawPipe.Stop(ctx)
		_ = provider.Stop(ctx)
		return nil, err
	}

	logger.Info("galene_raw: ready (mtu=", kcpCfg.MTU, " conv=", conv,
		" window=", kcpCfg.Window,
		" frame_size=", rawCfg.FrameSize,
		" fps=", rawCfg.FPS,
		" copies=", rawCfg.Copies, ")")

	return &app.BuildResult{
		Transport:    inner,
		Provider:     provider,
		BypassHosts:  provider.BypassHosts(),
		ConnectedIPs: provider.ConnectedIPsFunc(),
	}, nil
}
