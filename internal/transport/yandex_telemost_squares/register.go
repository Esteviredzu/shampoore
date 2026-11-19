package yandex_telemost_squares

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/core/framing"
	"github.com/Esteviredzu/shampoore/internal/core/reliability/kcp"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/provider/telemost"
)

var defaults = app.Values{
	"mux_flush_ms":        10,
	"mux_flush_threshold": 4000,

	"kcp_conv":     0,
	"kcp_mtu":      390,
	"kcp_window":   256,
	"kcp_nodelay":  1,
	"kcp_interval": 5,
	"kcp_resend":   2,
	"kcp_nc":       true,

	"fec_enabled":       false,
	"fec_data_shards":   10,
	"fec_parity_shards": 3,

	"squares_bits_per_cell": 4,
	"squares_cell_size":     16,
	"squares_cols":          120,
	"squares_rows":          80,
	"squares_fps":           45,
	"squares_bitrate":       "4000K",
	"squares_copies":        1,
}

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "yandex_telemost_squares",
		DisplayName: "Telemost Squares",
		Description: "Yandex Telemost with framing",
		Fields: []app.Field{
			{
				Key:         "conference_link",
				Label:       "Conference Link",
				Type:        app.FieldTypeText,
				Required:    true,
				Placeholder: "https://telemost.yandex.ru/j/...",
				Hint:        "Conference URL or its ID",
			},
		},
		Defaults:  defaults,
		DecodeURL: telemost.DecodeURL,
		EncodeURL: telemost.EncodeURL,
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
	palette, err := framing.NewPalette(eff.Int("squares_bits_per_cell", 0))
	if err != nil {
		return nil, fmt.Errorf("yandex_telemost_squares: palette: %w", err)
	}

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
		_ = providerPipe.Stop(ctx)
		return nil, fmt.Errorf("yandex_telemost_squares: signaling start: %w", err)
	}
	tunnel := providerPipe.Tunnel()
	if tunnel == nil {
		_ = providerPipe.Stop(ctx)
		return nil, fmt.Errorf("yandex_telemost_squares: tunnel is nil")
	}

	cellSize := eff.Int("squares_cell_size", 0)
	cols := eff.Int("squares_cols", 0)
	rows := eff.Int("squares_rows", 0)
	fps := eff.Int("squares_fps", 0)
	bitrate := eff.String("squares_bitrate", "")
	copies := eff.Int("squares_copies", 0)

	grid := framing.Grid{CellSize: cellSize, Cols: cols, Rows: rows}
	w, h, _, _, _, err := grid.DeriveLayout(palette)
	if err != nil {
		_ = providerPipe.Stop(ctx)
		return nil, err
	}

	sqCfg := framing.Config{
		Width:    w,
		Height:   h,
		Cols:     cols,
		Rows:     rows,
		CellSize: cellSize,
		FPS:      fps,
		Bitrate:  bitrate,
		Copies:   copies,
	}
	sqPipe, err := framing.NewPipe(tunnel, sqCfg, palette)
	if err != nil {
		_ = providerPipe.Stop(ctx)
		return nil, err
	}
	if err := sqPipe.Start(ctx); err != nil {
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
		MTU:      eff.Int("kcp_mtu", 390),
		Window:   eff.Int("kcp_window", 256),
		NoDelay:  eff.Int("kcp_nodelay", 1),
		Interval: eff.Int("kcp_interval", 5),
		Resend:   eff.Int("kcp_resend", 2),
		NC:       eff.Bool("kcp_nc", true),

		FECEnabled:      eff.Bool("fec_enabled", false),
		FECDataShards:   eff.Int("fec_data_shards", 10),
		FECParityShards: eff.Int("fec_parity_shards", 3),

		EncryptionPassword: s.EncryptionPassword,
		Role:               role,
	}
	inner, err := kcp.NewSession(sqPipe, logger, kcpCfg)
	if err != nil {
		_ = sqPipe.Stop(ctx)
		_ = providerPipe.Stop(ctx)
		return nil, err
	}

	providerPipe.SetTransportResetFunc(func() {
		logger.Info("yandex_telemost_squares: remote reconnect detected, resetting KCP session")
		if resetter, ok := interface{}(inner).(interface{ Reset() error }); ok {
			if err := resetter.Reset(); err != nil {
				logger.Warn("yandex_telemost_squares: KCP reset failed: ", err)
			}
		}
	})

	if err := inner.Start(ctx); err != nil {
		_ = sqPipe.Stop(ctx)
		_ = providerPipe.Stop(ctx)
		return nil, err
	}

	logger.Info("yandex_telemost_squares: ready (mtu=", kcpCfg.MTU, " conv=", conv,
		" frame=", w, "x", h,
		" bits=", palette.BitsPerCell,
		" fps=", fps, ")")

	return &app.BuildResult{
		Transport:    inner,
		Provider:     providerPipe,
		BypassHosts:  providerPipe.BypassHosts(),
		ConnectedIPs: providerPipe.ConnectedIPs,
	}, nil
}

func validate(raw json.RawMessage) error {
	cfg := telemost.DefaultConfig()
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
	}
	return cfg.Validate()
}
