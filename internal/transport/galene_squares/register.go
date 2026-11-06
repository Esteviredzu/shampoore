package galene_squares

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/core/framing"
	"github.com/Esteviredzu/shampoore-core/internal/core/reliability/kcp"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/provider/galene"
)

var defaults = app.Values{
	"mux_flush_ms":        10,
	"mux_flush_threshold": 5000,

	"kcp_conv":     0,
	"kcp_mtu":      900,
	"kcp_window":   512,
	"kcp_nodelay":  1,
	"kcp_interval": 5,
	"kcp_resend":   2,
	"kcp_nc":       true,

	"squares_bits_per_cell": 4,
	"squares_cell_size":     16,
	"squares_cols":          144,
	"squares_rows":          81,
	"squares_fps":           60,
	"squares_bitrate":       "10000K",
	"squares_copies":        1,
}

var insecureTLS = false

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "galene_squares",
		DisplayName: "Galene (framing + KCP)",
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

	palette, err := framing.NewPalette(eff.Int("squares_bits_per_cell", 0))
	if err != nil {
		return nil, fmt.Errorf("galene_squares: palette: %w", err)
	}

	var cfg galene.Config
	if data := s.TransportData(); len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	logger.Info("galene_squares: group_url=", cfg.GroupURL, " group=", cfg.Group, " user=", cfg.Username)

	provider, err := galene.NewProvider(ctx, cfg, logger, insecureTLS)
	if err != nil {
		return nil, err
	}
	tunnel := provider.Tunnel()
	if tunnel == nil {
		_ = provider.Stop(ctx)
		return nil, fmt.Errorf("galene_squares: tunnel is nil")
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
		_ = provider.Stop(ctx)
		return nil, err
	}

	sqCfg := framing.Config{
		Width: w, Height: h,
		Cols: cols, Rows: rows,
		CellSize: cellSize,
		FPS:      fps,
		Bitrate:  bitrate,
		Copies:   copies,
	}
	sqPipe, err := framing.NewPipe(tunnel, sqCfg, palette)
	if err != nil {
		_ = provider.Stop(ctx)
		return nil, err
	}
	if err := sqPipe.Start(ctx); err != nil {
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
	inner, err := kcp.NewSession(sqPipe, logger, kcpCfg)
	if err != nil {
		_ = sqPipe.Stop(ctx)
		_ = provider.Stop(ctx)
		return nil, err
	}
	if err := inner.Start(ctx); err != nil {
		_ = sqPipe.Stop(ctx)
		_ = provider.Stop(ctx)
		return nil, err
	}

	logger.Info("galene_squares: ready (mtu=", kcpCfg.MTU, " conv=", conv,
		" window=", kcpCfg.Window,
		" frame=", w, "x", h,
		" bits=", palette.BitsPerCell,
		" payload=", cols*rows*palette.BitsPerCell/8, "B",
		" fps=", fps, ")")

	return &app.BuildResult{
		Transport:    inner,
		Provider:     provider,
		BypassHosts:  provider.BypassHosts(),
		ConnectedIPs: provider.ConnectedIPsFunc(),
	}, nil
}
