package app

import (
	"context"
	"fmt"

	"github.com/Esteviredzu/shampoore/internal/logging"
)

func BuildRuntime(
	ctx context.Context,
	s *Settings,
	role string,
	logger logging.ContextLogger,
) (*Runtime, error) {
	d, ok := DescriptorFor(s.TransportKind())
	if !ok {
		return nil, fmt.Errorf("app: unknown transport %q", s.TransportKind())
	}

	eff := d.Defaults.Merge(s.Advanced)

	res, err := d.Build(ctx, s, role, logger, eff)
	if err != nil {
		return nil, err
	}

	rt := NewRuntime(res.Transport, res.Provider, res.BypassHosts, res.Side)
	rt.StreamMux = res.StreamMux
	rt.ConnectedIPs = res.ConnectedIPs
	rt.UserRegistry = res.UserRegistry
	rt.SessionServer = res.SessionServer
	rt.MuxConfig = MuxConfig{
		FlushMs:        eff.Int("mux_flush_ms", 20),
		FlushThreshold: eff.Int("mux_flush_threshold", 12000),
		UserID:         s.UserID,
		ExitNodeID:     s.ExitNodeID,
	}
	return rt, nil
}
