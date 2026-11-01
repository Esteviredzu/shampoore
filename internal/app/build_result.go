package app

import (
	"github.com/Esteviredzu/shampoore-core/internal/multiplexing"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

type BuildResult struct {
	// Transport is set for single-transport builds.
	Transport transport.Transport

	// Subflows is set for the multi transport. When non-empty, consumers
	// must construct a multiplexing/multi.MultiStreamMux instead of a
	// single tunnel.TunnelMultiplexer.
	Subflows []multiplexing.SubflowTransport

	Provider    Provider
	BypassHosts []string
	Side        []SideService

	ConnectedIPs func() []string
}