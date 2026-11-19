package app

import (
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type BuildResult struct {
	Transport transport.Transport

	StreamMux multiplexing.StreamMultiplexer

	Provider    Provider
	BypassHosts []string
	Side        []SideService

	ConnectedIPs func() []string

	UserRegistry  *auth.UserRegistry
	SessionServer SessionServer
}
