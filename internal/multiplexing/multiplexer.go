package multiplexing

import (
	"context"
	"net"

	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

type Stream interface {
	net.Conn
	ID() string
	Metadata() map[string]any
	Closed() bool
}

type StreamMultiplexer interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	OpenStream(ctx context.Context, metadata map[string]any) (Stream, error)
	AcceptStream(ctx context.Context) (Stream, error)
}

// SubflowTransport describes one subflow inside a multi-transport build.
// The multi descriptor builds N of these and returns them via
// app.BuildResult.Subflows instead of a single aggregated Transport.
type SubflowTransport struct {
	Name      string
	Transport transport.Transport
	Weight    int
}