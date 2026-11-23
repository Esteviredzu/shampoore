package app

import (
	"context"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type SessionHandler func(ctx context.Context, mux multiplexing.StreamMultiplexer) error

type SessionServer interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	SetSessionHandler(SessionHandler)
}

type SessionTransportSink func(userID string, tr transport.Transport) (release func(), done <-chan struct{})

type AggregableSession interface {
	SessionServer
	SetTransportSink(SessionTransportSink)
}
