package app

import (
	"context"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
)

type SessionHandler func(ctx context.Context, mux multiplexing.StreamMultiplexer) error

type SessionServer interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	SetSessionHandler(SessionHandler)
}
