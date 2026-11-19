package livekit

import (
	"context"
	"testing"
	"time"
)

type nopLogger struct{}

func (nopLogger) Trace(...any)                         {}
func (nopLogger) Debug(...any)                         {}
func (nopLogger) Info(...any)                          {}
func (nopLogger) Warn(...any)                          {}
func (nopLogger) Error(...any)                         {}
func (nopLogger) Fatal(...any)                         {}
func (nopLogger) Panic(...any)                         {}
func (nopLogger) TraceContext(context.Context, ...any) {}
func (nopLogger) DebugContext(context.Context, ...any) {}
func (nopLogger) InfoContext(context.Context, ...any)  {}
func (nopLogger) WarnContext(context.Context, ...any)  {}
func (nopLogger) ErrorContext(context.Context, ...any) {}
func (nopLogger) FatalContext(context.Context, ...any) {}
func (nopLogger) PanicContext(context.Context, ...any) {}

func TestNewProviderConnectFailureCleansUp(t *testing.T) {
	cfg := Config{
		SignalURL: "ws://127.0.0.1:1",
		APIKey:    "key",
		APISecret: "secret",
		Room:      "room",
		DataTopic: "shampoore",
	}

	started := time.Now()
	p, err := NewProvider(context.Background(), cfg, nopLogger{})
	if err == nil {
		if p != nil {
			_ = p.Stop(context.Background())
		}
		t.Fatal("expected a connect error against an unreachable signal server")
	}
	if elapsed := time.Since(started); elapsed > 25*time.Second {
		t.Fatalf("connect failure took too long: %s", elapsed)
	}
}

func TestProviderStartAfterStopFails(t *testing.T) {
	p := &Provider{
		logger:       nopLogger{},
		disconnected: make(chan struct{}, 1),
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := p.Start(context.Background()); err == nil {
		t.Fatal("expected start on a stopped provider to fail")
	}
}
