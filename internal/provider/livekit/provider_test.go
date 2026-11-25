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

func TestPublishFailureTrackerDebouncesHiccups(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)

	tr := publishFailureTracker{grace: 3 * time.Second}
	if tr.observeFailure(start, start.Add(-100*time.Millisecond)) {
		t.Fatal("a hiccup right after a successful publish must not rejoin the room")
	}
	if tr.observeFailure(start.Add(time.Second), start.Add(900*time.Millisecond)) {
		t.Fatal("a fresh successful publish resets the burst")
	}

	dead := publishFailureTracker{grace: 3 * time.Second}
	if dead.observeFailure(start, time.Time{}) {
		t.Fatal("the first failed publish must not rejoin the room")
	}
	if dead.observeFailure(start.Add(2*time.Second), time.Time{}) {
		t.Fatal("rejoined before the grace period elapsed")
	}
	if !dead.observeFailure(start.Add(3*time.Second), time.Time{}) {
		t.Fatal("a publish path that stays dead for the grace period must rejoin")
	}
}

func TestPipeLastPublishOK(t *testing.T) {
	p := newPipe(nil, "topic", "")
	if !p.LastPublishOK().IsZero() {
		t.Fatal("a pipe that never published must report a zero time")
	}
	now := time.Now()
	p.lastSentNs.Store(now.UnixNano())
	if got := p.LastPublishOK(); got.UnixNano() != now.UnixNano() {
		t.Fatalf("LastPublishOK = %v, want %v", got, now)
	}
}

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
