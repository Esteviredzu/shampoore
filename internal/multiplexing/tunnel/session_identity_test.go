package tunnel

import (
	"context"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/auth"
)

func TestSessionIdentityReturnsConfirmedHello(t *testing.T) {
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{
		UserID: "alice", Enabled: true, AllowedExits: []string{"exit-1"},
	}); err != nil {
		t.Fatal(err)
	}

	tr := newFakeTransport()
	m := New(tr, testLogger{}, registryConfig(t, registry))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	if user, exit := m.SessionIdentity(); user != "" || exit != "" {
		t.Fatalf("identity before hello = %q/%q", user, exit)
	}

	sendFrameToMux(t, tr, &TunnelFrame{
		Type:  FrameHello,
		Hello: &Hello{UserID: "alice", ExitNodeID: "exit-1"},
	})
	waitFor(t, time.Second, "authorization", func() bool {
		user, exit := m.SessionIdentity()
		return user == "alice" && exit == "exit-1"
	})
}

func TestSessionIdentityWithoutRegistry(t *testing.T) {
	tr := newFakeTransport()
	m := New(tr, testLogger{}, Config{
		FlushThreshold: 12000,
		FlushInterval:  5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	sendFrameToMux(t, tr, &TunnelFrame{
		Type:  FrameHello,
		Hello: &Hello{UserID: "alice", ExitNodeID: "local"},
	})
	waitFor(t, time.Second, "identity without registry", func() bool {
		user, exit := m.SessionIdentity()
		return user == "alice" && exit == "local"
	})
}

func TestSessionIdentityIgnoresRejectedHello(t *testing.T) {
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{UserID: "alice", Enabled: true}); err != nil {
		t.Fatal(err)
	}

	tr := newFakeTransport()
	m := New(tr, testLogger{}, registryConfig(t, registry))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	sendFrameToMux(t, tr, &TunnelFrame{
		Type:  FrameHello,
		Hello: &Hello{UserID: "mallory", ExitNodeID: "exit-1"},
	})
	waitForFrame(t, tr, time.Second, func(f *TunnelFrame) bool {
		return f.Type == FrameError && f.Error != nil && f.Error.Code == "unknown_user"
	})
	if user, exit := m.SessionIdentity(); user != "" || exit != "" {
		t.Fatalf("rejected hello leaked identity: %q/%q", user, exit)
	}
}
