package livekit

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	lkauth "github.com/livekit/protocol/auth"
	lksdk "github.com/livekit/server-sdk-go/v2"

	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
)

type fakeRoom struct {
	mu           sync.Mutex
	participants int
	events       roomEvents
	disconnected chan struct{}
	once         sync.Once
}

func newFakeRoom(events roomEvents) *fakeRoom {
	return &fakeRoom{events: events, disconnected: make(chan struct{})}
}

func (r *fakeRoom) LocalParticipant() *lksdk.LocalParticipant { return nil }

func (r *fakeRoom) RemoteParticipantCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.participants
}

func (r *fakeRoom) setParticipants(n int) {
	r.mu.Lock()
	r.participants = n
	r.mu.Unlock()
}

func (r *fakeRoom) Disconnect() {
	r.once.Do(func() { close(r.disconnected) })
}

type dialCall struct {
	url    string
	token  string
	events roomEvents
	room   *fakeRoom
}

type fakeDialer struct {
	mu    sync.Mutex
	calls []dialCall
}

func (d *fakeDialer) dial(_ context.Context, url, token string, events roomEvents) (roomSession, error) {
	room := newFakeRoom(events)
	d.mu.Lock()
	d.calls = append(d.calls, dialCall{url: url, token: token, events: events, room: room})
	d.mu.Unlock()
	return room, nil
}

func (d *fakeDialer) last() dialCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.calls) == 0 {
		return dialCall{}
	}
	return d.calls[len(d.calls)-1]
}

type deleteRecorder struct {
	mu    sync.Mutex
	rooms []string
	done  chan string
}

func newDeleteRecorder() *deleteRecorder {
	return &deleteRecorder{done: make(chan string, 8)}
}

func (d *deleteRecorder) delete(_ context.Context, room string) error {
	d.mu.Lock()
	d.rooms = append(d.rooms, room)
	d.mu.Unlock()
	select {
	case d.done <- room:
	default:
	}
	return nil
}

func newTestSessionServer(t *testing.T, cfg Config) (*MultiServer, *fakeDialer, *deleteRecorder, *auth.UserRegistry) {
	t.Helper()
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{
		UserID: "alice", Enabled: true, AllowedExits: []string{"exit-germany"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Upsert(auth.UserSpec{UserID: "bob", Enabled: false}); err != nil {
		t.Fatal(err)
	}
	server, dialer, deleted := newTestMultiServer(t, cfg, registry)
	return server, dialer, deleted, registry
}

func newTestMultiServer(t *testing.T, cfg Config, registry *auth.UserRegistry) (*MultiServer, *fakeDialer, *deleteRecorder) {
	t.Helper()
	if cfg.SignalURL == "" {
		cfg.SignalURL = "wss://livekit.example"
	}
	if cfg.APIKey == "" {
		cfg.APIKey = "key"
	}
	if cfg.APISecret == "" {
		cfg.APISecret = "secret"
	}
	if cfg.RoomPrefix == "" {
		cfg.RoomPrefix = "test"
	}
	server := NewMultiServer(cfg, nopLogger{}, registry, tunnel.Config{
		FlushThreshold: 12000,
		FlushInterval:  5 * time.Millisecond,
		Registry:       registry,
	}, "")
	dialer := &fakeDialer{}
	deleted := newDeleteRecorder()
	server.connectRoom = dialer.dial
	server.deleteRoom = deleted.delete
	server.SetSessionHandler(func(ctx context.Context, _ multiplexing.StreamMultiplexer) error {
		<-ctx.Done()
		return ctx.Err()
	})
	return server, dialer, deleted
}

func startTestMultiServer(t *testing.T, server *MultiServer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := server.Stop(stopCtx); err != nil {
			t.Errorf("stop: %v", err)
		}
	})
}

func newSubscriptionRegistry(t *testing.T, specs ...auth.UserSpec) *auth.UserRegistry {
	t.Helper()
	registry := auth.NewRegistry()
	for _, spec := range specs {
		if err := registry.Upsert(spec); err != nil {
			t.Fatalf("upsert %s: %v", spec.UserID, err)
		}
	}
	return registry
}

func waitForCondition(t *testing.T, timeout time.Duration, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForSessionCount(t *testing.T, server *MultiServer, want int) {
	t.Helper()
	waitForCondition(t, 5*time.Second, "session count", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return len(server.sessions) == want
	})
}

func TestMultiServerSubscribesToRegistry(t *testing.T) {
	registry := newSubscriptionRegistry(t,
		auth.UserSpec{UserID: "alice", Enabled: true},
		auth.UserSpec{UserID: "bob", Enabled: false},
	)
	server, dialer, deleted := newTestMultiServer(t, Config{IdleTimeoutMs: 60_000}, registry)
	startTestMultiServer(t, server)

	waitForSessionCount(t, server, 1)

	server.mu.Lock()
	session := server.sessions["test-alice"]
	server.mu.Unlock()
	if session == nil {
		t.Fatal("alice subscription session was not registered")
	}

	call := dialer.last()
	if call.token == "" {
		t.Fatal("server token was not used")
	}
	verifier, err := lkauth.ParseAPIToken(call.token)
	if err != nil {
		t.Fatalf("parse server token: %v", err)
	}
	if !strings.HasPrefix(verifier.Identity(), "server-") {
		t.Fatalf("server identity = %q", verifier.Identity())
	}
	if _, grants, err := verifier.Verify("secret"); err != nil {
		t.Fatalf("verify server token: %v", err)
	} else if grants.Video == nil || !grants.Video.RoomJoin || grants.Video.Room != "test-alice" {
		t.Fatalf("unexpected server grant: %+v", grants.Video)
	}

	call.room.setParticipants(1)
	call.events.OnParticipantJoined("alice")
	call.room.setParticipants(0)
	call.events.OnParticipantLeft("alice")
	time.Sleep(50 * time.Millisecond)

	server.mu.Lock()
	stillActive := len(server.sessions) == 1
	server.mu.Unlock()
	if !stillActive {
		t.Fatal("subscription session closed after the client left the room")
	}

	registry.Remove("alice")
	select {
	case <-call.room.disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("user removal did not disconnect the room")
	}
	select {
	case room := <-deleted.done:
		if room != "test-alice" {
			t.Fatalf("deleted room = %q, want test-alice", room)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("user removal did not delete the room")
	}
	waitForSessionCount(t, server, 0)
}

func TestMultiServerSubscriptionStopsInactiveUsers(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		registry := newSubscriptionRegistry(t, auth.UserSpec{UserID: "alice", Enabled: true})
		server, dialer, deleted := newTestMultiServer(t, Config{IdleTimeoutMs: 60_000}, registry)
		startTestMultiServer(t, server)
		waitForSessionCount(t, server, 1)
		call := dialer.last()

		if err := registry.Upsert(auth.UserSpec{UserID: "alice", Enabled: false}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-call.room.disconnected:
		case <-time.After(3 * time.Second):
			t.Fatal("disabling the user did not disconnect the room")
		}
		select {
		case room := <-deleted.done:
			if room != "test-alice" {
				t.Fatalf("deleted room = %q", room)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("disabling the user did not delete the room")
		}
		waitForSessionCount(t, server, 0)
	})

	t.Run("traffic limit", func(t *testing.T) {
		registry := newSubscriptionRegistry(t, auth.UserSpec{
			UserID: "alice", Enabled: true, TrafficLimitBytes: 10,
		})
		server, dialer, deleted := newTestMultiServer(t, Config{IdleTimeoutMs: 60_000}, registry)
		startTestMultiServer(t, server)
		waitForSessionCount(t, server, 1)
		call := dialer.last()

		if err := registry.AddTraffic("alice", 11, 0); err == nil {
			t.Fatal("expected traffic limit error")
		}
		select {
		case <-call.room.disconnected:
		case <-time.After(3 * time.Second):
			t.Fatal("traffic limit did not disconnect the room")
		}
		select {
		case room := <-deleted.done:
			if room != "test-alice" {
				t.Fatalf("deleted room = %q", room)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("traffic limit did not delete the room")
		}
		waitForSessionCount(t, server, 0)
	})

	t.Run("expiry", func(t *testing.T) {
		registry := newSubscriptionRegistry(t, auth.UserSpec{
			UserID: "alice", Enabled: true, ExpiresAt: time.Now().Add(time.Second),
		})
		server, dialer, deleted := newTestMultiServer(t, Config{IdleTimeoutMs: 60_000}, registry)
		server.syncInterval = 20 * time.Millisecond
		startTestMultiServer(t, server)
		waitForSessionCount(t, server, 1)
		call := dialer.last()

		select {
		case <-call.room.disconnected:
		case <-time.After(5 * time.Second):
			t.Fatal("expired user did not disconnect the room")
		}
		select {
		case room := <-deleted.done:
			if room != "test-alice" {
				t.Fatalf("deleted room = %q", room)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("expired user did not delete the room")
		}
		waitForSessionCount(t, server, 0)
	})
}
