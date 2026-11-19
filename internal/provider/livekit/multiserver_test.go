package livekit

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
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
	if cfg.TokenSecret == "" {
		cfg.TokenSecret = "s3cr3t"
	}
	if cfg.TokenListen == "" {
		cfg.TokenListen = "127.0.0.1:0"
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
	return server, dialer, deleted, registry
}

func startTestSessionServer(t *testing.T, server *MultiServer) string {
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
	addr := server.Addr()
	if addr == "" {
		t.Fatal("server did not bind a listener")
	}
	return addr
}

func postSession(t *testing.T, addr, path, secret, userID, exitNodeID string) (*http.Response, sessionResponse) {
	t.Helper()
	body, err := json.Marshal(sessionRequest{UserID: userID, ExitNodeID: exitNodeID})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		req.Header.Set("X-Shampoore-Secret", secret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post session: %v", err)
	}
	var out sessionResponse
	if resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
	return resp, out
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

func TestMultiServerIssuesSessionAndAuthorizesUser(t *testing.T) {
	server, dialer, deleted, _ := newTestSessionServer(t, Config{IdleTimeoutMs: 60_000})
	addr := startTestSessionServer(t, server)

	resp, session := postSession(t, addr, server.SessionPath(), "s3cr3t", "alice", "exit-germany")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	if session.URL != "wss://livekit.example" {
		t.Fatalf("url = %q", session.URL)
	}
	if !strings.HasPrefix(session.Room, "test-alice-") {
		t.Fatalf("room = %q, want test-alice-*", session.Room)
	}
	if session.Token == "" {
		t.Fatal("client token is empty")
	}

	verifier, err := lkauth.ParseAPIToken(session.Token)
	if err != nil {
		t.Fatalf("parse client token: %v", err)
	}
	if verifier.APIKey() != "key" {
		t.Fatalf("api key = %q", verifier.APIKey())
	}
	if verifier.Identity() != "alice" {
		t.Fatalf("client identity = %q", verifier.Identity())
	}
	claims, grants, err := verifier.Verify("secret")
	if err != nil {
		t.Fatalf("verify client token: %v", err)
	}
	if grants.Video == nil || !grants.Video.RoomJoin || grants.Video.Room != session.Room {
		t.Fatalf("unexpected video grant: %+v", grants.Video)
	}
	if claims.ExpiresAt == nil || !claims.ExpiresAt.After(time.Now()) {
		t.Fatalf("token does not expire in the future: %+v", claims.ExpiresAt)
	}

	call := dialer.last()
	if call.token == "" {
		t.Fatal("server token was not used")
	}
	serverVerifier, err := lkauth.ParseAPIToken(call.token)
	if err != nil {
		t.Fatalf("parse server token: %v", err)
	}
	if !strings.HasPrefix(serverVerifier.Identity(), "server-") {
		t.Fatalf("server identity = %q", serverVerifier.Identity())
	}
	if _, serverGrants, err := serverVerifier.Verify("secret"); err != nil {
		t.Fatalf("verify server token: %v", err)
	} else if serverGrants.Video == nil || serverGrants.Video.Room != session.Room {
		t.Fatalf("unexpected server grant: %+v", serverGrants.Video)
	}

	server.mu.Lock()
	active := len(server.sessions)
	server.mu.Unlock()
	if active != 1 {
		t.Fatalf("active sessions = %d, want 1", active)
	}

	if resp, _ := postSession(t, addr, server.SessionPath(), "", "alice", "exit-germany"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing secret status = %d, want 401", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	if resp, _ := postSession(t, addr, server.SessionPath(), "s3cr3t", "bob", ""); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("disabled user status = %d, want 403", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	if resp, _ := postSession(t, addr, server.SessionPath(), "s3cr3t", "alice", "exit-other"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("forbidden exit status = %d, want 403", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	server.mu.Lock()
	active = len(server.sessions)
	sessionRef := server.sessions[session.Room]
	server.mu.Unlock()
	if active != 1 {
		t.Fatalf("denied requests created sessions: %d", active)
	}
	if sessionRef == nil {
		t.Fatal("session was not registered")
	}
	sessionRef.close("test cleanup")
	if room := <-deleted.done; room != session.Room {
		t.Fatalf("deleted room = %q, want %q", room, session.Room)
	}
}

func TestMultiServerAutoClosesSession(t *testing.T) {
	t.Run("client left", func(t *testing.T) {
		server, dialer, deleted, _ := newTestSessionServer(t, Config{IdleTimeoutMs: 60_000})
		addr := startTestSessionServer(t, server)

		resp, session := postSession(t, addr, server.SessionPath(), "s3cr3t", "alice", "exit-germany")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		resp.Body.Close()

		call := dialer.last()
		call.room.setParticipants(1)
		if call.events.OnParticipantJoined != nil {
			call.events.OnParticipantJoined("alice")
		}
		call.room.setParticipants(0)
		if call.events.OnParticipantLeft != nil {
			call.events.OnParticipantLeft("alice")
		}

		select {
		case <-call.room.disconnected:
		case <-time.After(3 * time.Second):
			t.Fatal("server room was not disconnected")
		}
		select {
		case room := <-deleted.done:
			if room != session.Room {
				t.Fatalf("deleted room = %q", room)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("room delete was not invoked")
		}
		waitForCondition(t, 3*time.Second, "session removal", func() bool {
			server.mu.Lock()
			defer server.mu.Unlock()
			return len(server.sessions) == 0
		})
	})

	t.Run("idle timeout", func(t *testing.T) {
		server, dialer, deleted, _ := newTestSessionServer(t, Config{IdleTimeoutMs: 50})
		addr := startTestSessionServer(t, server)

		resp, session := postSession(t, addr, server.SessionPath(), "s3cr3t", "alice", "exit-germany")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		resp.Body.Close()

		call := dialer.last()
		select {
		case <-call.room.disconnected:
		case <-time.After(3 * time.Second):
			t.Fatal("idle session did not disconnect the room")
		}
		select {
		case room := <-deleted.done:
			if room != session.Room {
				t.Fatalf("deleted room = %q", room)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("idle session did not invoke room delete")
		}
		waitForCondition(t, 3*time.Second, "idle session removal", func() bool {
			server.mu.Lock()
			defer server.mu.Unlock()
			return len(server.sessions) == 0
		})
	})
}

func TestMultiServerHandlerContextCanceledOnStop(t *testing.T) {
	server, _, _, _ := newTestSessionServer(t, Config{IdleTimeoutMs: 60_000})
	handlerDone := make(chan struct{})
	server.SetSessionHandler(func(ctx context.Context, _ multiplexing.StreamMultiplexer) error {
		<-ctx.Done()
		close(handlerDone)
		return ctx.Err()
	})
	addr := startTestSessionServer(t, server)
	resp, _ := postSession(t, addr, server.SessionPath(), "s3cr3t", "alice", "exit-germany")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("session handler was not canceled")
	}
}
