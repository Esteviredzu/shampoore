package httpstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type restartCountingLogger struct {
	testLogger
	restarts atomic.Int64
}

func (l *restartCountingLogger) Warn(args ...any) {
	if strings.Contains(fmt.Sprint(args...), "server session restarted") {
		l.restarts.Add(1)
	}
}

func (l *restartCountingLogger) restartCount() int64 { return l.restarts.Load() }

func newPairHarness(t *testing.T, fallback http.Handler) (*PairBroker, *httptest.Server, *http.Client) {
	t.Helper()
	broker, err := NewPairBroker(testProfile(DownlinkStream), testServerConfig(fallback))
	if err != nil {
		t.Fatalf("new pair broker: %v", err)
	}
	srv, httpClient, _, _ := newH2Server(t, broker)
	t.Cleanup(func() { _ = broker.Close() })
	return broker, srv, httpClient
}

func newPairPeer(t *testing.T, srv *httptest.Server, httpClient *http.Client, role, user string) *Client {
	t.Helper()
	return newPairPeerWithLogger(t, srv, httpClient, role, user, testLogger{})
}

func newPairPeerWithLogger(t *testing.T, srv *httptest.Server, httpClient *http.Client, role, user string, logger logging.ContextLogger) *Client {
	t.Helper()
	client, err := NewClient(testProfile(DownlinkStream), ClientConfig{
		BaseURL:          srv.URL,
		Secret:           testSecret,
		Namespace:        testNamespace,
		UserID:           user,
		Role:             role,
		HTTPClient:       httpClient,
		Logger:           logger,
		MaxMessageSize:   32 << 10,
		AckInterval:      50 * time.Millisecond,
		ReadIdleTimeout:  5 * time.Second,
		ReconnectDelay:   10 * time.Millisecond,
		ReconnectMax:     50 * time.Millisecond,
		MaxUplinkRetries: 1,
	})
	if err != nil {
		t.Fatalf("new %s client: %v", role, err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start %s client: %v", role, err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Stop(stopCtx)
	})
	return client
}

func waitPairReady(t *testing.T, broker *PairBroker, rooms, sessions int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if broker.RoomCount() >= rooms && broker.SessionCount() >= sessions {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for pair (rooms=%d sessions=%d, got %d/%d)",
				rooms, sessions, broker.RoomCount(), broker.SessionCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func sendEventually(t *testing.T, client *Client, payload []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := client.Send(ctx, payload)
		cancel()
		if err == nil {
			return
		}
		last = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("send %q: %v", payload, last)
}

func receiveEventually(t *testing.T, client *Client, want []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		got, err := client.Receive(ctx)
		cancel()
		if err == nil {
			if bytes.Equal(got, want) {
				return
			}
			t.Fatalf("received %q, want %q", got, want)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Fatalf("timeout waiting for %q", want)
}
func TestPairBrokerRoutesBothDirections(t *testing.T) {
	fallbackReady := make(chan struct{}, 1)
	fallback := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case fallbackReady <- struct{}{}:
		default:
		}
		w.Header().Set("X-Mock-Jellyfin", "yes")
		_, _ = w.Write([]byte("jellyfin-stub"))
	})
	broker, srv, httpClient := newPairHarness(t, fallback)
	client := newPairPeer(t, srv, httpClient, RoleClient, "alice")
	exit := newPairPeer(t, srv, httpClient, RoleExit, "alice")
	waitPairReady(t, broker, 1, 2)

	sendEventually(t, client, []byte("from-client"), 10*time.Second)
	receiveEventually(t, exit, []byte("from-client"), 10*time.Second)

	sendEventually(t, exit, []byte("from-exit"), 10*time.Second)
	receiveEventually(t, client, []byte("from-exit"), 10*time.Second)

	resp, err := httpClient.Get(srv.URL + "/System/Info/Public")
	if err != nil {
		t.Fatalf("fallback get: %v", err)
	}
	body := make([]byte, 64)
	n, _ := resp.Body.Read(body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.HasPrefix(body[:n], []byte("jellyfin-stub")) {
		t.Fatalf("fallback response: status=%d body=%q", resp.StatusCode, body[:n])
	}
	select {
	case <-fallbackReady:
	default:
		t.Fatal("fallback handler was not called")
	}
	if resp.Header.Get("X-Mock-Jellyfin") != "yes" {
		t.Fatal("fallback headers were not passed through")
	}
}

func TestPairBrokerIsolatesUsers(t *testing.T) {
	broker, srv, httpClient := newPairHarness(t, nil)
	aliceClient := newPairPeer(t, srv, httpClient, RoleClient, "alice")
	aliceExit := newPairPeer(t, srv, httpClient, RoleExit, "alice")
	bobClient := newPairPeer(t, srv, httpClient, RoleClient, "bob")
	bobExit := newPairPeer(t, srv, httpClient, RoleExit, "bob")
	waitPairReady(t, broker, 2, 4)

	sendEventually(t, aliceClient, []byte("alice-only"), 10*time.Second)
	receiveEventually(t, aliceExit, []byte("alice-only"), 10*time.Second)

	sendEventually(t, bobClient, []byte("bob-only"), 10*time.Second)
	receiveEventually(t, bobExit, []byte("bob-only"), 10*time.Second)

	sendEventually(t, aliceExit, []byte("alice-back"), 10*time.Second)
	receiveEventually(t, aliceClient, []byte("alice-back"), 10*time.Second)
	sendEventually(t, bobExit, []byte("bob-back"), 10*time.Second)
	receiveEventually(t, bobClient, []byte("bob-back"), 10*time.Second)
}

func TestHandlerRejectsExitRoleByDefault(t *testing.T) {
	handler := NewHandler(testProfile(DownlinkStream), testServerConfig(nil), nil)
	srv, httpClient, _, _ := newH2Server(t, handler)
	t.Cleanup(func() { _ = handler.Close() })

	exit := newPairPeer(t, srv, httpClient, RoleExit, "alice")
	deadline := time.Now().Add(10 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		last = exit.Send(ctx, []byte("forbidden"))
		cancel()
		if last != nil {
			if errors.Is(last, transport.ErrTransportReset) {
				return
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("exit role was accepted by a client-only handler: %v", last)
}

func TestPairBrokerKeepsExitAliveAcrossClientReconnects(t *testing.T) {
	broker, srv, httpClient := newPairHarness(t, nil)
	exitLog := &restartCountingLogger{}
	exit := newPairPeerWithLogger(t, srv, httpClient, RoleExit, "alice", exitLog)
	waitPairReady(t, broker, 1, 1)

	time.Sleep(time.Second)
	if got := exitLog.restartCount(); got != 0 {
		t.Fatalf("exit saw %d server session restarts while waiting for a peer", got)
	}
	generation := exit.serverGen.Load()
	if generation == 0 {
		t.Fatal("exit never observed a server generation")
	}

	client := newPairPeer(t, srv, httpClient, RoleClient, "alice")
	waitPairReady(t, broker, 1, 2)
	sendEventually(t, client, []byte("first-client"), 10*time.Second)
	receiveEventually(t, exit, []byte("first-client"), 10*time.Second)
	sendEventually(t, exit, []byte("exit-to-first"), 10*time.Second)
	receiveEventually(t, client, []byte("exit-to-first"), 10*time.Second)
	if got := exitLog.restartCount(); got != 0 {
		t.Fatalf("exit saw %d server session restarts after pairing", got)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := client.Stop(stopCtx); err != nil {
		t.Fatalf("stop first client: %v", err)
	}
	stopCancel()

	second := newPairPeer(t, srv, httpClient, RoleClient, "alice")
	waitPairReady(t, broker, 1, 2)
	sendEventually(t, second, []byte("second-client"), 10*time.Second)
	receiveEventually(t, exit, []byte("second-client"), 10*time.Second)
	sendEventually(t, exit, []byte("exit-to-second"), 10*time.Second)
	receiveEventually(t, second, []byte("exit-to-second"), 10*time.Second)

	if got := exitLog.restartCount(); got != 0 {
		t.Fatalf("exit saw %d server session restarts after the client reconnected", got)
	}
	if now := exit.serverGen.Load(); now != generation {
		t.Fatalf("exit server generation changed from %d to %d", generation, now)
	}
}

func TestPairBrokerResumesAfterExitReconnect(t *testing.T) {
	broker, srv, httpClient := newPairHarness(t, nil)
	clientLog := &restartCountingLogger{}
	client := newPairPeerWithLogger(t, srv, httpClient, RoleClient, "alice", clientLog)
	exit := newPairPeer(t, srv, httpClient, RoleExit, "alice")
	waitPairReady(t, broker, 1, 2)

	sendEventually(t, client, []byte("before-restart"), 10*time.Second)
	receiveEventually(t, exit, []byte("before-restart"), 10*time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = exit.Stop(stopCtx)
	stopCancel()
	newExit := newPairPeer(t, srv, httpClient, RoleExit, "alice")
	waitPairReady(t, broker, 1, 2)

	sendEventually(t, client, []byte("after-restart"), 15*time.Second)
	receiveEventually(t, newExit, []byte("after-restart"), 15*time.Second)
	sendEventually(t, newExit, []byte("exit-back"), 15*time.Second)
	receiveEventually(t, client, []byte("exit-back"), 15*time.Second)

	if got := clientLog.restartCount(); got != 0 {
		t.Fatalf("client saw %d server session restarts after the exit reconnected", got)
	}
}

func TestSessionTokenRoleRoundTrip(t *testing.T) {
	sid, _, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, want := range []string{RoleClient, RoleExit} {
		token, err := signSessionToken(testSecret, SessionInfo{
			SessionID: sid,
			Namespace: testNamespace,
			UserID:    "alice",
			Role:      want,
		}, now, time.Minute)
		if err != nil {
			t.Fatalf("sign %s: %v", want, err)
		}
		info, err := verifySessionToken(testSecret, token, now, time.Minute, time.Minute, testNamespace)
		if err != nil {
			t.Fatalf("verify %s: %v", want, err)
		}
		if info.Role != want {
			t.Fatalf("role = %q, want %q", info.Role, want)
		}
	}

	defaultToken, err := signSessionToken(testSecret, SessionInfo{SessionID: sid, Namespace: testNamespace}, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	info, err := verifySessionToken(testSecret, defaultToken, now, time.Minute, time.Minute, testNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if info.Role != RoleClient {
		t.Fatalf("default role = %q, want client", info.Role)
	}

	if _, err := signSessionToken(testSecret, SessionInfo{SessionID: sid, Role: "admin"}, now, time.Minute); err == nil {
		t.Fatal("signSessionToken accepted an unsupported role")
	}
}

func TestClientRoleNormalization(t *testing.T) {
	srv, httpClient, _, _ := newH2Server(t, http.NotFoundHandler())
	client, err := NewClient(testProfile(DownlinkStream), ClientConfig{
		BaseURL:    srv.URL,
		Secret:     testSecret,
		Namespace:  testNamespace,
		UserID:     "alice",
		HTTPClient: httpClient,
		Logger:     testLogger{},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if got := client.SessionInfo().Role; got != RoleClient {
		t.Fatalf("default client role = %q", got)
	}
	if _, err := NewClient(testProfile(DownlinkStream), ClientConfig{
		BaseURL:    srv.URL,
		Secret:     testSecret,
		Namespace:  testNamespace,
		HTTPClient: httpClient,
		Logger:     testLogger{},
		Role:       "admin",
	}); err == nil {
		t.Fatal("NewClient accepted an unsupported role")
	}
}
