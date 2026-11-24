package httpstream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

func newWatchdogClient(t *testing.T, srv *httptest.Server, profile MimicProfile, idle time.Duration) *Client {
	t.Helper()
	cfg := ClientConfig{
		BaseURL:           srv.URL,
		Secret:            testSecret,
		Namespace:         testNamespace,
		UserID:            "alice",
		Logger:            testLogger{},
		AllowInsecureHTTP: true,
		MaxMessageSize:    32 << 10,
		AckInterval:       50 * time.Millisecond,
		ReadIdleTimeout:   idle,
		ReconnectDelay:    10 * time.Millisecond,
		ReconnectMax:      20 * time.Millisecond,
	}
	client, err := NewClient(profile, cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client
}

func waitForCondition(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForTransportReset(t *testing.T, client *Client, timeout time.Duration) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	_, err := client.Receive(ctx)
	if !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("Receive on a dead downlink = %v, want ErrTransportReset", err)
	}
	return time.Since(start)
}

func TestDownlinkIdleTimeoutReconnects(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		hits.Add(1)
		setStreamHeaders(w)
		w.WriteHeader(http.StatusOK)
		flushResponse(w)
		<-r.Context().Done()
	}))
	defer srv.Close()

	client := newWatchdogClient(t, srv, testProfile(DownlinkStream), 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startClient(t, client, ctx)

	if elapsed := waitForTransportReset(t, client, 3*time.Second); elapsed > 2*time.Second {
		t.Fatalf("idle downlink detection took %s", elapsed)
	}
	waitForCondition(t, "the downlink to be re-established", 3*time.Second, func() bool {
		return hits.Load() >= 2
	})
}

func TestDownlinkHeadersTimeoutReconnects(t *testing.T) {
	restore := requestTimeout
	requestTimeout = 150 * time.Millisecond
	t.Cleanup(func() { requestTimeout = restore })

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		hits.Add(1)
		<-r.Context().Done()
	}))
	defer srv.Close()

	client := newWatchdogClient(t, srv, testProfile(DownlinkStream), 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startClient(t, client, ctx)

	if elapsed := waitForTransportReset(t, client, 3*time.Second); elapsed > 2*time.Second {
		t.Fatalf("header timeout detection took %s", elapsed)
	}
	waitForCondition(t, "the download request to be retried", 3*time.Second, func() bool {
		return hits.Load() >= 2
	})
}

func TestChunkedPollSilenceReconnects(t *testing.T) {
	restore := requestTimeout
	requestTimeout = 120 * time.Millisecond
	t.Cleanup(func() { requestTimeout = restore })

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		hits.Add(1)
		setStreamHeaders(w)
		w.WriteHeader(http.StatusOK)
		flushResponse(w)
		<-r.Context().Done()
	}))
	defer srv.Close()

	client := newWatchdogClient(t, srv, testProfile(DownlinkChunkedPoll), 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startClient(t, client, ctx)

	if elapsed := waitForTransportReset(t, client, 3*time.Second); elapsed > 2*time.Second {
		t.Fatalf("poll silence detection took %s", elapsed)
	}
	waitForCondition(t, "the poll to be retried", 3*time.Second, func() bool {
		return hits.Load() >= 2
	})
}
