package raw_ws

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

func TestStalledSessionReconnectsQuickly(t *testing.T) {
	var accepted atomic.Int32
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted.Add(1)
		<-done
		_ = conn.Close()
	}))
	t.Cleanup(func() {
		close(done)
		srv.Close()
	})

	tr := newTestTransport(srv.URL, false)
	tr.pingInterval = 20 * time.Millisecond
	tr.pingDeadline = 20 * time.Millisecond
	tr.readIdleTimeout = 80 * time.Millisecond
	tr.reconnectDelay = 10 * time.Millisecond
	cancel := startTransport(t, tr)
	defer cancel()
	defer stopWithTimeout(t, tr)

	recvCtx, recvCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer recvCancel()

	start := time.Now()
	_, err := tr.Receive(recvCtx)
	if !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("expected ErrTransportReset after a stalled session, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("stalled session detection took too long: %s", elapsed)
	}
	if got := accepted.Load(); got < 2 {
		t.Fatalf("transport did not reconnect, accepted connections: %d", got)
	}
}

func TestReconnectBackoffGrowsAndIsCapped(t *testing.T) {
	base := 10 * time.Millisecond
	delay := nextReconnectDelay(nextReconnectDelay(base, base), base)
	if delay <= base {
		t.Fatalf("backoff did not grow: %s", delay)
	}
	for i := 0; i < 20; i++ {
		delay = nextReconnectDelay(delay, base)
	}
	if delay != reconnectMaxDelay {
		t.Fatalf("backoff did not cap at %s: %s", reconnectMaxDelay, delay)
	}
	if got := nextReconnectDelay(base, base); got != 2*base {
		t.Fatalf("backoff step = %s, want %s", got, 2*base)
	}
	if got := nextReconnectDelay(0, base); got <= base {
		t.Fatalf("backoff from zero = %s, want more than %s", got, base)
	}
}

func TestReconnectJitterIsBounded(t *testing.T) {
	for _, d := range []time.Duration{time.Millisecond, 100 * time.Millisecond, 5 * time.Second} {
		got := nextBackoffDelay(d)
		if got < d || got > d+d/4 {
			t.Fatalf("jittered delay %s out of bounds for %s", got, d)
		}
	}
}
