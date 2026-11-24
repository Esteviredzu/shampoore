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

func holdWebSocket(t *testing.T, accepted *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if accepted != nil {
			accepted.Add(1)
		}
		defer conn.Close()
		for {

			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func waitForActiveSession(t *testing.T, tr *wsTransport) *wsSession {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if sess := tr.activeSession(); sess != nil {
			return sess
		}
		if time.Now().After(deadline) {
			t.Fatal("transport did not connect")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForAccepts(t *testing.T, accepted *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for accepted.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("transport did not reconnect, accepted connections: %d", accepted.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDetachedSessionUnblocksReceive(t *testing.T) {
	var accepted atomic.Int32
	srv := holdWebSocket(t, &accepted)

	tr := newTestTransport(srv.URL, false)
	tr.pingInterval = 20 * time.Millisecond
	tr.pingDeadline = 20 * time.Millisecond
	tr.readIdleTimeout = 200 * time.Millisecond
	tr.reconnectDelay = 10 * time.Millisecond
	cancel := startTransport(t, tr)
	defer cancel()
	defer stopWithTimeout(t, tr)

	sess := waitForActiveSession(t, tr)
	tr.failConnSession(sess.conn, errors.New("simulated write failure"))

	recvCtx, recvCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer recvCancel()

	start := time.Now()
	_, err := tr.Receive(recvCtx)
	if !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("Receive after a dead session = %v, want ErrTransportReset", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Receive stayed blocked for %s after the session died", elapsed)
	}
	waitForAccepts(t, &accepted, 2)
}

func TestForcedRotationWhileBusy(t *testing.T) {
	var accepted atomic.Int32
	srv := holdWebSocket(t, &accepted)

	tr := newTestTransport(srv.URL, false)
	tr.rotationMin = 40 * time.Millisecond
	tr.rotationMax = 60 * time.Millisecond
	tr.rotateIdle = time.Minute
	tr.readIdleTimeout = 100 * time.Millisecond
	tr.pingInterval = 20 * time.Millisecond
	tr.pingDeadline = 20 * time.Millisecond
	tr.reconnectDelay = 10 * time.Millisecond
	cancel := startTransport(t, tr)
	defer cancel()
	defer stopWithTimeout(t, tr)

	waitForActiveSession(t, tr)
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {

		_ = tr.Send(context.Background(), []byte("tick"))
		time.Sleep(10 * time.Millisecond)
	}
	if got := accepted.Load(); got < 2 {
		t.Fatalf("busy session was never rotated, accepted connections: %d", got)
	}
}

func TestSilentSessionDropsAndReconnects(t *testing.T) {
	var accepted atomic.Int32
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted.Add(1)
		defer conn.Close()
		if accepted.Load() > 1 {
			return
		}

		<-blocked
	}))
	t.Cleanup(func() {
		close(blocked)
		srv.Close()
	})

	tr := newTestTransport(srv.URL, false)
	tr.pingInterval = 10 * time.Millisecond
	tr.pingDeadline = 10 * time.Millisecond
	tr.readIdleTimeout = 80 * time.Millisecond
	tr.reconnectDelay = 10 * time.Millisecond
	cancel := startTransport(t, tr)
	defer cancel()
	defer stopWithTimeout(t, tr)

	waitForActiveSession(t, tr)
	recvCtx, recvCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer recvCancel()
	start := time.Now()
	_, err := tr.Receive(recvCtx)
	if !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("Receive on a silent socket = %v, want ErrTransportReset", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("silent socket detection took too long: %s", elapsed)
	}
	waitForAccepts(t, &accepted, 2)
}
