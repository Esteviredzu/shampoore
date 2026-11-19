package raw_ws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

type testLogger struct{}

func (testLogger) Trace(...any)                         {}
func (testLogger) Debug(...any)                         {}
func (testLogger) Info(...any)                          {}
func (testLogger) Warn(...any)                          {}
func (testLogger) Error(...any)                         {}
func (testLogger) Fatal(...any)                         {}
func (testLogger) Panic(...any)                         {}
func (testLogger) TraceContext(context.Context, ...any) {}
func (testLogger) DebugContext(context.Context, ...any) {}
func (testLogger) InfoContext(context.Context, ...any)  {}
func (testLogger) WarnContext(context.Context, ...any)  {}
func (testLogger) ErrorContext(context.Context, ...any) {}
func (testLogger) FatalContext(context.Context, ...any) {}
func (testLogger) PanicContext(context.Context, ...any) {}

func testConfig(serverURL string, batch bool) Config {
	return Config{
		ServerURL:       serverURL,
		Room:            "room-1",
		Password:        "secret",
		Batch:           batch,
		AllowInsecureWS: true,
	}
}

func newTestTransport(serverURL string, batch bool) *wsTransport {
	tr := newTransport(testConfig(serverURL, batch), testLogger{})
	tr.reconnectDelay = 20 * time.Millisecond
	return tr
}

func stopWithTimeout(t *testing.T, tr *wsTransport) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- tr.Stop(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return: transport worker is wedged")
	}
}

func startTransport(t *testing.T, tr *wsTransport) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if err := tr.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start: %v", err)
	}
	return cancel
}

func TestReconnectSignalsTransportReset(t *testing.T) {
	for _, batch := range []bool{false, true} {
		batch := batch
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			var accepted atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				up := websocket.Upgrader{}
				conn, err := up.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				accepted.Add(1)

				_ = conn.Close()
			}))
			defer srv.Close()

			tr := newTestTransport(srv.URL, batch)
			cancel := startTransport(t, tr)
			defer cancel()
			defer stopWithTimeout(t, tr)

			recvCtx, recvCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer recvCancel()

			_, err := tr.Receive(recvCtx)
			if !errors.Is(err, transport.ErrTransportReset) {
				t.Fatalf("expected ErrTransportReset after reconnect, got %v", err)
			}
			if got := accepted.Load(); got < 2 {
				t.Fatalf("expected the transport to reconnect, accepted connections: %d", got)
			}
		})
	}
}

func TestSendFailsFastOnWriteError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	tr := newTransport(testConfig(srv.URL, false), testLogger{})
	defer func() { _ = tr.Stop(context.Background()) }()

	wsURL, err := buildWSURL(srv.URL, "room-1")
	if err != nil {
		t.Fatalf("build url: %v", err)
	}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	tr.connMu.Lock()
	tr.conn = conn
	tr.connMu.Unlock()

	_ = conn.Close()

	start := time.Now()
	err = tr.Send(context.Background(), []byte("payload"))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("send on a broken connection succeeded")
	}
	if elapsed > time.Second {
		t.Fatalf("send took %s: write error is still retried in a loop", elapsed)
	}
	if !strings.Contains(err.Error(), "write") {
		t.Fatalf("expected a write error, got %v", err)
	}

	tr.connMu.Lock()
	current := tr.conn
	tr.connMu.Unlock()
	if current != nil {
		t.Fatal("broken connection was not detached")
	}

	for i := 0; i < 3; i++ {
		start = time.Now()
		if err := tr.Send(context.Background(), []byte("payload")); err == nil {
			t.Fatalf("send %d succeeded without a connection", i)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("send %d took %s after the connection was dropped", i, elapsed)
		}
	}
}

func TestSendFailsFastWhenDisconnected(t *testing.T) {
	tr := newTransport(testConfig("ws://127.0.0.1:1", true), testLogger{})
	defer func() { _ = tr.Stop(context.Background()) }()

	start := time.Now()
	err := tr.Send(context.Background(), []byte("payload"))
	if err == nil {
		t.Fatal("send without a connection succeeded")
	}
	if !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("expected ErrTransportReset, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("send took %s while disconnected", elapsed)
	}
}
