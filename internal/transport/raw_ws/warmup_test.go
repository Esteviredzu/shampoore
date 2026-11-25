package raw_ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWarmupDoesNotDelayTheStreamDial(t *testing.T) {
	var warmupHits atomic.Int32
	wsReady := make(chan struct{})
	var wsOnce sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			up := websocket.Upgrader{}
			conn, err := up.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			wsOnce.Do(func() { close(wsReady) })
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					break
				}
			}
			_ = conn.Close()
			return
		}
		warmupHits.Add(1)
		select {
		case <-wsReady:
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		_, _ = w.Write([]byte("<html></html>"))
	}))
	t.Cleanup(srv.Close)

	tr := newTestTransport(srv.URL, false)
	ctx, cancel := context.WithCancel(context.Background())
	if err := tr.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = tr.Stop(context.Background())
	})

	select {
	case <-wsReady:
	case <-time.After(2 * time.Second):
		t.Fatal("the stream WebSocket was not dialled while the decoy warmup was still running")
	}

	deadline := time.Now().Add(5 * time.Second)
	for warmupHits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if warmupHits.Load() == 0 {
		t.Fatal("the decoy warmup never ran: the mimicry profile was dropped")
	}
}
