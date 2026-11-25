package raw_ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestTransportRestartsWithSingleWarmup(t *testing.T) {
	var warmupHits atomic.Int32
	var accepted atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/ws") {
			up := websocket.Upgrader{}
			conn, err := up.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			accepted.Add(1)
			defer conn.Close()
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}
		warmupHits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	tr := newTestTransport(srv.URL, true)

	tr.warmupPaths = []string{"/"}

	waitActive := func(want int32) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for {
			tr.connMu.Lock()
			active := tr.active
			tr.connMu.Unlock()
			if active != nil && accepted.Load() >= want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("transport did not connect (accepted=%d)", accepted.Load())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitWarmup := func() int32 {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for warmupHits.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		return warmupHits.Load()
	}

	cancel := startTransport(t, tr)
	waitActive(1)
	if err := tr.Send(context.Background(), []byte("first")); err != nil {
		t.Fatalf("send on first run: %v", err)
	}

	firstWarmup := waitWarmup()
	if firstWarmup == 0 {
		t.Fatal("warmup did not run on the first dial")
	}
	cancel()
	stopWithTimeout(t, tr)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := tr.Start(ctx2); err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer stopWithTimeout(t, tr)
	waitActive(2)
	if err := tr.Send(context.Background(), []byte("second")); err != nil {
		t.Fatalf("send after restart: %v", err)
	}
	if got := warmupHits.Load(); got != firstWarmup {
		t.Fatalf("warmup ran again on reconnect: hits %d -> %d", firstWarmup, got)
	}
}
