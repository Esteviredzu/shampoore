package socks5

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/userlimit"
)

func TestRemoteHandlerSharesUserSpeedLimit(t *testing.T) {
	echoAddr, stopEcho := startEchoServer(t)
	defer stopEcho()
	host, port := echoTarget(t, echoAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reg := userlimit.New(userlimit.WithBurst(2048))
	reg.SetLimit("alice", 32*1024)
	handler := NewRemoteHandler(ctx, &identityTestMux{user: "alice"}, testLogger{}, 4096, true)
	handler.SetUserLimiter(reg)

	payload := bytes.Repeat([]byte("x"), 16*1024)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, server := net.Pipe()
			stream := &netPipeStream{
				conn: server,
				id:   "limited-stream",
				meta: map[string]any{"host": host, "port": float64(port)},
			}
			done := make(chan struct{})
			go func() {
				handler.handleStream(stream)
				close(done)
			}()

			writeErr := make(chan error, 1)
			go func() {
				_, err := client.Write(payload)
				writeErr <- err
			}()
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(client, got); err != nil {
				t.Errorf("read echo: %v", err)
			} else if !bytes.Equal(got, payload) {
				t.Errorf("echo mismatch: got %d bytes", len(got))
			}
			if err := <-writeErr; err != nil {
				t.Errorf("write payload: %v", err)
			}
			_ = client.Close()
			<-done
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if elapsed < 700*time.Millisecond {
		t.Fatalf("shared per-user limit was not enforced: %v for 2x16KiB round trips", elapsed)
	}
	if elapsed > 20*time.Second {
		t.Fatalf("shared per-user limit is far too slow: %v", elapsed)
	}
	if !userlimitActive(reg, "alice") {
		t.Fatal("traffic did not mark the user as active")
	}
}

func userlimitActive(reg *userlimit.Registry, userID string) bool {
	for _, id := range reg.ActiveUsers(userlimit.ActiveWindow) {
		if id == userID {
			return true
		}
	}
	return false
}
