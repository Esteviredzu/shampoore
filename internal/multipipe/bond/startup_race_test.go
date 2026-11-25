package bond

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type lateTransport struct {
	transport.Transport
	ready atomic.Bool

	sendErrs atomic.Int32
}

func (t *lateTransport) Open() { t.ready.Store(true) }

func (t *lateTransport) Send(ctx context.Context, data []byte) error {
	if !t.ready.Load() {
		t.sendErrs.Add(1)
		return fmt.Errorf("%w: transport is still connecting", transport.ErrTransportReset)
	}
	return t.Transport.Send(ctx, data)
}

func (t *lateTransport) Receive(ctx context.Context) ([]byte, error) {
	if !t.ready.Load() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		return nil, fmt.Errorf("%w: transport is still connecting", transport.ErrTransportReset)
	}
	return t.Transport.Receive(ctx)
}

func TestBondSurvivesTransportThatIsStillConnecting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientEnd, serverEnd := newMemPair()
	late := &lateTransport{Transport: clientEnd}

	srv := NewServer([]string{"0-raw"}, nil, testLogger{}, tunnel.Config{
		FlushThreshold: 4096,
		FlushInterval:  2 * time.Millisecond,
	}, nil)
	srv.SetSessionHandler(readingEcho)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	release, _ := srv.register("0-raw", "alice", serverEnd)
	if release == nil {
		t.Fatal("tube was not accepted")
	}
	t.Cleanup(release)

	mux, err := NewMux([]transport.Transport{late}, []string{"0-raw"}, testLogger{})
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}
	mux.SetIdentity("alice", "")
	if err := mux.Start(ctx); err != nil {
		t.Fatalf("bond start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = mux.Stop(stopCtx)
	})

	conn := mux.session.acquire()
	if conn == nil {
		t.Fatal("bond produced no connection")
	}
	sessionID := conn.session

	go func() {
		time.Sleep(150 * time.Millisecond)
		late.Open()
	}()

	stream, err := mux.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream while the tube is still connecting: %v", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))

	payload := []byte("hello-bond")
	if _, err := stream.Write(payload); err != nil {
		t.Fatalf("write while connecting: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("echo after the tube connected: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("echo = %q, want %q", got, payload)
	}

	live := mux.session.acquire()
	if live == nil {
		t.Fatal("bond connection disappeared")
	}
	if err := live.terminalErr(); err != nil {
		t.Fatalf("bond session died while its only tube was connecting: %v", err)
	}
	if live.session != sessionID {
		t.Fatalf("bond recreated the session (%d -> %d): the startup chunk was not retried",
			sessionID, live.session)
	}
	if late.sendErrs.Load() == 0 {
		t.Fatal("test did not exercise a send before the transport was ready")
	}
}
