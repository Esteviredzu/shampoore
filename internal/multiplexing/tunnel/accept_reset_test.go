package tunnel

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

func TestAcceptStreamReturnsWhenSessionDies(t *testing.T) {
	tr := newFakeTransport()
	m := New(tr, testLogger{}, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	accepted := make(chan error, 1)
	go func() {
		_, err := m.AcceptStream(context.Background())
		accepted <- err
	}()

	time.Sleep(20 * time.Millisecond)
	tr.errCh <- io.EOF

	select {
	case err := <-accepted:
		if !errors.Is(err, transport.ErrTransportReset) {
			t.Fatalf("AcceptStream error = %v, want ErrTransportReset", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AcceptStream blocked after the tunnel session died")
	}
}

func TestAcceptStreamReturnsEOFAfterStop(t *testing.T) {
	tr := newFakeTransport()
	m := New(tr, testLogger{}, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	accepted := make(chan error, 1)
	go func() {
		_, err := m.AcceptStream(context.Background())
		accepted <- err
	}()

	time.Sleep(20 * time.Millisecond)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := m.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("AcceptStream returned a stream after Stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AcceptStream blocked after the multiplexer stopped")
	}
}
