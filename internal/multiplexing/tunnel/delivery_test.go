package tunnel

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

func TestOpenStreamFailsWhenSendFails(t *testing.T) {
	tr := newFakeTransport()
	tr.setSendFunc(func(context.Context, []byte) error { return errors.New("boom") })

	m := New(tr, testLogger{}, testConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	start := time.Now()
	if _, err := m.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443}); err == nil {
		t.Fatal("OpenStream succeeded although the OPEN frame could not be sent")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("OpenStream took %s: it must fail fast", elapsed)
	}
	m.mu.Lock()
	live := len(m.streams)
	m.mu.Unlock()
	if live != 0 {
		t.Fatalf("%d ghost streams left in the multiplexer", live)
	}
}

func TestStreamReadDeadlineUnblocks(t *testing.T) {
	s := newTunnelStream(nil, StreamID(uuid.New()), nil)
	if err := s.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	start := time.Now()
	if _, err := s.Read(make([]byte, 8)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read error = %v, want os.ErrDeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("read returned after %s: deadline was not enforced", elapsed)
	}
}

func TestStreamDeadlineClearedAfterData(t *testing.T) {
	s := newTunnelStream(nil, StreamID(uuid.New()), nil)
	if err := s.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		s.deliverData([]byte("hi"))
	}()

	buf := make([]byte, 2)
	if _, err := s.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "hi" {
		t.Fatalf("payload = %q", buf)
	}
}

func TestFailSessionNotifiesAndFailsFast(t *testing.T) {
	tr := newFakeTransport()
	notified := make(chan error, 1)
	cfg := testConfig()
	cfg.OnSessionError = func(err error) {
		select {
		case notified <- err:
		default:
		}
	}

	m := New(tr, testLogger{}, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	tr.errCh <- errors.New("transport died")

	select {
	case <-notified:
	case <-time.After(2 * time.Second):
		t.Fatal("OnSessionError was not called for a dead transport")
	}

	start := time.Now()
	if _, err := m.OpenStream(context.Background(), map[string]any{"host": "h", "port": 1}); !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("OpenStream after session failure = %v, want ErrTransportReset", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("OpenStream took %s after a dead session", elapsed)
	}
}
