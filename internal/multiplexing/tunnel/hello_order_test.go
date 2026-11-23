package tunnel

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

func encodeBatch(t *testing.T, frames ...*TunnelFrame) []byte {
	t.Helper()

	var out []byte
	for _, f := range frames {
		if f.Epoch == 0 {
			f.Epoch = 1
		}
		encoded, err := EncodeFrame(f)
		if err != nil {
			t.Fatalf("encode frame: %v", err)
		}
		out = append(out, encoded...)
	}
	return out
}

func assertHelloBeforeOpen(t *testing.T, iter int, frames []*TunnelFrame) {
	t.Helper()

	hellos := make(map[uint32]bool)
	opens := 0
	for _, f := range frames {
		switch f.Type {
		case FrameHello:
			hellos[f.Epoch] = true
		case FrameOpen:
			opens++
			if !hellos[f.Epoch] {
				t.Fatalf("iteration %d: OPEN on epoch %d was sent before HELLO", iter, f.Epoch)
			}
		}
	}
	if opens == 0 {
		t.Fatalf("iteration %d: no OPEN frames were sent", iter)
	}
}

func TestOpenStreamWaitsForHelloWrite(t *testing.T) {
	tr := newFakeTransport()

	helloEntered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	tr.setSendFunc(func(ctx context.Context, data []byte) error {
		frames, err := NewFrameDecoder().Feed(data)
		if err != nil {
			return err
		}
		for _, f := range frames {
			if f.Type != FrameHello {
				continue
			}
			enterOnce.Do(func() { close(helloEntered) })
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			break
		}

		tr.mu.Lock()
		tr.sent = append(tr.sent, append([]byte(nil), data...))
		tr.mu.Unlock()
		return nil
	})

	cfg := testConfig()
	cfg.UserID = "alice"
	cfg.ExitNodeID = "exit-1"
	m := New(tr, testLogger{}, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	started := make(chan error, 1)
	go func() { started <- m.Start(ctx) }()

	select {
	case <-helloEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("HELLO frame was never handed to the transport")
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	if err := m.awaitHello(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		waitCancel()
		t.Fatalf("awaitHello returned %v while HELLO was still in flight", err)
	}
	waitCancel()

	opened := make(chan error, 1)
	go func() {
		stream, err := m.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
		if err == nil {
			_ = stream.Close()
		}
		opened <- err
	}()

	select {
	case err := <-opened:
		t.Fatalf("OpenStream finished before HELLO reached the transport: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	if err := <-started; err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := <-opened; err != nil {
		t.Fatalf("open stream: %v", err)
	}

	frames := tr.sentFrames(t)
	helloIdx, openIdx := -1, -1
	for i, f := range frames {
		if f.Type == FrameHello && helloIdx < 0 {
			helloIdx = i
		}
		if f.Type == FrameOpen && openIdx < 0 {
			openIdx = i
		}
	}
	if helloIdx < 0 {
		t.Fatal("HELLO frame was not sent")
	}
	if openIdx < 0 {
		t.Fatal("OPEN frame was not sent")
	}
	if helloIdx > openIdx {
		t.Fatalf("OPEN frame #%d was sent before HELLO frame #%d", openIdx, helloIdx)
	}
	if frames[helloIdx].Epoch != frames[openIdx].Epoch {
		t.Fatalf("HELLO epoch %d != OPEN epoch %d",
			frames[helloIdx].Epoch, frames[openIdx].Epoch)
	}

	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestOpenStreamConcurrentStartNeverOvertakesHello(t *testing.T) {
	for iter := 0; iter < 10; iter++ {
		tr := newFakeTransport()
		cfg := testConfig()
		cfg.UserID = "alice"
		cfg.ExitNodeID = "exit-1"
		cfg.FlushInterval = time.Millisecond
		m := New(tr, testLogger{}, cfg)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		started := make(chan error, 1)
		go func() { started <- m.Start(ctx) }()

		var wg sync.WaitGroup
		openErrs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				deadline := time.Now().Add(5 * time.Second)
				for {
					stream, err := m.OpenStream(ctx, map[string]any{"probe": i})
					if err == nil {
						_ = stream.Close()
						return
					}

					if !strings.Contains(err.Error(), "not started") {
						openErrs <- err
						return
					}
					if time.Now().After(deadline) {
						openErrs <- err
						return
					}
					time.Sleep(time.Millisecond)
				}
			}(i)
		}

		if err := <-started; err != nil {
			t.Fatalf("iteration %d: start: %v", iter, err)
		}
		wg.Wait()
		close(openErrs)
		for err := range openErrs {
			t.Fatalf("iteration %d: open stream: %v", iter, err)
		}
		assertHelloBeforeOpen(t, iter, tr.sentFrames(t))

		tr.clearSent()
		tr.errCh <- transport.ErrTransportReset
		waitForFrame(t, tr, 2*time.Second, func(f *TunnelFrame) bool {
			return f.Type == FrameReset
		})

		stream, err := m.OpenStream(ctx, map[string]any{"after_reset": true})
		if err != nil {
			t.Fatalf("iteration %d: open after reset: %v", iter, err)
		}
		streamID := streamIDOf(t, stream)
		waitForFrame(t, tr, 2*time.Second, func(f *TunnelFrame) bool {
			return f.Type == FrameOpen && f.StreamID == streamID
		})
		_ = stream.Close()
		assertHelloBeforeOpen(t, iter, tr.sentFrames(t))

		cancel()
		if err := m.Stop(context.Background()); err != nil {
			t.Fatalf("iteration %d: stop: %v", iter, err)
		}
	}
}

func TestHelloHandshakeAuthorizesServerBeforeFirstOpen(t *testing.T) {
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{UserID: "alice", Enabled: true}); err != nil {
		t.Fatal(err)
	}

	clientTr, serverTr := linkedPair()

	serverCfg := linkedConfig()
	serverCfg.Registry = registry
	serverErrs := make(chan error, 1)
	serverCfg.OnSessionError = func(err error) {
		select {
		case serverErrs <- err:
		default:
		}
	}

	clientCfg := linkedConfig()
	clientCfg.UserID = "alice"
	clientErrs := make(chan error, 1)
	clientCfg.OnSessionError = func(err error) {
		select {
		case clientErrs <- err:
		default:
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := New(serverTr, testLogger{}, serverCfg)
	if err := server.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop(context.Background()) }()

	client := New(clientTr, testLogger{}, clientCfg)
	started := make(chan error, 1)
	go func() { started <- client.Start(ctx) }()

	accepted := make(chan error, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			accepted <- err
			return
		}
		defer stream.Close()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(stream, buf); err != nil {
			accepted <- err
			return
		}
		_, err = stream.Write(buf)
		accepted <- err
	}()

	var (
		stream  multiplexing.Stream
		openErr error
	)
	deadline := time.Now().Add(2 * time.Second)
	for {
		stream, openErr = client.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
		if openErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("open stream: %v", openErr)
		}
		time.Sleep(time.Millisecond)
	}
	defer stream.Close()

	if err := <-started; err != nil {
		t.Fatalf("client start: %v", err)
	}

	if _, err := stream.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(stream, reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if string(reply) != "ping" {
		t.Fatalf("reply = %q, want ping", reply)
	}
	if err := <-accepted; err != nil {
		t.Fatalf("server handler: %v", err)
	}

	select {
	case err := <-clientErrs:
		t.Fatalf("client session error: %v", err)
	case err := <-serverErrs:
		t.Fatalf("server session error: %v", err)
	default:
	}
}
func TestBatchHelloPingOpenAuthorizesStream(t *testing.T) {
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{UserID: "alice", Enabled: true}); err != nil {
		t.Fatal(err)
	}

	tr := newFakeTransport()
	m := New(tr, testLogger{}, registryConfig(t, registry))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	streamID := StreamID(uuid.New())
	tr.recvCh <- encodeBatch(t,
		&TunnelFrame{Type: FrameHello, Hello: &Hello{UserID: "alice", ExitNodeID: "exit-1"}},
		&TunnelFrame{Type: FramePing},
		&TunnelFrame{
			StreamID: streamID,
			Type:     FrameOpen,
			Metadata: map[string]any{"host": "example.com", "port": float64(443)},
		},
	)

	acceptCtx, acceptCancel := context.WithTimeout(ctx, 2*time.Second)
	defer acceptCancel()
	stream, err := m.AcceptStream(acceptCtx)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if stream.ID() != uuid.UUID(streamID).String() {
		t.Fatalf("unexpected stream id: %s", stream.ID())
	}
	if user, exit := m.SessionIdentity(); user != "alice" || exit != "exit-1" {
		t.Fatalf("session identity = %q/%q", user, exit)
	}

	waitForFrame(t, tr, time.Second, func(f *TunnelFrame) bool { return f.Type == FramePong })
	for _, f := range tr.sentFrames(t) {
		if f.Type == FrameError && f.Error != nil && f.Error.Code == "hello_required" {
			t.Fatal("batch [HELLO, PING, OPEN] was rejected with hello_required")
		}
	}
}

func TestOpenBeforeHelloUsesConfiguredIdentity(t *testing.T) {
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{UserID: "alice", Enabled: true}); err != nil {
		t.Fatal(err)
	}

	tr := newFakeTransport()
	cfg := registryConfig(t, registry)

	cfg.UserID = "alice"
	cfg.ExitNodeID = "exit-1"
	m := New(tr, testLogger{}, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	streamID := StreamID(uuid.New())
	sendFrameToMux(t, tr, &TunnelFrame{
		StreamID: streamID,
		Type:     FrameOpen,
		Metadata: map[string]any{"host": "example.com", "port": float64(443)},
	})

	acceptCtx, acceptCancel := context.WithTimeout(ctx, 2*time.Second)
	defer acceptCancel()
	stream, err := m.AcceptStream(acceptCtx)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if stream.ID() != uuid.UUID(streamID).String() {
		t.Fatalf("unexpected stream id: %s", stream.ID())
	}
	if user, exit := m.SessionIdentity(); user != "alice" || exit != "exit-1" {
		t.Fatalf("session identity = %q/%q", user, exit)
	}

	for _, f := range tr.sentFrames(t) {
		if f.Type == FrameError {
			t.Fatalf("unexpected error frame: %+v", f.Error)
		}
	}
}

func TestPinnedServerIdentityDoesNotSendHello(t *testing.T) {
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{UserID: "alice", Enabled: true}); err != nil {
		t.Fatal(err)
	}

	tr := newFakeTransport()
	cfg := registryConfig(t, registry)

	cfg.UserID = "alice"
	cfg.ExitNodeID = "42"
	m := New(tr, testLogger{}, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	streamID := StreamID(uuid.New())
	sendFrameToMux(t, tr, &TunnelFrame{
		StreamID: streamID,
		Type:     FrameOpen,
		Metadata: map[string]any{"host": "example.com", "port": float64(443)},
	})

	acceptCtx, acceptCancel := context.WithTimeout(ctx, 2*time.Second)
	defer acceptCancel()
	stream, err := m.AcceptStream(acceptCtx)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer stream.Close()

	if user, exit := m.SessionIdentity(); user != "alice" || exit != "42" {
		t.Fatalf("session identity = %q/%q", user, exit)
	}
	for _, f := range tr.sentFrames(t) {
		if f.Type == FrameHello {
			t.Fatal("a mux with a registry announced itself with HELLO")
		}
		if f.Type == FrameError {
			t.Fatalf("unexpected error frame: %+v", f.Error)
		}
	}
}
