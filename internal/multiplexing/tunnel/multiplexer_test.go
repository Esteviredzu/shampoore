package tunnel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
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

type fakeTransport struct {
	mu             sync.Mutex
	started        bool
	maxMessageSize int
	sendFn         func(ctx context.Context, data []byte) error
	sent           [][]byte

	recvCh chan []byte
	errCh  chan error
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		maxMessageSize: 1 << 20,
		recvCh:         make(chan []byte, 256),
		errCh:          make(chan error, 16),
	}
}

func (f *fakeTransport) Start(context.Context) error {
	f.mu.Lock()
	f.started = true
	f.mu.Unlock()
	return nil
}

func (f *fakeTransport) Stop(context.Context) error {
	f.mu.Lock()
	f.started = false
	f.mu.Unlock()
	return nil
}

func (f *fakeTransport) Send(ctx context.Context, data []byte) error {
	f.mu.Lock()
	fn := f.sendFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, data)
	}

	copied := append([]byte(nil), data...)
	f.mu.Lock()
	f.sent = append(f.sent, copied)
	f.mu.Unlock()
	return nil
}

func (f *fakeTransport) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-f.errCh:
		return nil, err
	case data := <-f.recvCh:
		return data, nil
	}
}

func (f *fakeTransport) MaxMessageSize() int   { return f.maxMessageSize }
func (f *fakeTransport) BytesDelivered() int64 { return 0 }
func (f *fakeTransport) BytesAcked() int64     { return 0 }

func (f *fakeTransport) setSendFunc(fn func(ctx context.Context, data []byte) error) {
	f.mu.Lock()
	f.sendFn = fn
	f.mu.Unlock()
}

func (f *fakeTransport) clearSent() {
	f.mu.Lock()
	f.sent = nil
	f.mu.Unlock()
}

func (f *fakeTransport) sentFrames(t *testing.T) []*TunnelFrame {
	t.Helper()

	f.mu.Lock()
	chunks := make([][]byte, len(f.sent))
	for i, chunk := range f.sent {
		chunks[i] = append([]byte(nil), chunk...)
	}
	f.mu.Unlock()

	decoder := NewFrameDecoder()
	var frames []*TunnelFrame
	for _, chunk := range chunks {
		decoded, err := decoder.Feed(chunk)
		if err != nil {
			t.Fatalf("decode sent data: %v", err)
		}
		frames = append(frames, decoded...)
	}
	return frames
}

func waitForFrame(
	t *testing.T,
	tr *fakeTransport,
	timeout time.Duration,
	match func(*TunnelFrame) bool,
) *TunnelFrame {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		for _, frame := range tr.sentFrames(t) {
			if match(frame) {
				return frame
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected frame was not sent within %s", timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func testConfig() Config {
	return Config{
		FlushThreshold: 1 << 20,
		FlushInterval:  5 * time.Millisecond,
	}
}

func streamIDOf(t *testing.T, s multiplexing.Stream) StreamID {
	t.Helper()
	ts, ok := s.(*TunnelStream)
	if !ok {
		t.Fatalf("unexpected stream type %T", s)
	}
	return ts.id
}

type acceptResult struct {
	stream multiplexing.Stream
	err    error
}

func TestTransportResetClosesStreamsAndRotatesEpoch(t *testing.T) {
	tr := newFakeTransport()
	m := New(tr, testLogger{}, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if err := m.Stop(context.Background()); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	stream, err := m.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	openID := streamIDOf(t, stream)

	open := waitForFrame(t, tr, time.Second, func(f *TunnelFrame) bool {
		return f.Type == FrameOpen && f.StreamID == openID
	})
	oldEpoch := open.Epoch

	tr.errCh <- transport.ErrTransportReset

	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := stream.Read(buf)
		readErr <- err
	}()

	select {
	case err := <-readErr:
		if !errors.Is(err, transport.ErrTransportReset) {
			t.Fatalf("expected ErrTransportReset, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream read did not unblock after transport reset")
	}

	reset := waitForFrame(t, tr, 2*time.Second, func(f *TunnelFrame) bool {
		return f.Type == FrameReset
	})
	if reset.Epoch == oldEpoch {
		t.Fatalf("epoch was not rotated: %d", reset.Epoch)
	}

	next, err := m.OpenStream(ctx, map[string]any{"host": "example.com", "port": 80})
	if err != nil {
		t.Fatalf("open stream after reset: %v", err)
	}
	nextID := streamIDOf(t, next)
	nextOpen := waitForFrame(t, tr, time.Second, func(f *TunnelFrame) bool {
		return f.Type == FrameOpen && f.StreamID == nextID
	})
	if nextOpen.Epoch != reset.Epoch {
		t.Fatalf("new stream epoch %d != reset epoch %d", nextOpen.Epoch, reset.Epoch)
	}
}

func TestRemoteEpochChangeClosesStreams(t *testing.T) {
	tr := newFakeTransport()
	m := New(tr, testLogger{}, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if err := m.Stop(context.Background()); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	accepted := make(chan acceptResult, 1)
	go func() {
		s, err := m.AcceptStream(ctx)
		accepted <- acceptResult{stream: s, err: err}
	}()

	streamID := StreamID(uuid.New())
	open, err := EncodeFrame(&TunnelFrame{
		StreamID: streamID,
		Type:     FrameOpen,
		Epoch:    100,
		Metadata: map[string]any{"host": "example.com", "port": float64(443)},
	})
	if err != nil {
		t.Fatalf("encode open: %v", err)
	}
	tr.recvCh <- open

	var result acceptResult
	select {
	case result = <-accepted:
		if result.err != nil {
			t.Fatalf("accept: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream was not accepted")
	}

	data, err := EncodeFrame(&TunnelFrame{
		StreamID: streamID,
		Type:     FrameData,
		Epoch:    200,
		Data:     []byte("new session"),
	})
	if err != nil {
		t.Fatalf("encode data: %v", err)
	}
	tr.recvCh <- data

	buf := make([]byte, 16)
	if _, err := result.stream.Read(buf); !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("expected ErrTransportReset, got %v", err)
	}

	staleID := StreamID(uuid.New())
	stale, err := EncodeFrame(&TunnelFrame{
		StreamID: staleID,
		Type:     FrameOpen,
		Epoch:    100,
		Metadata: map[string]any{"host": "stale.example"},
	})
	if err != nil {
		t.Fatalf("encode stale open: %v", err)
	}
	tr.recvCh <- stale

	staleCtx, staleCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer staleCancel()
	if _, err := m.AcceptStream(staleCtx); err == nil {
		t.Fatal("stale frame from retired epoch was accepted")
	}

	newUUID := uuid.New()
	newID := StreamID(newUUID)
	fresh, err := EncodeFrame(&TunnelFrame{
		StreamID: newID,
		Type:     FrameOpen,
		Epoch:    200,
		Metadata: map[string]any{"host": "fresh.example"},
	})
	if err != nil {
		t.Fatalf("encode fresh open: %v", err)
	}
	tr.recvCh <- fresh

	freshCtx, freshCancel := context.WithTimeout(ctx, 2*time.Second)
	defer freshCancel()
	freshStream, err := m.AcceptStream(freshCtx)
	if err != nil {
		t.Fatalf("accept fresh stream: %v", err)
	}
	if freshStream.ID() != newUUID.String() {
		t.Fatalf("unexpected fresh stream id: %s", freshStream.ID())
	}
}

func TestStreamCloseFlushesCloseFrame(t *testing.T) {
	tr := newFakeTransport()
	m := New(tr, testLogger{}, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if err := m.Stop(context.Background()); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	stream, err := m.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	streamID := streamIDOf(t, stream)
	open := waitForFrame(t, tr, time.Second, func(f *TunnelFrame) bool {
		return f.Type == FrameOpen && f.StreamID == streamID
	})

	tr.clearSent()
	if err := stream.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	closing := waitForFrame(t, tr, time.Second, func(f *TunnelFrame) bool {
		return f.Type == FrameClose && f.StreamID == streamID
	})
	if closing.Epoch != open.Epoch {
		t.Fatalf("close frame epoch %d != open epoch %d", closing.Epoch, open.Epoch)
	}
}

func TestStopFlushHonorsTimeout(t *testing.T) {
	tr := newFakeTransport()
	tr.setSendFunc(func(ctx context.Context, _ []byte) error {
		<-ctx.Done()
		return ctx.Err()
	})

	m := New(tr, testLogger{}, testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := m.enqueueFrame(m.ctx, &TunnelFrame{
		StreamID: StreamID(uuid.New()),
		Type:     FrameClose,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	start := time.Now()
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	elapsed := time.Since(start)

	limit := stopFlushTimeout + 2*time.Second
	if elapsed > limit {
		t.Fatalf("stop took %s, expected at most %s", elapsed, limit)
	}
}

func TestConcurrentStartStop(t *testing.T) {
	tr := newFakeTransport()
	m := New(tr, testLogger{}, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		if err := m.Start(ctx); err != nil {
			t.Errorf("start: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := m.Start(ctx); err != nil {
			t.Errorf("start: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := m.Stop(context.Background()); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()
	wg.Wait()

	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("final stop: %v", err)
	}
}
