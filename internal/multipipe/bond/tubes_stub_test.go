package bond

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type captureLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *captureLogger) add(args ...any) {
	l.mu.Lock()
	l.msgs = append(l.msgs, fmt.Sprint(args...))
	l.mu.Unlock()
}

func (l *captureLogger) Trace(a ...any)                           { l.add(a...) }
func (l *captureLogger) Debug(a ...any)                           { l.add(a...) }
func (l *captureLogger) Info(a ...any)                            { l.add(a...) }
func (l *captureLogger) Warn(a ...any)                            { l.add(a...) }
func (l *captureLogger) Error(a ...any)                           { l.add(a...) }
func (l *captureLogger) Fatal(a ...any)                           { l.add(a...) }
func (l *captureLogger) Panic(a ...any)                           { l.add(a...) }
func (l *captureLogger) TraceContext(_ context.Context, a ...any) { l.add(a...) }
func (l *captureLogger) DebugContext(_ context.Context, a ...any) { l.add(a...) }
func (l *captureLogger) InfoContext(_ context.Context, a ...any)  { l.add(a...) }
func (l *captureLogger) WarnContext(_ context.Context, a ...any)  { l.add(a...) }
func (l *captureLogger) ErrorContext(_ context.Context, a ...any) { l.add(a...) }
func (l *captureLogger) FatalContext(_ context.Context, a ...any) { l.add(a...) }
func (l *captureLogger) PanicContext(_ context.Context, a ...any) { l.add(a...) }

func (l *captureLogger) contains(substr string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, msg := range l.msgs {
		if strings.Contains(msg, substr) {
			return msg, true
		}
	}
	return "", false
}

func (l *captureLogger) dump() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.msgs, "\n")
}

type stubTube struct {
	out chan []byte
	in  chan []byte

	sent  atomic.Int64
	recvd atomic.Int64
}

func newStubPair() (*stubTube, *stubTube) {
	a2b := make(chan []byte, 256)
	b2a := make(chan []byte, 256)
	return &stubTube{out: a2b, in: b2a}, &stubTube{out: b2a, in: a2b}
}

func (s *stubTube) Start(context.Context) error { return nil }
func (s *stubTube) Stop(context.Context) error  { return nil }

func (s *stubTube) Send(ctx context.Context, data []byte) error {
	cp := append([]byte(nil), data...)
	select {
	case s.out <- cp:
		s.sent.Add(int64(len(cp)))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *stubTube) Receive(ctx context.Context) ([]byte, error) {
	select {
	case data := <-s.in:
		s.recvd.Add(int64(len(data)))
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *stubTube) MaxMessageSize() int   { return 12000 }
func (s *stubTube) BytesDelivered() int64 { return s.recvd.Load() }
func (s *stubTube) BytesAcked() int64     { return s.sent.Load() }

func (s *stubTube) WalkLayers() []transport.Layer {
	return []transport.Layer{{
		Kind: "pipe", Type: "livekit",
		Snapshot: transport.Snapshot{
			NetworkSent: s.sent.Load(),
			NetworkRecv: s.recvd.Load(),
			Timestamp:   time.Now(),
		},
	}}
}

type stubSessionServer struct {
	mu      sync.Mutex
	userID  string
	tube    *stubTube
	sink    app.SessionTransportSink
	started bool
	release func()
}

func newStubSessionServer(userID string, tube *stubTube) *stubSessionServer {
	return &stubSessionServer{userID: userID, tube: tube}
}

func (s *stubSessionServer) SetSessionHandler(app.SessionHandler) {}

func (s *stubSessionServer) SetTransportSink(sink app.SessionTransportSink) {
	s.mu.Lock()
	s.sink = sink
	s.mu.Unlock()
}

func (s *stubSessionServer) Start(context.Context) error {
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
	return nil
}

func (s *stubSessionServer) Stop(context.Context) error {
	s.mu.Lock()
	release := s.release
	s.release = nil
	s.mu.Unlock()
	if release != nil {
		release()
	}
	return nil
}

func (s *stubSessionServer) register() bool {
	s.mu.Lock()
	sink := s.sink
	started := s.started
	s.mu.Unlock()
	if !started || sink == nil {
		return false
	}
	release, _ := sink(s.userID, s.tube)
	if release == nil {
		return false
	}
	s.mu.Lock()
	s.release = release
	s.mu.Unlock()
	return true
}
