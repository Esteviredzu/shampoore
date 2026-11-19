package kcp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

type fakePipe struct {
	mu       sync.Mutex
	started  bool
	stopped  bool
	sendErr  error
	sendHook func(data []byte, key string) error
	sent     int
	recvCh   chan []byte
}

func newFakePipe() *fakePipe {
	return &fakePipe{recvCh: make(chan []byte, 64)}
}

func (p *fakePipe) Start(context.Context) error {
	p.mu.Lock()
	p.started = true
	p.mu.Unlock()
	return nil
}

func (p *fakePipe) Stop(context.Context) error {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
	return nil
}

func (p *fakePipe) SendMessage(_ context.Context, data []byte, key string) error {
	p.mu.Lock()
	hook := p.sendHook
	err := p.sendErr
	p.mu.Unlock()
	if hook != nil {
		if hookErr := hook(data, key); hookErr != nil {
			return hookErr
		}
	}
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.sent++
	p.mu.Unlock()
	return nil
}

func (p *fakePipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case data := <-p.recvCh:
		return data, nil
	}
}

func (p *fakePipe) DeleteMessage(context.Context, string) error { return nil }
func (p *fakePipe) SessionID() string                           { return "fake" }
func (p *fakePipe) MaxMessageSize() int                         { return 1200 }

func (p *fakePipe) setSendError(err error) {
	p.mu.Lock()
	p.sendErr = err
	p.mu.Unlock()
}

func (p *fakePipe) setSendHook(fn func(data []byte, key string) error) {
	p.mu.Lock()
	p.sendHook = fn
	p.mu.Unlock()
}

func (p *fakePipe) sentCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent
}

func TestOutputQueueBoundedWithoutBusyLoop(t *testing.T) {
	pipe := newFakePipe()
	pipe.setSendError(errors.New("network down"))

	s, err := NewSession(pipe, testLogger{}, DefaultConfig())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	payload := []byte("0123456789")
	start := time.Now()
	iterations := maxPendingOutput*2 + 32
	for i := 0; i < iterations; i++ {
		s.output(payload, len(payload))
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("output took too long (%v), looks like a busy loop", elapsed)
	}

	s.outMu.Lock()
	queued := len(s.pendingOutput)
	pendingBytes := s.pendingBytes
	s.outMu.Unlock()

	if queued > maxPendingOutput {
		t.Fatalf("pendingOutput grew to %d, limit is %d", queued, maxPendingOutput)
	}
	if pendingBytes > maxPendingOutputBytes {
		t.Fatalf("pendingBytes grew to %d, limit is %d", pendingBytes, maxPendingOutputBytes)
	}
	if queued == 0 {
		t.Fatal("expected chunks to be queued for a failing pipe")
	}
	if got := pipe.sentCount(); got != 0 {
		t.Fatalf("output performed network I/O: %d sends", got)
	}
}

func TestFlushPendingOutputRequeuesOnError(t *testing.T) {
	pipe := newFakePipe()
	pipe.setSendError(errors.New("network down"))

	s, err := NewSession(pipe, testLogger{}, DefaultConfig())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	const chunks = 10
	payload := []byte("chunk-payload")
	for i := 0; i < chunks; i++ {
		s.output(payload, len(payload))
	}

	start := time.Now()
	for i := 0; i < 1000; i++ {
		s.flushPendingOutput()
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("flush loop took too long (%v), looks like a busy loop", elapsed)
	}
	if got := pipe.sentCount(); got != 0 {
		t.Fatalf("unexpected successful sends: %d", got)
	}

	s.outMu.Lock()
	queued := len(s.pendingOutput)
	s.outMu.Unlock()
	if queued != chunks {
		t.Fatalf("pendingOutput = %d, want %d after failed flush", queued, chunks)
	}

	pipe.setSendError(nil)
	s.flushPendingOutput()

	if got := pipe.sentCount(); got != chunks {
		t.Fatalf("sent = %d, want %d after recovery", got, chunks)
	}
	s.outMu.Lock()
	queued = len(s.pendingOutput)
	s.outMu.Unlock()
	if queued != 0 {
		t.Fatalf("pendingOutput = %d, want 0 after successful flush", queued)
	}
}

func TestOutputDoesNotSendUnderKCPMu(t *testing.T) {
	pipe := newFakePipe()
	s, err := NewSession(pipe, testLogger{}, DefaultConfig())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	var violated atomic.Bool
	var sends atomic.Int32
	pipe.setSendHook(func([]byte, string) error {
		sends.Add(1)
		if !s.kcpMu.TryLock() {
			violated.Store(true)
			return nil
		}
		s.kcpMu.Unlock()
		return nil
	})

	payload := make([]byte, 64)
	s.kcpMu.Lock()
	s.output(payload, len(payload))
	s.kcpMu.Unlock()

	if violated.Load() {
		t.Fatal("network I/O happened while kcpMu was held")
	}
	if sends.Load() != 0 {
		t.Fatal("output() performed network I/O")
	}

	s.flushPendingOutput()

	if violated.Load() {
		t.Fatal("flushPendingOutput performed I/O while kcpMu was held")
	}
	if sends.Load() == 0 {
		t.Fatal("flushPendingOutput did not send queued chunks")
	}
}

func TestConcurrentStartStop(t *testing.T) {
	pipe := newFakePipe()
	s, err := NewSession(pipe, testLogger{}, DefaultConfig())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(start bool) {
			defer wg.Done()
			if start {
				_ = s.Start(ctx)
				return
			}
			_ = s.Stop(ctx)
		}(i%2 == 0)
	}
	wg.Wait()

	if err := s.Stop(ctx); err != nil {
		t.Fatalf("final Stop: %v", err)
	}
}
