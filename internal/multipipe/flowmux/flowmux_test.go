package flowmux

import (
	"context"
	"sync"
	"testing"
	"time"

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

type fakeTransport struct{}

func (fakeTransport) Start(context.Context) error { return nil }
func (fakeTransport) Stop(context.Context) error  { return nil }
func (fakeTransport) Send(context.Context, []byte) error {
	return nil
}
func (fakeTransport) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (fakeTransport) MaxMessageSize() int   { return 1 << 20 }
func (fakeTransport) BytesDelivered() int64 { return 0 }
func (fakeTransport) BytesAcked() int64     { return 0 }

func TestWalkLayersCacheTTL(t *testing.T) {
	m, err := New(
		[]transport.Transport{fakeTransport{}},
		[]string{"test"},
		testLogger{},
		Config{
			FlushThreshold: 4096,
			FlushInterval:  5 * time.Millisecond,
			SampleInterval: 10 * time.Millisecond,
			StatsTTL:       100 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	first := m.WalkLayers()
	if len(first) == 0 {
		t.Fatal("WalkLayers returned no layers")
	}
	second := m.WalkLayers()
	if &first[0] != &second[0] {
		t.Fatal("WalkLayers rebuilt layers within the cache TTL")
	}

	time.Sleep(200 * time.Millisecond)
	third := m.WalkLayers()
	if len(third) == 0 {
		t.Fatal("WalkLayers returned no layers after TTL")
	}
	if &first[0] == &third[0] {
		t.Fatal("WalkLayers did not refresh layers after the cache TTL")
	}
}
func TestConcurrentStartStop(t *testing.T) {

	m, err := New(
		[]transport.Transport{fakeTransport{}},
		[]string{"test"},
		testLogger{},
		Config{
			FlushThreshold: 4096,
			FlushInterval:  5 * time.Millisecond,
			SampleInterval: 10 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

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

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := m.Stop(stopCtx); err != nil {
		t.Fatalf("final stop: %v", err)
	}
}
