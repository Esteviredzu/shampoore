package transport

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

type rateLimitTestTransport struct {
	max  int
	mu   sync.Mutex
	sent int
}

func (t *rateLimitTestTransport) Start(context.Context) error { return nil }
func (t *rateLimitTestTransport) Stop(context.Context) error  { return nil }

func (t *rateLimitTestTransport) Send(_ context.Context, data []byte) error {
	t.mu.Lock()
	t.sent++
	t.mu.Unlock()
	return nil
}

func (t *rateLimitTestTransport) Receive(context.Context) ([]byte, error) {
	return nil, io.EOF
}

func (t *rateLimitTestTransport) MaxMessageSize() int { return t.max }
func (t *rateLimitTestTransport) BytesDelivered() int64 {
	return 0
}
func (t *rateLimitTestTransport) BytesAcked() int64 { return 0 }

func (t *rateLimitTestTransport) sentCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sent
}

func TestRateLimitedAllowsTransportMaxMessage(t *testing.T) {
	inner := &rateLimitTestTransport{max: 512 * 1024}
	limited, ok := NewRateLimited(inner, 10_000).(*rateLimited)
	if !ok {
		t.Fatal("NewRateLimited did not return a rateLimited transport")
	}
	if limited.burst < inner.max {
		t.Fatalf("burst = %d, want >= %d", limited.burst, inner.max)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, size := range []int{32 * 1024, 200 * 1024, 512 * 1024} {
		start := time.Now()
		if err := limited.Send(ctx, make([]byte, size)); err != nil {
			t.Fatalf("send %d bytes: %v", size, err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("send %d bytes took %s", size, elapsed)
		}
	}
	if got := inner.sentCount(); got != 3 {
		t.Fatalf("inner transport received %d messages, want 3", got)
	}
}

func TestRateLimitedSplitsOversizedMessage(t *testing.T) {
	inner := &rateLimitTestTransport{max: 16 * 1024}
	limited := NewRateLimited(inner, 1_024)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := limited.Send(ctx, make([]byte, 64*1024)); err != nil {
		t.Fatalf("oversized message was rejected: %v", err)
	}
	if got := inner.sentCount(); got != 1 {
		t.Fatalf("inner transport received %d messages, want 1", got)
	}
}
