package flowmux

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

type errSendTransport struct {
	err error
}

func (e errSendTransport) Start(context.Context) error        { return nil }
func (e errSendTransport) Stop(context.Context) error         { return nil }
func (e errSendTransport) Send(context.Context, []byte) error { return e.err }
func (e errSendTransport) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (e errSendTransport) MaxMessageSize() int   { return 1 << 20 }
func (e errSendTransport) BytesDelivered() int64 { return 0 }
func (e errSendTransport) BytesAcked() int64     { return 0 }

func testFlowMux(t *testing.T, transports []transport.Transport, names []string) *FlowMux {
	t.Helper()
	m, err := New(transports, names, testLogger{}, Config{
		FlushThreshold: 4096,
		FlushInterval:  2 * time.Millisecond,
		SampleInterval: time.Hour,
		StatsTTL:       time.Hour,
	})
	if err != nil {
		t.Fatalf("new flowmux: %v", err)
	}
	return m
}

func TestOpenStreamFailsOverToHealthySubflow(t *testing.T) {
	bad := errSendTransport{err: errors.New("subflow down")}
	good := fakeTransport{}
	m := testFlowMux(t, []transport.Transport{bad, good}, []string{"bad", "good"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	stream, err := m.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream did not fail over to the healthy subflow: %v", err)
	}
	_ = stream.Close()

	if m.subflows[0].failUntil.Load() == 0 {
		t.Fatal("broken subflow was not put into failure cooldown")
	}
}

func TestCandidatesSkipSubflowInCooldown(t *testing.T) {
	m := testFlowMux(t, []transport.Transport{fakeTransport{}, fakeTransport{}}, []string{"a", "b"})
	m.subflows[0].failUntil.Store(time.Now().Add(time.Minute).UnixNano())

	alive := m.candidates(time.Now().UnixNano())
	if len(alive) != 1 || alive[0] != m.subflows[1] {
		t.Fatalf("candidates = %d, want only the healthy subflow", len(alive))
	}

	m.subflows[1].failUntil.Store(time.Now().Add(time.Minute).UnixNano())
	if got := len(m.candidates(time.Now().UnixNano())); got != 2 {
		t.Fatalf("candidates = %d, want the full list when everything is cooling down", got)
	}
}

func TestNextScoreDecaysWhenIdle(t *testing.T) {
	if got := nextScore(1000, 0); got != 750 {
		t.Fatalf("idle score = %d, want 750", got)
	}
	if got := nextScore(1000, 1000); got != 1000 {
		t.Fatalf("steady score = %d, want 1000", got)
	}
	if got := nextScore(0, 500); got != 150 {
		t.Fatalf("ramp-up score = %d, want 150", got)
	}
}
