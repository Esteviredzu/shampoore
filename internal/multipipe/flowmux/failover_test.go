package flowmux

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
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

func TestCompositeScoreResistsRTTSpikesAndPenalizesLoadAndResets(t *testing.T) {
	m := testFlowMux(t, []transport.Transport{fakeTransport{}, fakeTransport{}}, []string{"fast", "silo"})
	fast := m.subflows[0]
	silo := m.subflows[1]

	fast.throughput.Store(2_000_000)
	silo.throughput.Store(2_000_000)

	for i := 0; i < 4; i++ {
		fast.updateHealth(tunnel.HealthSnapshot{SRTT: 50 * time.Millisecond, RTTVAR: 10 * time.Millisecond}, 2*time.Second)
		silo.updateHealth(tunnel.HealthSnapshot{SRTT: 150 * time.Millisecond, RTTVAR: 20 * time.Millisecond}, 2*time.Second)
	}
	beforeSpike := silo.compositeScore(1)
	silo.updateHealth(tunnel.HealthSnapshot{SRTT: 3 * time.Second, RTTVAR: 500 * time.Millisecond}, 2*time.Second)
	afterSpike := silo.compositeScore(1)

	if afterSpike < beforeSpike*0.5 {
		t.Fatalf("single RTT spike collapsed score too aggressively: before=%.1f after=%.1f", beforeSpike, afterSpike)
	}

	fastUnloaded := fast.compositeScore(1)
	fast.active.Store(20)
	fast.updateHealth(tunnel.HealthSnapshot{SRTT: 50 * time.Millisecond, RTTVAR: 10 * time.Millisecond, QueuedBytes: 4_000_000}, 2*time.Second)
	fastLoaded := fast.compositeScore(5)
	if fastLoaded >= fastUnloaded*0.5 {
		t.Fatalf("heavy load did not penalize score enough: unloaded=%.1f loaded=%.1f", fastUnloaded, fastLoaded)
	}

	fast.active.Store(0)
	fast.updateHealth(tunnel.HealthSnapshot{SRTT: 50 * time.Millisecond, RTTVAR: 10 * time.Millisecond, QueuedBytes: 0, Resets: 0}, 2*time.Second)
	clean := fast.compositeScore(1)
	fast.updateHealth(tunnel.HealthSnapshot{SRTT: 50 * time.Millisecond, RTTVAR: 10 * time.Millisecond, Resets: 3}, 2*time.Second)
	flaky := fast.compositeScore(1)
	if flaky >= clean*0.3 {
		t.Fatalf("resets did not reduce score enough: clean=%.1f flaky=%.1f", clean, flaky)
	}

	fast.updateHealth(tunnel.HealthSnapshot{SRTT: 50 * time.Millisecond, RTTVAR: 10 * time.Millisecond, Resets: 3}, 90*time.Second)
	recovered := fast.compositeScore(1)
	if recovered <= flaky*2.0 {
		t.Fatalf("error penalty did not decay over time: flaky=%.1f recovered=%.1f", flaky, recovered)
	}
}
