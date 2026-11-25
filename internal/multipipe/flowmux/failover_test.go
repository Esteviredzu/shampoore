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

type errStartTransport struct{ fakeTransport }

func (errStartTransport) Start(context.Context) error { return errors.New("subflow is down") }
func (errStartTransport) Stop(context.Context) error  { return nil }

func TestStartSkipsSubflowThatFailsToStart(t *testing.T) {
	m := testFlowMux(t,
		[]transport.Transport{errStartTransport{}, fakeTransport{}},
		[]string{"dead", "live"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start with one dead subflow: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	if len(m.subflows) != 1 || m.subflows[0].name != "live" {
		t.Fatalf("live subflows = %d, want only %q", len(m.subflows), "live")
	}
	stream, err := m.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream over the surviving subflow: %v", err)
	}
	_ = stream.Close()
}

func TestStartFailsWhenEverySubflowIsDead(t *testing.T) {
	m := testFlowMux(t,
		[]transport.Transport{errStartTransport{}, errStartTransport{}},
		[]string{"dead-1", "dead-2"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Start(ctx); err == nil {
		_ = m.Stop(context.Background())
		t.Fatal("start succeeded with no live subflow")
	}
	if m.started {
		t.Fatal("a failed start left the multiplexer marked as started")
	}
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

func TestLatencyAwareCandidatesNeverEmpty(t *testing.T) {
	m := testFlowMux(t,
		[]transport.Transport{fakeTransport{}, fakeTransport{}, fakeTransport{}},
		[]string{"fast", "slow", "cold"})

	setRTT := func(sf *subflow, rtt time.Duration) {
		sf.healthMu.Lock()
		sf.medianRTT = rtt
		sf.healthMu.Unlock()
	}

	setRTT(m.subflows[0], 400*time.Millisecond)
	setRTT(m.subflows[1], 500*time.Millisecond)
	setRTT(m.subflows[2], 900*time.Millisecond)
	if got := m.latencyAwareCandidates(m.candidates(time.Now().UnixNano())); len(got) == 0 {
		t.Fatal("latencyAwareCandidates returned no candidates with high RTT on every tube")
	}
	if sf := m.pickSubflow(); sf == nil {
		t.Fatal("pickSubflow returned nil while every tube is alive")
	}

	for _, sf := range m.subflows {
		setRTT(sf, 0)
	}
	if got := m.latencyAwareCandidates(m.candidates(time.Now().UnixNano())); len(got) != len(m.subflows) {
		t.Fatalf("latencyAwareCandidates = %d before any measurement, want %d", len(got), len(m.subflows))
	}
	if sf := m.pickSubflow(); sf == nil {
		t.Fatal("pickSubflow returned nil before the first RTT measurement")
	}

	if got := m.latencyAwareCandidates(nil); len(got) != len(m.subflows) {
		t.Fatalf("latencyAwareCandidates(nil) = %d, want %d", len(got), len(m.subflows))
	}

	setRTT(m.subflows[0], 20*time.Millisecond)
	setRTT(m.subflows[1], 800*time.Millisecond)
	setRTT(m.subflows[2], 0)
	got := m.latencyAwareCandidates(m.candidates(time.Now().UnixNano()))
	if len(got) == 0 {
		t.Fatal("latencyAwareCandidates returned an empty list with a fast tube")
	}
	for _, sf := range got {
		if sf == m.subflows[1] {
			t.Fatal("the slow tube was not filtered out")
		}
	}
	if got[0] != m.subflows[0] && got[len(got)-1] != m.subflows[0] {
		t.Fatal("the fast tube is missing from the filtered candidates")
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
