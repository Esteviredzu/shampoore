package flowmux

import (
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

func setTubeHealth(sf *subflow, throughput int64, rtt time.Duration) {
	sf.throughput.Store(throughput)
	sf.healthMu.Lock()
	sf.medianRTT = rtt
	sf.healthMu.Unlock()
}

func resetLoad(m *FlowMux) {
	for _, sf := range m.subflows {
		sf.active.Store(0)
		sf.inflight.Store(0)
	}
}

func primeMux(m *FlowMux) {
	m.pickCount.Add(1)
	m.firstOpenAt.Store(time.Now().Add(-time.Hour).UnixNano())
}

func pickShares(m *FlowMux, picks int) map[*subflow]int {
	counts := make(map[*subflow]int, len(m.subflows))
	for i := 0; i < picks; i++ {
		sf := m.pickSubflow()
		if sf == nil {
			return nil
		}
		counts[sf]++

		sf.active.Add(1)
	}
	return counts
}

func TestFlowMuxBalancesFastTubes(t *testing.T) {
	m := testFlowMux(t,
		[]transport.Transport{fakeTransport{}, fakeTransport{}, fakeTransport{}},
		[]string{"jellyfin", "raw_ws", "livekit"})
	for _, sf := range m.subflows {
		setTubeHealth(sf, 4_000_000, 40*time.Millisecond)
	}
	primeMux(m)

	const picks = 120
	shares := pickShares(m, picks)
	if shares == nil {
		t.Fatal("pickSubflow returned nil")
	}
	for sf, count := range shares {
		share := float64(count) / float64(picks)
		if share < 0.2 || share > 0.45 {
			t.Fatalf("tube %s got %.0f%% of %d streams: the fast tubes are not balanced", sf.name, share*100, picks)
		}
	}
	if len(shares) != 3 {
		t.Fatalf("only %d tubes received streams, want 3", len(shares))
	}
}

func TestFlowMuxSkewedBandwidthStillSpreads(t *testing.T) {
	m := testFlowMux(t,
		[]transport.Transport{fakeTransport{}, fakeTransport{}, fakeTransport{}},
		[]string{"jellyfin", "raw_ws", "livekit"})
	setTubeHealth(m.subflows[0], 50_000_000, 40*time.Millisecond)
	setTubeHealth(m.subflows[1], 5_000_000, 40*time.Millisecond)
	setTubeHealth(m.subflows[2], 5_000_000, 40*time.Millisecond)
	primeMux(m)

	const picks = 90
	shares := pickShares(m, picks)
	if shares == nil {
		t.Fatal("pickSubflow returned nil")
	}
	for sf, count := range shares {
		share := float64(count) / float64(picks)
		if share < 0.15 {
			t.Fatalf("tube %s got only %.0f%% of the streams: the measurement skew is back", sf.name, share*100)
		}
		if sf == m.subflows[0] && share > 0.6 {
			t.Fatalf("the fastest tube collected %.0f%% of the streams", share*100)
		}
	}
}

func TestFlowMuxBusyTubeIsSkipped(t *testing.T) {
	m := testFlowMux(t,
		[]transport.Transport{fakeTransport{}, fakeTransport{}, fakeTransport{}},
		[]string{"busy", "warm", "idle"})
	for _, sf := range m.subflows {
		setTubeHealth(sf, 4_000_000, 40*time.Millisecond)
	}
	resetLoad(m)
	primeMux(m)
	m.subflows[0].active.Store(8)
	m.subflows[1].active.Store(1)

	for i := 0; i < 20; i++ {
		sf := m.pickSubflow()
		if sf == m.subflows[0] {
			t.Fatalf("pick %d: the overloaded tube was chosen while an idle sibling exists", i)
		}
	}

	shares := pickShares(m, 60)
	if shares == nil {
		t.Fatal("pickSubflow returned nil")
	}
	if busy := shares[m.subflows[0]]; float64(busy)/60 > 0.4 {
		t.Fatalf("the overloaded tube still got %d of 60 streams", busy)
	}
}

func TestFlowMuxKeepsSlowTubeInReserve(t *testing.T) {
	m := testFlowMux(t,
		[]transport.Transport{fakeTransport{}, fakeTransport{}, fakeTransport{}},
		[]string{"jellyfin", "raw_ws", "silo"})
	setTubeHealth(m.subflows[0], 4_000_000, 40*time.Millisecond)
	setTubeHealth(m.subflows[1], 4_000_000, 60*time.Millisecond)
	setTubeHealth(m.subflows[2], 8_000_000, 400*time.Millisecond)
	primeMux(m)

	shares := pickShares(m, 90)
	if shares == nil {
		t.Fatal("pickSubflow returned nil")
	}
	if shares[m.subflows[2]] != 0 {
		t.Fatalf("the slow silo tube received %d streams instead of staying in reserve", shares[m.subflows[2]])
	}
}
