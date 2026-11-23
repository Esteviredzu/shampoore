package manager

import (
	"testing"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

type fakeTelemetry struct {
	layers []transport.Layer
}

func (f fakeTelemetry) WalkLayers() []transport.Layer { return f.layers }

func TestRefreshStatsUsesSessionTelemetry(t *testing.T) {
	me := &ManagedEntry{
		Name:     "multi",
		status:   StatusRunning,
		statsAgg: transport.NewStatsAggregator(),
		telemetry: fakeTelemetry{layers: []transport.Layer{{
			Kind: "mux",
			Type: "bond",
			Snapshot: transport.Snapshot{
				UsefulSent: 1000,
				UsefulRecv: 2000,
			},
		}}},
	}

	got := me.refreshStats()
	if got.Name != "multi" || got.Status != StatusRunning {
		t.Fatalf("stats = %+v, want name/status preserved", got)
	}
	if got.Overall.UsefulSent != 1000 || got.Overall.UsefulRecv != 2000 {
		t.Fatalf("overall useful bytes = %+v, want 1000/2000", got.Overall)
	}
}

func TestRefreshStatsWithoutTelemetryStaysEmpty(t *testing.T) {
	me := &ManagedEntry{
		Name:     "multi",
		status:   StatusRunning,
		statsAgg: transport.NewStatsAggregator(),
	}

	got := me.refreshStats()
	if got.Overall.UsefulSent != 0 || got.Overall.UsefulRecv != 0 {
		t.Fatalf("overall useful bytes = %+v, want zero", got.Overall)
	}
}
