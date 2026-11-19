package transport

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testClock struct {
	mu sync.Mutex
	at time.Time
}

func newTestClock() *testClock {
	return &testClock{at: time.Unix(1_700_000_000, 0)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.at = c.at.Add(d)
	c.mu.Unlock()
}

func testStatsLayers(wire int64) []Layer {
	return []Layer{
		{
			Kind:     "mux",
			Type:     "test",
			Snapshot: Snapshot{UsefulSent: wire / 2, UsefulRecv: wire / 2},
		},
		{
			Kind:     "pipe",
			Type:     "test",
			Name:     "a",
			Snapshot: Snapshot{NetworkSent: wire, NetworkRecv: wire},
		},
	}
}

func TestStatsAggregatorTTLCache(t *testing.T) {
	clock := newTestClock()
	agg := NewStatsAggregatorWithTTL(time.Minute)
	agg.now = clock.Now

	first := agg.Update(testStatsLayers(100))
	if first.Overall.NetworkSent != 100 {
		t.Fatalf("first snapshot network sent = %d, want 100", first.Overall.NetworkSent)
	}

	cached := agg.Update(testStatsLayers(200))
	if cached.Overall.NetworkSent != first.Overall.NetworkSent {
		t.Fatalf("snapshot was rebuilt within TTL: got %d, want %d",
			cached.Overall.NetworkSent, first.Overall.NetworkSent)
	}

	clock.Advance(2 * time.Minute)
	fresh := agg.Update(testStatsLayers(200))
	if fresh.Overall.NetworkSent != 200 {
		t.Fatalf("snapshot was not refreshed after TTL: got %d, want 200", fresh.Overall.NetworkSent)
	}

	agg.Invalidate()
	forced := agg.Update(testStatsLayers(300))
	if forced.Overall.NetworkSent != 300 {
		t.Fatalf("Invalidate did not drop the cached snapshot: got %d, want 300", forced.Overall.NetworkSent)
	}
}

func TestStatsAggregatorUpdateJSONReusesPayload(t *testing.T) {
	agg := NewStatsAggregatorWithTTL(time.Minute)

	_, first, err := agg.UpdateJSON(testStatsLayers(100))
	if err != nil {
		t.Fatalf("first UpdateJSON: %v", err)
	}
	_, second, err := agg.UpdateJSON(testStatsLayers(200))
	if err != nil {
		t.Fatalf("second UpdateJSON: %v", err)
	}
	if len(first) == 0 || len(second) == 0 {
		t.Fatal("UpdateJSON returned an empty payload")
	}
	if &first[0] != &second[0] {
		t.Fatal("UpdateJSON re-encoded the cached snapshot instead of reusing []byte")
	}

	var decoded AggregatedStats
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatalf("unmarshal cached payload: %v", err)
	}
	if decoded.Overall.NetworkSent != 100 {
		t.Fatalf("cached payload network sent = %d, want 100", decoded.Overall.NetworkSent)
	}
}

func TestJSONCacheTTL(t *testing.T) {
	clock := newTestClock()
	cache := NewJSONCache(time.Minute)
	cache.now = clock.Now

	var builds atomic.Int32
	build := func() ([]byte, error) {
		builds.Add(1)
		return []byte("payload"), nil
	}

	first, err := cache.Get(build)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	second, err := cache.Get(build)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if &first[0] != &second[0] {
		t.Fatal("JSONCache did not reuse the cached payload within TTL")
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("build calls within TTL = %d, want 1", got)
	}

	clock.Advance(2 * time.Minute)
	if _, err := cache.Get(build); err != nil {
		t.Fatalf("Get after TTL: %v", err)
	}
	if got := builds.Load(); got != 2 {
		t.Fatalf("build calls after TTL = %d, want 2", got)
	}

	cache.Invalidate()
	if _, err := cache.Get(build); err != nil {
		t.Fatalf("Get after Invalidate: %v", err)
	}
	if got := builds.Load(); got != 3 {
		t.Fatalf("build calls after Invalidate = %d, want 3", got)
	}
}

func TestJSONCacheDoesNotCacheErrors(t *testing.T) {
	cache := NewJSONCache(time.Minute)

	var builds atomic.Int32
	sentinel := errors.New("build failed")
	build := func() ([]byte, error) {
		builds.Add(1)
		return nil, sentinel
	}

	for i := 0; i < 3; i++ {
		if _, err := cache.Get(build); !errors.Is(err, sentinel) {
			t.Fatalf("Get error = %v, want %v", err, sentinel)
		}
	}
	if got := builds.Load(); got != 3 {
		t.Fatalf("failed builds = %d, want 3 (errors must not be cached)", got)
	}
}

func TestStatsCachesConcurrent(t *testing.T) {
	agg := NewStatsAggregatorWithTTL(2 * time.Millisecond)
	cache := NewJSONCache(2 * time.Millisecond)

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				layers := testStatsLayers(int64(worker*100 + j + 1))
				stats, payload, err := agg.UpdateJSON(layers)
				if err != nil {
					t.Errorf("worker %d UpdateJSON: %v", worker, err)
					return
				}
				var decoded AggregatedStats
				if err := json.Unmarshal(payload, &decoded); err != nil {
					t.Errorf("worker %d unmarshal: %v", worker, err)
					return
				}
				if stats.Overall.NetworkSent != decoded.Overall.NetworkSent {
					t.Errorf("worker %d payload mismatch: %d != %d",
						worker, stats.Overall.NetworkSent, decoded.Overall.NetworkSent)
					return
				}

				data, err := cache.Get(func() ([]byte, error) {
					return json.Marshal(stats.Overall)
				})
				if err != nil || len(data) == 0 {
					t.Errorf("worker %d JSONCache.Get: len=%d err=%v", worker, len(data), err)
					return
				}

				if j%10 == 0 {
					agg.Invalidate()
					cache.Invalidate()
				}
			}
		}(worker)
	}
	wg.Wait()
}
