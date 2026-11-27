package panel

import (
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

func speedTestStart() time.Time {
	return time.Unix(1_700_000_000, 0)
}

func TestUserSpeedTrackerComputesBytesPerSecond(t *testing.T) {
	tracker := newUserSpeedTracker()
	start := speedTestStart()

	tracker.Observe(1, []panelstore.TrafficDelta{{UserID: "user-a", DownloadBytes: 4096}}, start)
	if got := tracker.Speed("user-a", start); got != 0 {
		t.Fatalf("speed after the first heartbeat = %d, want 0", got)
	}

	tracker.Observe(1, []panelstore.TrafficDelta{{
		UserID:        "user-a",
		UploadBytes:   512,
		DownloadBytes: 1536,
	}}, start.Add(2*time.Second))
	if got := tracker.Speed("user-a", start.Add(2*time.Second)); got != 1024 {
		t.Fatalf("speed = %d, want 1024", got)
	}

	fresh := start.Add(2*time.Second + userSpeedSampleTTL - time.Second)
	if got := tracker.Speed("user-a", fresh); got != 1024 {
		t.Fatalf("speed before the TTL = %d, want 1024", got)
	}

	stale := start.Add(2*time.Second + userSpeedSampleTTL + time.Second)
	if got := tracker.Speed("user-a", stale); got != 0 {
		t.Fatalf("speed after the TTL = %d, want 0", got)
	}
}

func TestUserSpeedTrackerSumsNodesAndAggregatesDeltas(t *testing.T) {
	tracker := newUserSpeedTracker()
	start := speedTestStart()
	second := start.Add(2 * time.Second)

	tracker.Observe(1, nil, start)
	tracker.Observe(2, nil, start)

	tracker.Observe(1, []panelstore.TrafficDelta{{UserID: "user-a", DownloadBytes: 1024}}, second)

	tracker.Observe(2, []panelstore.TrafficDelta{
		{UserID: "user-a", UploadBytes: 2048},
		{UserID: "user-a", DownloadBytes: 2048},
	}, second)

	if got := tracker.Speed("user-a", second); got != 512+2048 {
		t.Fatalf("summed speed = %d, want %d", got, 512+2048)
	}

	tracker.Observe(2, []panelstore.TrafficDelta{{UserID: "user-b", UploadBytes: 2048}}, start.Add(4*time.Second))
	if got := tracker.Speed("user-b", start.Add(4*time.Second)); got != 1024 {
		t.Fatalf("user-b speed = %d, want 1024", got)
	}
	if got := tracker.Speed("user-a", start.Add(4*time.Second)); got != 512+2048 {
		t.Fatalf("user-a speed after user-b traffic = %d, want %d", got, 512+2048)
	}
}

func TestUserSpeedTrackerResetsAfterIdleHeartbeats(t *testing.T) {
	tracker := newUserSpeedTracker()
	start := speedTestStart()

	tracker.Observe(1, nil, start)
	tracker.Observe(1, []panelstore.TrafficDelta{{UserID: "user-a", DownloadBytes: 5120}}, start.Add(2*time.Second))
	if got := tracker.Speed("user-a", start.Add(2*time.Second)); got != 2560 {
		t.Fatalf("speed before idle = %d, want 2560", got)
	}

	for offset := 4 * time.Second; offset <= userSpeedSampleTTL+5*time.Second; offset += 2 * time.Second {
		tracker.Observe(1, nil, start.Add(offset))
	}
	idleAt := start.Add(userSpeedSampleTTL + 5*time.Second)
	if got := tracker.Speed("user-a", idleAt); got != 0 {
		t.Fatalf("speed after idle = %d, want 0", got)
	}
}

func TestUserSpeedTrackerConcurrentAccess(t *testing.T) {
	tracker := newUserSpeedTracker()
	now := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			nodeID := int64(worker%2 + 1)
			for j := 0; j < 100; j++ {
				tracker.Observe(nodeID, []panelstore.TrafficDelta{{UserID: "user-a", DownloadBytes: 1024}}, now)
				_ = tracker.Speed("user-a", now)
			}
		}(i)
	}
	wg.Wait()
}
