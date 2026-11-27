package panel

import (
	"strings"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

const userSpeedSampleTTL = 15 * time.Second

type userSpeedSample struct {
	bps int64
	at  time.Time
}

type userSpeedTracker struct {
	mu    sync.Mutex
	users map[string]map[int64]userSpeedSample
	nodes map[int64]time.Time
}

func newUserSpeedTracker() *userSpeedTracker {
	return &userSpeedTracker{
		users: make(map[string]map[int64]userSpeedSample),
		nodes: make(map[int64]time.Time),
	}
}

func (t *userSpeedTracker) Observe(nodeID int64, traffic []panelstore.TrafficDelta, now time.Time) {
	if t == nil {
		return
	}
	bytesByUser := make(map[string]int64, len(traffic))
	for _, delta := range traffic {
		userID := strings.TrimSpace(delta.UserID)
		if userID == "" {
			continue
		}
		if normalized, err := auth.NormalizeUserID(userID); err == nil {
			userID = normalized
		}
		up, down := delta.UploadBytes, delta.DownloadBytes
		if up < 0 {
			up = 0
		}
		if down < 0 {
			down = 0
		}
		if up == 0 && down == 0 {
			continue
		}
		bytesByUser[userID] += up + down
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	previous, seen := t.nodes[nodeID]
	t.nodes[nodeID] = now
	t.pruneLocked(now)

	if !seen || len(bytesByUser) == 0 {
		return
	}
	elapsed := now.Sub(previous).Seconds()
	if elapsed <= 0 {
		return
	}
	for userID, bytes := range bytesByUser {
		byNode := t.users[userID]
		if byNode == nil {
			byNode = make(map[int64]userSpeedSample, 1)
			t.users[userID] = byNode
		}
		byNode[nodeID] = userSpeedSample{bps: int64(float64(bytes) / elapsed), at: now}
	}
}

func (t *userSpeedTracker) Speed(userID string, now time.Time) int64 {
	if t == nil || userID == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var total int64
	for _, sample := range t.users[userID] {
		if now.Sub(sample.at) > userSpeedSampleTTL {
			continue
		}
		total += sample.bps
	}
	return total
}

func (t *userSpeedTracker) pruneLocked(now time.Time) {
	for userID, byNode := range t.users {
		for nodeID, sample := range byNode {
			if now.Sub(sample.at) > userSpeedSampleTTL {
				delete(byNode, nodeID)
			}
		}
		if len(byNode) == 0 {
			delete(t.users, userID)
		}
	}
	for nodeID, last := range t.nodes {
		if now.Sub(last) > userSpeedSampleTTL {
			delete(t.nodes, nodeID)
		}
	}
}
