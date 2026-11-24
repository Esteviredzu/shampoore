package panelstore

import (
	"context"
	"testing"
	"time"
)

func TestRecordNodeMetricsUpdatesLastSeenButNotUpdatedAt(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	node := mkRelay(t, store, "relay-heartbeat", "ws.heartbeat.example.com")
	createdAt := node.UpdatedAt
	if createdAt.IsZero() {
		t.Fatal("created node has no updated_at")
	}

	time.Sleep(2 * time.Millisecond)
	if err := store.RecordNodeMetrics(ctx, node.ID, NodeMetrics{
		CPUPercent:     37,
		MemUsedBytes:   256 << 20,
		MemTotalBytes:  1024 << 20,
		DiskUsedBytes:  4 << 30,
		DiskTotalBytes: 32 << 30,
	}); err != nil {
		t.Fatalf("record metrics: %v", err)
	}
	afterMetrics, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !afterMetrics.UpdatedAt.Equal(createdAt) {
		t.Fatalf("heartbeat moved updated_at: %v -> %v", createdAt, afterMetrics.UpdatedAt)
	}
	if afterMetrics.LastSeen.IsZero() || !afterMetrics.LastSeen.After(createdAt) {
		t.Fatalf("heartbeat did not move last_seen: %v", afterMetrics.LastSeen)
	}
	if afterMetrics.Status != NodeStatusOnline {
		t.Fatalf("status after heartbeat = %q", afterMetrics.Status)
	}

	time.Sleep(2 * time.Millisecond)
	if err := store.TouchNode(ctx, node.ID); err != nil {
		t.Fatalf("touch node: %v", err)
	}
	afterTouch, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !afterTouch.UpdatedAt.Equal(createdAt) {
		t.Fatalf("touch moved updated_at: %v -> %v", createdAt, afterTouch.UpdatedAt)
	}
	if !afterTouch.LastSeen.After(afterMetrics.LastSeen) {
		t.Fatalf("touch did not move last_seen: %v", afterTouch.LastSeen)
	}

	if err := store.SetNodeStatus(ctx, node.ID, NodeStatusDeployed, ""); err != nil {
		t.Fatalf("set status: %v", err)
	}
	afterStatus, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !afterStatus.UpdatedAt.Equal(createdAt) {
		t.Fatalf("status write moved updated_at: %v -> %v", createdAt, afterStatus.UpdatedAt)
	}

	time.Sleep(2 * time.Millisecond)
	afterStatus.Name = "relay-heartbeat-renamed"
	if _, err := store.UpdateNode(ctx, afterStatus); err != nil {
		t.Fatalf("update node: %v", err)
	}
	afterUpdate, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !afterUpdate.UpdatedAt.After(createdAt) {
		t.Fatalf("config update did not move updated_at: %v", afterUpdate.UpdatedAt)
	}

	time.Sleep(2 * time.Millisecond)
	if err := store.RecordNodeDeploy(ctx, node.ID, NodeStatusDeployed, ""); err != nil {
		t.Fatalf("record deploy: %v", err)
	}
	afterDeploy, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !afterDeploy.UpdatedAt.After(afterUpdate.UpdatedAt) {
		t.Fatalf("deploy did not move updated_at: %v", afterDeploy.UpdatedAt)
	}
}
