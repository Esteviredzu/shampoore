package panelstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUserRouteOrderAndCascadeCleanup(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, store, "relay-order", "ws.order.example.com")
	exit := mkExit(t, store, "exit-order")
	routes := mkSingleRoutes(t, store, relay, exit)

	user, err := store.CreateUser(ctx, User{
		Name:            "ordered",
		Enabled:         true,
		AllowedRouteIDs: []int64{routes[2].ID, routes[0].ID, routes[1].ID},
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	want := []int64{routes[2].ID, routes[0].ID, routes[1].ID}
	if got := user.AllowedRouteIDs; len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("created order = %v, want %v", got, want)
	}

	user.AllowedRouteIDs = []int64{routes[1].ID, routes[2].ID}
	updated, err := store.UpdateUser(ctx, user)
	if err != nil {
		t.Fatalf("update user: %v", err)
	}
	if got := updated.AllowedRouteIDs; len(got) != 2 || got[0] != routes[1].ID || got[1] != routes[2].ID {
		t.Fatalf("updated order = %v", got)
	}

	if err := store.DeleteRoute(ctx, routes[2].ID); err != nil {
		t.Fatalf("delete route: %v", err)
	}
	afterDelete, err := store.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterDelete.AllowedRouteIDs) != 1 || afterDelete.AllowedRouteIDs[0] != routes[1].ID {
		t.Fatalf("routes after route delete = %v", afterDelete.AllowedRouteIDs)
	}

	if err := store.DeleteNode(ctx, exit.ID); err != nil {
		t.Fatalf("delete exit node: %v", err)
	}
	afterNode, err := store.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterNode.AllowedRouteIDs) != 0 {
		t.Fatalf("routes after node delete = %v", afterNode.AllowedRouteIDs)
	}
}

func TestUserRejectsUnknownRouteReferences(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, store, "relay-invalid", "ws.invalid.example.com")
	exit := mkExit(t, store, "exit-invalid")
	routes := mkSingleRoutes(t, store, relay, exit)

	if _, err := store.CreateUser(ctx, User{
		Name:            "dangling-create",
		Enabled:         true,
		AllowedRouteIDs: []int64{routes[0].ID, 99999},
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("create with unknown route = %v, want ErrInvalid", err)
	}

	user, err := store.CreateUser(ctx, User{Name: "valid", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	user.AllowedRouteIDs = []int64{99999}
	if _, err := store.UpdateUser(ctx, user); !errors.Is(err, ErrInvalid) {
		t.Fatalf("update with unknown route = %v, want ErrInvalid", err)
	}
}

func TestRecordNodeMetricsPreservedOnConfigUpdate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	node := mkRelay(t, store, "relay-metrics", "ws.metrics.example.com")

	before := time.Now().Add(-time.Second)
	metrics := NodeMetrics{
		CPUPercent:     42.5,
		MemUsedBytes:   512 << 20,
		MemTotalBytes:  1024 << 20,
		DiskUsedBytes:  8 << 30,
		DiskTotalBytes: 40 << 30,
	}
	if err := store.RecordNodeMetrics(ctx, node.ID, metrics); err != nil {
		t.Fatalf("record metrics: %v", err)
	}
	got, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.CPUPercent != metrics.CPUPercent || got.MemUsedBytes != metrics.MemUsedBytes ||
		got.DiskUsedBytes != metrics.DiskUsedBytes || got.DiskTotalBytes != metrics.DiskTotalBytes {
		t.Fatalf("stored metrics = %+v", got)
	}
	if got.LastSeen.Before(before) {
		t.Fatalf("last_seen was not updated: %v", got.LastSeen)
	}
	if got.Status != NodeStatusOnline {
		t.Fatalf("status after heartbeat = %q", got.Status)
	}

	node.Name = "relay-metrics-renamed"
	if _, err := store.UpdateNode(ctx, node); err != nil {
		t.Fatalf("update node: %v", err)
	}
	afterUpdate, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterUpdate.CPUPercent != metrics.CPUPercent || afterUpdate.MemTotalBytes != metrics.MemTotalBytes {
		t.Fatalf("configuration update wiped telemetry: %+v", afterUpdate)
	}
	if afterUpdate.LastSeen.IsZero() {
		t.Fatal("configuration update wiped last_seen")
	}
	if err := store.RecordNodeMetrics(ctx, node.ID, NodeMetrics{CPUPercent: 120}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid cpu = %v, want ErrInvalid", err)
	}
}
