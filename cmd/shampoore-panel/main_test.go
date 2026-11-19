package main

import (
	"context"
	"errors"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/provisioner"
)

type fakeNodeCleaner struct {
	calls    []int64
	failErr  map[int64]bool
	failSoft map[int64]bool
}

func (f *fakeNodeCleaner) CleanupNode(_ context.Context, node panelstore.Node) (*provisioner.CleanupResult, error) {
	f.calls = append(f.calls, node.ID)
	if f.failErr[node.ID] {
		return &provisioner.CleanupResult{
			NodeID: node.ID,
			Steps:  []provisioner.Step{{Name: "ssh connect", Error: "unreachable"}},
		}, errors.New("unreachable")
	}
	if f.failSoft[node.ID] {
		return &provisioner.CleanupResult{NodeID: node.ID, Success: false}, nil
	}
	return &provisioner.CleanupResult{NodeID: node.ID, Success: true}, nil
}

func TestCleanupNodesContinuesAfterFailures(t *testing.T) {
	nodes := []panelstore.Node{
		{ID: 1, Name: "relay", SSHUser: "root", SSHHost: "203.0.113.1"},
		{ID: 2, Name: "exit", SSHUser: "root", SSHHost: "203.0.113.2"},
		{ID: 3, Name: "gone", SSHUser: "root", SSHHost: "203.0.113.3"},
	}
	cleaner := &fakeNodeCleaner{
		failErr:  map[int64]bool{2: true},
		failSoft: map[int64]bool{3: true},
	}

	ok, failed := cleanupNodes(context.Background(), nodes, cleaner, logging.New("panel-test", false))
	if ok != 1 || failed != 2 {
		t.Fatalf("ok=%d failed=%d, want ok=1 failed=2", ok, failed)
	}
	if len(cleaner.calls) != len(nodes) {
		t.Fatalf("cleanup called %d times, want %d (all nodes must be visited)", len(cleaner.calls), len(nodes))
	}
	for i, want := range []int64{1, 2, 3} {
		if cleaner.calls[i] != want {
			t.Fatalf("call %d = %d, want %d", i, cleaner.calls[i], want)
		}
	}
}

func TestCleanupNodesEmpty(t *testing.T) {
	cleaner := &fakeNodeCleaner{}
	ok, failed := cleanupNodes(context.Background(), nil, cleaner, logging.New("panel-test", false))
	if ok != 0 || failed != 0 {
		t.Fatalf("ok=%d failed=%d, want 0/0", ok, failed)
	}
	if len(cleaner.calls) != 0 {
		t.Fatalf("cleanup called %d times for an empty list", len(cleaner.calls))
	}
}
