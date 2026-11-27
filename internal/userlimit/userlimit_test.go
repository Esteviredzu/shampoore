package userlimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRegistryUnlimitedWaitsImmediately(t *testing.T) {
	reg := New(WithBurst(2048))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := reg.Wait(ctx, "alice", 128*1024); err != nil {
		t.Fatalf("unlimited wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("unlimited wait took %v", elapsed)
	}
	if got := reg.Limit("alice"); got != 0 {
		t.Fatalf("limit = %d, want 0", got)
	}
}

func TestRegistrySharesLimitAcrossConcurrentStreams(t *testing.T) {

	reg := New(WithBurst(1024))
	const bps = 32 * 1024
	reg.SetLimit("alice", bps)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const chunk = 1024
	const perStream = 10

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perStream; j++ {
				if err := reg.Wait(ctx, "alice", chunk); err != nil {
					t.Errorf("wait: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if elapsed < 400*time.Millisecond {
		t.Fatalf("two streams exceeded the shared limit: %v for %d bytes", elapsed, 2*perStream*chunk)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("shared limit far too slow: %v", elapsed)
	}
}

func TestRegistryLimitsArePerUser(t *testing.T) {
	reg := New(WithBurst(1024))
	reg.SetLimit("alice", 16*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	if err := reg.Wait(ctx, "bob", 32*1024); err != nil {
		t.Fatalf("bob wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("bob inherited alice's limit: %v", elapsed)
	}
}

func TestRegistryDynamicLimitUpdateWithoutReconnect(t *testing.T) {
	reg := New(WithBurst(1024))
	reg.SetLimit("alice", 1024*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := reg.Wait(ctx, "alice", 8192); err != nil {
		t.Fatalf("fast wait: %v", err)
	}

	reg.SetLimit("alice", 16*1024)
	if got := reg.Limit("alice"); got != 16*1024 {
		t.Fatalf("limit after update = %d, want %d", got, 16*1024)
	}
	start := time.Now()
	if err := reg.Wait(ctx, "alice", 8192); err != nil {
		t.Fatalf("slow wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("lowered limit was not applied: %v", elapsed)
	}

	reg.SetLimit("alice", 0)
	start = time.Now()
	if err := reg.Wait(ctx, "alice", 256*1024); err != nil {
		t.Fatalf("unlimited wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("limit reset was not applied: %v", elapsed)
	}
}

func TestRegistryReplaceLimits(t *testing.T) {
	reg := New()
	reg.SetLimit("alice", 1000)
	reg.SetLimit("bob", 2000)
	reg.ReplaceLimits(map[string]int64{"bob": 4096, "carol": 8192})

	if got := reg.Limit("alice"); got != 0 {
		t.Fatalf("alice limit = %d, want 0 after removal", got)
	}
	if got := reg.Limit("bob"); got != 4096 {
		t.Fatalf("bob limit = %d, want 4096", got)
	}
	if got := reg.Limit("carol"); got != 8192 {
		t.Fatalf("carol limit = %d, want 8192", got)
	}
}

func TestRegistryActiveUsers(t *testing.T) {
	reg := New()
	reg.Touch("alice")
	reg.Touch("bob")
	if got := reg.ActiveUsers(time.Minute); len(got) != 2 || got[0] != "alice" || got[1] != "bob" {
		t.Fatalf("active users = %v", got)
	}
	time.Sleep(5 * time.Millisecond)
	if got := reg.ActiveUsers(2 * time.Millisecond); len(got) != 0 {
		t.Fatalf("stale users still active: %v", got)
	}
	reg.Touch("alice")
	got := reg.ActiveUsers(2 * time.Millisecond)
	if len(got) != 1 || got[0] != "alice" {
		t.Fatalf("active users after touch = %v", got)
	}
}

func TestRegistryWaitHonoursContext(t *testing.T) {
	reg := New(WithBurst(1024))
	reg.SetLimit("alice", 1024)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = reg.Wait(ctx, "alice", 1024)
	if err := reg.Wait(ctx, "alice", 8*1024); err == nil {
		t.Fatal("wait on a cancelled context returned nil")
	}
}
