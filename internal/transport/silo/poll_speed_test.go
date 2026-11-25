package silo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

type listTimingStore struct {
	*fakeStore
	mu    sync.Mutex
	times []time.Time
}

func (s *listTimingStore) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	s.mu.Lock()
	s.times = append(s.times, time.Now())
	s.mu.Unlock()
	return s.fakeStore.ListObjects(ctx, bucket, opts)
}

func (s *listTimingStore) snapshot() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Time, len(s.times))
	copy(out, s.times)
	return out
}

func TestSiloPollsImmediatelyAfterDataAndCapsIdlePoll(t *testing.T) {
	store := &listTimingStore{fakeStore: newFakeStore("bucket")}
	cfg := testPipeConfig()
	cfg.PollIntervalMs = 150
	cfg.FlushMs = 5

	pipe, err := newPipeWithStore(&cfg, "server", testLogger{}, store)
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := pipe.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopCancel()
		_ = pipe.Stop(stopCtx)
	})

	_, readPrefix := buildPrefixes(&cfg, "server")
	waitUntil(t, 3*time.Second, "initial bucket list", func() bool {
		return len(store.snapshot()) >= 1
	})

	key := readPrefix + "msg_0001_fast"
	store.putObject(key, appendBatchMessage(nil, []byte("hello")), time.Now())
	if got := receiveWithTimeout(t, pipe, 3*time.Second); string(got) != "hello" {
		t.Fatalf("delivered %q, want hello", got)
	}

	waitUntil(t, 200*time.Millisecond, "poll right after a delivery", func() bool {
		times := store.snapshot()
		if len(times) < 2 {
			return false
		}
		return times[len(times)-1].Sub(times[len(times)-2]) < 25*time.Millisecond
	})

	time.Sleep(600 * time.Millisecond)

	times := store.snapshot()
	if len(times) < 3 {
		t.Fatalf("poll loop made only %d list calls", len(times))
	}
	immediate := false
	var maxGap time.Duration
	for i := 1; i < len(times); i++ {
		gap := times[i].Sub(times[i-1])
		if gap < 25*time.Millisecond {
			immediate = true
		}
		if gap > maxGap {
			maxGap = gap
		}
	}
	if !immediate {
		t.Fatal("no poll ran immediately after data was delivered")
	}
	if maxGap > maxPollBackoff+60*time.Millisecond {
		t.Fatalf("longest idle poll gap = %s, want <= %s", maxGap, maxPollBackoff)
	}
}

func TestIdleRescanSkipsActiveWriter(t *testing.T) {
	store := newFakeStore("bucket")
	cfg := testPipeConfig()
	pipe, err := newPipeWithStore(&cfg, "server", testLogger{}, store)
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}
	pipe.headMu.Lock()
	pipe.lastKey = "msg_0001"
	pipe.headMu.Unlock()

	stale := time.Now().Add(-time.Hour)
	if !pipe.idleHead(stale) {
		t.Fatal("an idle pipe with a read cursor must rescan")
	}

	pipe.lastWrite.Store(time.Now().UnixNano())
	if pipe.idleHead(stale) {
		t.Fatal("idle rescan fired while the pipe was still writing")
	}

	pipe.lastWrite.Store(time.Now().Add(-idleRescanInterval - time.Second).UnixNano())
	if !pipe.idleHead(stale) {
		t.Fatal("idle rescan did not resume after the writer went quiet")
	}
}

func TestSiloWriteDoesNotWaitFlushInterval(t *testing.T) {
	store := newFakeStore("bucket")
	cfg := testPipeConfig()
	cfg.FlushMs = 250

	pipe, err := newPipeWithStore(&cfg, "client", testLogger{}, store)
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pipe.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopCancel()
		_ = pipe.Stop(stopCtx)
	})

	for i, payload := range []string{"first", "second", "third"} {
		start := time.Now()
		if err := pipe.SendMessage(ctx, []byte(payload), ""); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Fatalf("send %d took %s: a queued write waited for FlushMs", i, elapsed)
		}
	}
}
