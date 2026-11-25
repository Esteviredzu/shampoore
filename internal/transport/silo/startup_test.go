package silo

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

type getCountingStore struct {
	*fakeStore
	gets atomic.Int32
}

func (s *getCountingStore) GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	s.gets.Add(1)
	return s.fakeStore.GetObject(ctx, bucket, object, opts)
}

func TestSiloStartSkipsBucketHistory(t *testing.T) {
	store := newFakeStore("bucket")
	cfg := testPipeConfig()
	cfg.FlushMs = 5

	_, readPrefix := buildPrefixes(&cfg, "server")
	staleBatch := appendBatchMessage(nil, []byte("stale"))
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("%smsg_%020d_1", readPrefix, i)

		store.putObject(key, staleBatch, time.Now().Add(-2*time.Hour))
	}

	counting := &getCountingStore{fakeStore: store}
	pipe, err := newPipeWithStore(&cfg, "server", testLogger{}, counting)
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}

	start := time.Now()
	if err := pipe.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = pipe.Stop(stopCtx)
	}()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("pipe start took %s, want < 1s", elapsed)
	}

	time.Sleep(300 * time.Millisecond)
	if got := counting.gets.Load(); got != 0 {
		t.Fatalf("bucket history was downloaded on start-up: %d GetObject calls", got)
	}

	fresh := appendBatchMessage(nil, []byte("hello"))
	key := fmt.Sprintf("%smsg_%d_7", readPrefix, time.Now().UnixNano())
	store.putObject(key, fresh, time.Now())

	recvCtx, recvCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer recvCancel()
	msg, err := pipe.ReceiveMessage(recvCtx)
	if err != nil {
		t.Fatalf("receive fresh object: %v", err)
	}
	if string(msg) != "hello" {
		t.Fatalf("fresh message = %q, want hello", msg)
	}

	waitUntil(t, 3*time.Second, "stale history cleanup", func() bool {
		return countPrefix(store, readPrefix) == 0
	})
}

func TestSiloStartIgnoresClockSkew(t *testing.T) {
	const skew = 5 * time.Minute

	for _, tc := range []struct {
		name    string
		history time.Duration
		fresh   time.Duration
	}{
		{name: "client behind", history: -skew, fresh: -skew},
		{name: "client ahead", history: skew, fresh: skew},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore("bucket")
			cfg := testPipeConfig()
			cfg.FlushMs = 5

			_, readPrefix := buildPrefixes(&cfg, "server")
			historyBatch := appendBatchMessage(nil, []byte("history"))
			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("%smsg_%020d_1", readPrefix, i)
				store.putObject(key, historyBatch, time.Now().Add(tc.history))
			}

			counting := &getCountingStore{fakeStore: store}
			pipe, err := newPipeWithStore(&cfg, "server", testLogger{}, counting)
			if err != nil {
				t.Fatalf("new pipe: %v", err)
			}
			if err := pipe.Start(context.Background()); err != nil {
				t.Fatalf("start: %v", err)
			}
			defer func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = pipe.Stop(stopCtx)
			}()

			time.Sleep(200 * time.Millisecond)
			if got := counting.gets.Load(); got != 0 {
				t.Fatalf("clock skew caused history downloads: %d GetObject calls", got)
			}

			fresh := appendBatchMessage(nil, []byte("fresh-under-skew"))
			key := fmt.Sprintf("%smsg_%d_9", readPrefix, time.Now().UnixNano())
			store.putObject(key, fresh, time.Now().Add(tc.fresh))

			recvCtx, recvCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer recvCancel()
			msg, err := pipe.ReceiveMessage(recvCtx)
			if err != nil {
				t.Fatalf("fresh object was dropped because of clock skew: %v", err)
			}
			if string(msg) != "fresh-under-skew" {
				t.Fatalf("fresh message = %q", msg)
			}
		})
	}
}

func countPrefix(store *fakeStore, prefix string) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	n := 0
	for key := range store.objects {
		if strings.HasPrefix(key, prefix) {
			n++
		}
	}
	return n
}
