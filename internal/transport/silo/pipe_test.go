package silo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func TestNextPollDelayBacksOff(t *testing.T) {
	base := 10 * time.Millisecond
	max := 500 * time.Millisecond
	want := []time.Duration{
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		160 * time.Millisecond,
		320 * time.Millisecond,
		500 * time.Millisecond,
		500 * time.Millisecond,
	}

	cur := base
	for i, expected := range want {
		cur = nextPollDelay(cur, base, max)
		if cur != expected {
			t.Fatalf("step %d: delay = %s, want %s", i, cur, expected)
		}
	}
}

func TestNextPollDelayClampsBase(t *testing.T) {
	if got := nextPollDelay(0, 0, 0); got != 10*time.Millisecond {
		t.Fatalf("delay = %s, want 10ms", got)
	}
	if got := nextPollDelay(2*time.Second, time.Second, 500*time.Millisecond); got != time.Second {
		t.Fatalf("delay = %s, want base 1s", got)
	}
}

type creatingStore struct {
	*fakeStore
	created []string
	makeErr error
}

func (s *creatingStore) MakeBucket(_ context.Context, bucket string, _ minio.MakeBucketOptions) error {
	if s.makeErr != nil {
		return s.makeErr
	}
	s.created = append(s.created, bucket)
	return nil
}

func TestEnsureBucketCreatesMissingBucket(t *testing.T) {
	ctx := context.Background()
	store := &creatingStore{fakeStore: newFakeStore("existing")}

	if err := ensureBucket(ctx, store, "existing"); err != nil {
		t.Fatalf("existing bucket: %v", err)
	}
	if len(store.created) != 0 {
		t.Fatalf("existing bucket was created again: %v", store.created)
	}

	if err := ensureBucket(ctx, store, "fresh"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if len(store.created) != 1 || store.created[0] != "fresh" {
		t.Fatalf("created buckets = %v, want [fresh]", store.created)
	}

	store.makeErr = minio.ErrorResponse{Code: "BucketAlreadyOwnedByYou"}
	if err := ensureBucket(ctx, store, "raced"); err != nil {
		t.Fatalf("already owned bucket: %v", err)
	}

	store.makeErr = errors.New("access denied")
	if err := ensureBucket(ctx, store, "denied"); err == nil {
		t.Fatal("expected access denied error")
	}
}
