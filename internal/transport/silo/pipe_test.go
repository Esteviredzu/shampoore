package silo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func appendBatchMessage(batch, msg []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(msg)))
	batch = append(batch, hdr[:]...)
	return append(batch, msg...)
}

func recvFromPipe(t *testing.T, pipe *Pipe) []byte {
	t.Helper()
	select {
	case data := <-pipe.recvCh:
		return data
	case <-time.After(time.Second):
		t.Fatal("no message was delivered")
		return nil
	}
}

func TestParseBatchPreservesMessageBoundaries(t *testing.T) {
	payloads := [][]byte{
		[]byte("a"),
		bytes.Repeat([]byte("b"), 7),
		bytes.Repeat([]byte("c"), 8),
		bytes.Repeat([]byte("d"), 100),
	}
	var batch []byte
	for _, payload := range payloads {
		batch = appendBatchMessage(batch, payload)
	}

	messages, err := parseBatch(batch, 1024)
	if err != nil {
		t.Fatalf("parse batch: %v", err)
	}
	if len(messages) != len(payloads) {
		t.Fatalf("parsed %d messages, want %d", len(messages), len(payloads))
	}
	for i := range payloads {
		if !bytes.Equal(messages[i], payloads[i]) {
			t.Fatalf("message %d = %d bytes, want %d", i, len(messages[i]), len(payloads[i]))
		}
	}
}

func TestParseBatchRejectsEmptyTruncatedAndOversized(t *testing.T) {
	if _, err := parseBatch(nil, 1024); !errors.Is(err, errBatchIncomplete) {
		t.Fatalf("empty object error = %v, want incomplete", err)
	}

	zeroLen := appendBatchMessage(nil, nil)
	if _, err := parseBatch(zeroLen, 1024); !errors.Is(err, errBatchCorrupt) {
		t.Fatalf("zero-length message error = %v, want corrupt", err)
	}

	truncated := []byte{0, 0, 0, 5, 'h', 'i'}
	if _, err := parseBatch(truncated, 1024); !errors.Is(err, errBatchIncomplete) {
		t.Fatalf("truncated message error = %v, want incomplete", err)
	}

	trailing := append(appendBatchMessage(nil, []byte("ok")), 0, 0)
	if _, err := parseBatch(trailing, 1024); !errors.Is(err, errBatchIncomplete) {
		t.Fatalf("trailing bytes error = %v, want incomplete", err)
	}

	oversized := appendBatchMessage(nil, bytes.Repeat([]byte("x"), 64))
	if _, err := parseBatch(oversized, 32); !errors.Is(err, errBatchCorrupt) {
		t.Fatalf("oversized message error = %v, want corrupt", err)
	}
}

func TestProcessFileDefersIncompleteBatch(t *testing.T) {
	store := newFakeStore("shared-bucket")
	cfg := testSiloConfig("shared-bucket", "alice")
	pipe, err := newPipeWithStore(&cfg, "server", testLogger{}, store)
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}

	key := "default/users/alice/c2s/msg_0001"
	complete := appendBatchMessage(nil, []byte("hello"))

	preallocated := append(append([]byte(nil), complete...), make([]byte, 8)...)
	store.putObject(key, preallocated, time.Now())

	obj := listedObject{key: key, size: int64(len(preallocated)), modified: time.Now()}
	if pipe.processFile(context.Background(), obj) {
		t.Fatal("incomplete batch was reported as delivered")
	}
	if !store.hasObject(key) {
		t.Fatal("incomplete batch object was removed")
	}
	select {
	case msg := <-pipe.recvCh:
		t.Fatalf("empty or partial message was delivered: %q", msg)
	default:
	}

	store.putObject(key, complete, time.Now())
	obj = listedObject{key: key, size: int64(len(complete)), modified: time.Now()}
	if !pipe.processFile(context.Background(), obj) {
		t.Fatal("complete batch was not delivered")
	}
	if got := recvFromPipe(t, pipe); !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("delivered %q, want hello", got)
	}
	if store.hasObject(key) {
		t.Fatal("delivered object was not removed")
	}
}

func TestProcessFileDropsStaleCorruptBatch(t *testing.T) {
	prev := incompleteBatchGrace
	incompleteBatchGrace = 20 * time.Millisecond
	t.Cleanup(func() { incompleteBatchGrace = prev })

	store := newFakeStore("shared-bucket")
	cfg := testSiloConfig("shared-bucket", "alice")
	pipe, err := newPipeWithStore(&cfg, "server", testLogger{}, store)
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}

	key := "default/users/alice/c2s/msg_0000"
	store.putObject(key, make([]byte, 16), time.Now().Add(-time.Hour))

	obj := listedObject{key: key, size: 16, modified: time.Now().Add(-time.Hour)}
	if !pipe.processFile(context.Background(), obj) {
		t.Fatal("stale corrupt batch was deferred instead of dropped")
	}
	if store.hasObject(key) {
		t.Fatal("stale corrupt batch object was not removed")
	}
	select {
	case msg := <-pipe.recvCh:
		t.Fatalf("corrupt batch produced a message: %q", msg)
	default:
	}
}

func TestSendMessageAcksAfterUpload(t *testing.T) {
	store := newFakeStore("shared-bucket")
	cfg := testSiloConfig("shared-bucket", "")
	cfg.FlushMs = 5
	pipe, err := newPipeWithStore(&cfg, "client", testLogger{}, store)
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := pipe.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = pipe.Stop(context.Background()) }()

	payload := []byte("ack-me")
	if err := pipe.SendMessage(ctx, payload, ""); err != nil {
		t.Fatalf("send: %v", err)
	}

	store.mu.Lock()
	keys := make([]string, 0, len(store.objects))
	for key := range store.objects {
		if len(key) >= len("default/c2s/") && key[:len("default/c2s/")] == "default/c2s/" {
			keys = append(keys, key)
		}
	}
	store.mu.Unlock()
	if len(keys) == 0 {
		t.Fatal("SendMessage acknowledged before the object became visible")
	}

	found := false
	for _, key := range keys {
		data, ok := store.object(key)
		if !ok {
			continue
		}
		messages, err := parseBatch(data, maxBatchMessageSize)
		if err != nil || len(messages) != 1 {
			continue
		}
		if bytes.Equal(messages[0], payload) {
			found = true
		}
	}
	if !found {
		t.Fatal("acknowledged object does not contain the original message")
	}
}

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
