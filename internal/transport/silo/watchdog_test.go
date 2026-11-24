package silo

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

type stallingStore struct {
	*fakeStore
	calls      atomic.Int32
	stallAfter int32
}

func (s *stallingStore) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	n := s.calls.Add(1)
	if s.stallAfter == 0 || n > s.stallAfter {
		ch := make(chan minio.ObjectInfo)
		go func() {
			defer close(ch)
			<-ctx.Done()
		}()
		return ch
	}
	return s.fakeStore.ListObjects(ctx, bucket, opts)
}

func testPipeConfig() Config {
	return Config{
		Endpoint:       "s3.test",
		AccessKey:      "access",
		SecretKey:      "secret",
		Bucket:         "bucket",
		TunnelID:       "default",
		MinFileSizeKB:  1,
		MaxFileSizeKB:  2,
		FlushMs:        20,
		PollIntervalMs: 10,
	}
}

func TestSiloPollSurvivesStalledRequests(t *testing.T) {
	restoreRequest := s3RequestTimeout
	s3RequestTimeout = 60 * time.Millisecond
	t.Cleanup(func() { s3RequestTimeout = restoreRequest })

	store := &stallingStore{fakeStore: newFakeStore("bucket")}
	cfg := testPipeConfig()
	pipe, err := newPipeWithStore(&cfg, "client", testLogger{}, store)
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}
	if err := pipe.Start(context.Background()); err != nil {
		t.Fatalf("start pipe: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = pipe.Stop(stopCtx)
	})

	deadline := time.Now().Add(2 * time.Second)
	for store.calls.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("poll loop stalled: only %d list calls", store.calls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := pipe.ReceiveMessage(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ReceiveMessage = %v, want the caller's deadline", err)
	}
}

func TestSiloDownloadSilenceResetsSession(t *testing.T) {
	restoreRequest := s3RequestTimeout
	restoreSilence := downloadSilenceLimit
	s3RequestTimeout = 60 * time.Millisecond
	downloadSilenceLimit = 200 * time.Millisecond
	t.Cleanup(func() {
		s3RequestTimeout = restoreRequest
		downloadSilenceLimit = restoreSilence
	})

	store := &stallingStore{fakeStore: newFakeStore("bucket"), stallAfter: 1}
	store.putObject("default/s2c/msg_0001", appendBatchMessage(nil, []byte("hello")), time.Now())

	cfg := testPipeConfig()
	pipe, err := newPipeWithStore(&cfg, "client", testLogger{}, store)
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}
	if err := pipe.Start(context.Background()); err != nil {
		t.Fatalf("start pipe: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = pipe.Stop(stopCtx)
	})

	recvCtx, recvCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer recvCancel()
	msg, err := pipe.ReceiveMessage(recvCtx)
	if err != nil {
		t.Fatalf("first message: %v", err)
	}
	if string(msg) != "hello" {
		t.Fatalf("first message = %q", msg)
	}

	resetCtx, resetCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer resetCancel()
	if _, err := pipe.ReceiveMessage(resetCtx); !errors.Is(err, transport.ErrPipeReset) {
		t.Fatalf("ReceiveMessage after silence = %v, want ErrPipeReset", err)
	}
}
