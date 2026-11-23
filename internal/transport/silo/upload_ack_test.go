package silo

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

type failingPutStore struct {
	*fakeStore
}

func (s failingPutStore) PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, errors.New("boom")
}

func TestSendMessageReportsUploadFailure(t *testing.T) {
	cfg := testSiloConfig("shared-bucket", "")
	cfg.FlushMs = 5
	pipe, err := newPipeWithStore(&cfg, "client", testLogger{}, failingPutStore{newFakeStore("shared-bucket")})
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := pipe.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = pipe.Stop(context.Background()) }()

	start := time.Now()
	if err := pipe.SendMessage(ctx, []byte("payload"), ""); err == nil {
		t.Fatal("upload failure was swallowed: the tunnel would silently lose frames")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("SendMessage took %s to report the upload error", elapsed)
	}
}

func TestSendMessageUnblocksOnStop(t *testing.T) {
	cfg := testSiloConfig("shared-bucket", "")
	cfg.FlushMs = 5000
	pipe, err := newPipeWithStore(&cfg, "client", testLogger{}, newFakeStore("shared-bucket"))
	if err != nil {
		t.Fatalf("new pipe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := pipe.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- pipe.SendMessage(context.Background(), []byte("payload"), "")
	}()
	time.Sleep(20 * time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("SendMessage stayed blocked after the pipe was stopped")
	}
	_ = pipe.Stop(context.Background())
}
