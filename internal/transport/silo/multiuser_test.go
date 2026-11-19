package silo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
)

type fakeStore struct {
	mu      sync.Mutex
	bucket  string
	objects map[string][]byte
	history []string
}

func newFakeStore(bucket string) *fakeStore {
	return &fakeStore{bucket: bucket, objects: make(map[string][]byte)}
}

func (s *fakeStore) BucketExists(_ context.Context, bucket string) (bool, error) {
	return bucket == s.bucket, nil
}

func (s *fakeStore) MakeBucket(_ context.Context, bucket string, _ minio.MakeBucketOptions) error {
	if bucket != s.bucket {
		return errors.New("access denied")
	}
	return nil
}

func (s *fakeStore) PutObject(_ context.Context, bucket, object string, reader io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	if bucket != s.bucket {
		return minio.UploadInfo{}, errors.New("no such bucket")
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	s.mu.Lock()
	s.objects[object] = append([]byte(nil), data...)
	s.history = append(s.history, object)
	s.mu.Unlock()
	return minio.UploadInfo{Key: object, Size: int64(len(data))}, nil
}

func (s *fakeStore) GetObject(_ context.Context, bucket, object string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	if bucket != s.bucket {
		return nil, errors.New("no such bucket")
	}
	s.mu.Lock()
	data, ok := s.objects[object]
	s.mu.Unlock()
	if !ok {
		return nil, minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), nil
}

func (s *fakeStore) RemoveObject(_ context.Context, bucket, object string, _ minio.RemoveObjectOptions) error {
	if bucket != s.bucket {
		return errors.New("no such bucket")
	}
	s.mu.Lock()
	delete(s.objects, object)
	s.mu.Unlock()
	return nil
}

func (s *fakeStore) ListObjects(_ context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo, 16)
	if bucket != s.bucket {
		close(ch)
		return ch
	}
	s.mu.Lock()
	var keys []string
	for key := range s.objects {
		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			continue
		}
		if opts.StartAfter != "" && key <= opts.StartAfter {
			continue
		}
		keys = append(keys, key)
	}
	s.mu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		ch <- minio.ObjectInfo{Key: key}
	}
	close(ch)
	return ch
}

func (s *fakeStore) hasPrefix(prefix string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (s *fakeStore) recorded(prefix string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range s.history {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func testSiloConfig(bucket, userID string) Config {
	cfg := DefaultConfig()
	cfg.Bucket = bucket
	cfg.UserID = userID
	cfg.MinFileSizeKB = 1
	cfg.MaxFileSizeKB = 1
	cfg.FlushMs = 5
	cfg.PollIntervalMs = 10
	_ = cfg.Validate()
	return cfg
}

func waitUntil(t *testing.T, timeout time.Duration, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func receiveWithTimeout(t *testing.T, pipe *Pipe, timeout time.Duration) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	data, err := pipe.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	return data
}

func TestBuildPrefixes(t *testing.T) {
	cases := []struct {
		name      string
		userID    string
		role      string
		wantWrite string
		wantRead  string
	}{
		{name: "legacy client", role: "client", wantWrite: "default/c2s/", wantRead: "default/s2c/"},
		{name: "legacy server", role: "server", wantWrite: "default/s2c/", wantRead: "default/c2s/"},
		{name: "user client", userID: "alice", role: "client", wantWrite: "users/alice/c2s/", wantRead: "users/alice/s2c/"},
		{name: "user server", userID: "alice", role: "server", wantWrite: "users/alice/s2c/", wantRead: "users/alice/c2s/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.UserID = tc.userID
			write, read := buildPrefixes(&cfg, tc.role)
			if write != tc.wantWrite || read != tc.wantRead {
				t.Fatalf("prefixes = %q/%q, want %q/%q", write, read, tc.wantWrite, tc.wantRead)
			}
		})
	}
}

func TestSanitizeUserID(t *testing.T) {
	if _, err := sanitizeUserID("../escape"); err == nil {
		t.Fatal("path traversal user id was accepted")
	}
	if _, err := sanitizeUserID("bad id"); err == nil {
		t.Fatal("user id with space was accepted")
	}
	if got, err := sanitizeUserID("  550e8400-e29b-41d4-a716-446655440000  "); err != nil || got != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("unexpected normalized id: %q, %v", got, err)
	}
	if user, ok := ParseUserFromKey("users/alice/c2s/msg_1"); !ok || user != "alice" {
		t.Fatalf("unexpected parsed user: %q %v", user, ok)
	}
}

func TestTwoUsersShareOneBucket(t *testing.T) {
	store := newFakeStore("shared-bucket")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfgAlice := testSiloConfig("shared-bucket", "alice")
	cfgBob := testSiloConfig("shared-bucket", "bob")

	clientAlice, err := newPipeWithStore(&cfgAlice, "client", testLogger{}, store)
	if err != nil {
		t.Fatal(err)
	}
	clientBob, err := newPipeWithStore(&cfgBob, "client", testLogger{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := clientAlice.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := clientBob.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = clientAlice.Stop(context.Background()) }()
	defer func() { _ = clientBob.Stop(context.Background()) }()

	if err := clientAlice.SendMessage(ctx, []byte("from-alice"), ""); err != nil {
		t.Fatal(err)
	}
	if err := clientBob.SendMessage(ctx, []byte("from-bob"), ""); err != nil {
		t.Fatal(err)
	}

	waitUntil(t, 2*time.Second, "alice upload", func() bool { return store.hasPrefix("users/alice/c2s/") })
	waitUntil(t, 2*time.Second, "bob upload", func() bool { return store.hasPrefix("users/bob/c2s/") })
	if store.recorded("users/alice/s2c/") || store.recorded("users/bob/s2c/") {
		t.Fatal("client wrote into an s2c prefix")
	}

	serverCfgAlice := cfgAlice
	serverAlice, err := newPipeWithStore(&serverCfgAlice, "server", testLogger{}, store)
	if err != nil {
		t.Fatal(err)
	}
	serverCfgBob := cfgBob
	serverBob, err := newPipeWithStore(&serverCfgBob, "server", testLogger{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := serverAlice.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := serverBob.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = serverAlice.Stop(context.Background()) }()
	defer func() { _ = serverBob.Stop(context.Background()) }()

	gotAlice := receiveWithTimeout(t, serverAlice, 2*time.Second)
	gotBob := receiveWithTimeout(t, serverBob, 2*time.Second)
	if string(gotAlice) != "from-alice" {
		t.Fatalf("alice received %q", gotAlice)
	}
	if string(gotBob) != "from-bob" {
		t.Fatalf("bob received %q", gotBob)
	}

	if err := serverAlice.SendMessage(ctx, []byte("reply-alice"), ""); err != nil {
		t.Fatal(err)
	}
	if err := serverBob.SendMessage(ctx, []byte("reply-bob"), ""); err != nil {
		t.Fatal(err)
	}

	waitUntil(t, 2*time.Second, "alice reply upload", func() bool { return store.recorded("users/alice/s2c/") })
	waitUntil(t, 2*time.Second, "bob reply upload", func() bool { return store.recorded("users/bob/s2c/") })

	if got := receiveWithTimeout(t, clientAlice, 2*time.Second); string(got) != "reply-alice" {
		t.Fatalf("alice client received %q", got)
	}
	if got := receiveWithTimeout(t, clientBob, 2*time.Second); string(got) != "reply-bob" {
		t.Fatalf("bob client received %q", got)
	}
}

func TestMultiServerStartsAndStopsPerUser(t *testing.T) {
	store := newFakeStore("shared-bucket")
	cfg := testSiloConfig("shared-bucket", "")
	cfg.Multiuser = true

	registry := auth.NewRegistry()
	for _, id := range []string{"alice", "bob"} {
		if err := registry.Upsert(auth.UserSpec{UserID: id, Enabled: true}); err != nil {
			t.Fatal(err)
		}
	}

	server := newMultiServer(cfg, testLogger{}, registry, "", store, app.Values{
		"mux_flush_ms":        5,
		"mux_flush_threshold": 12000,
	})
	server.SetSessionHandler(func(ctx context.Context, _ multiplexing.StreamMultiplexer) error {
		<-ctx.Done()
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = server.Stop(stopCtx)
	}()

	waitUntil(t, 5*time.Second, "two sessions", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return len(server.sessions) == 2
	})

	registry.Remove("alice")
	waitUntil(t, 5*time.Second, "alice session removal", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		_, alice := server.sessions["alice"]
		_, bob := server.sessions["bob"]
		return !alice && bob
	})
}

func runUserRoundTrip(ctx context.Context, client, server *Pipe, payload, reply []byte) error {
	if err := client.SendMessage(ctx, payload, ""); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	up, err := server.ReceiveMessage(ctx)
	if err != nil {
		return fmt.Errorf("server receive: %w", err)
	}
	if !bytes.Equal(up, payload) {
		return fmt.Errorf("server got %q, want %q", up, payload)
	}
	if err := server.SendMessage(ctx, reply, ""); err != nil {
		return fmt.Errorf("server send: %w", err)
	}
	down, err := client.ReceiveMessage(ctx)
	if err != nil {
		return fmt.Errorf("client receive: %w", err)
	}
	if !bytes.Equal(down, reply) {
		return fmt.Errorf("client got %q, want %q", down, reply)
	}
	return nil
}

func TestTwoUsersConcurrentIsolation(t *testing.T) {
	store := newFakeStore("shared-bucket")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type user struct {
		id     string
		cfg    Config
		client *Pipe
		server *Pipe
	}
	users := make([]*user, 0, 2)
	for _, id := range []string{"alice", "bob"} {
		cfg := testSiloConfig("shared-bucket", id)
		client, err := newPipeWithStore(&cfg, "client", testLogger{}, store)
		if err != nil {
			t.Fatal(err)
		}
		serverCfg := cfg
		server, err := newPipeWithStore(&serverCfg, "server", testLogger{}, store)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := server.Start(ctx); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = client.Stop(context.Background()) }()
		defer func() { _ = server.Stop(context.Background()) }()
		users = append(users, &user{id: id, cfg: cfg, client: client, server: server})
	}

	errCh := make(chan error, len(users))
	var wg sync.WaitGroup
	for _, u := range users {
		u := u
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := []byte("payload-" + u.id)
			reply := []byte("reply-" + u.id)
			errCh <- runUserRoundTrip(ctx, u.client, u.server, payload, reply)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent round trip: %v", err)
		}
	}

	for _, id := range []string{"alice", "bob"} {
		if !store.recorded("users/" + id + "/c2s/") {
			t.Fatalf("no c2s objects for %s", id)
		}
		if !store.recorded("users/" + id + "/s2c/") {
			t.Fatalf("no s2c objects for %s", id)
		}
	}
}
