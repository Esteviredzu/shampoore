package silo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/testutil/s3test"
)

const (
	reconnectTunnelID = "route-7"
	reconnectPassword = "silo-reconnect-secret"
	reconnectBucket   = "shampoore-e2e"
)

func s3SiloConfig(endpoint, userID string) Config {
	cfg := DefaultConfig()
	cfg.Endpoint = endpoint
	cfg.AccessKey = "minioadmin"
	cfg.SecretKey = "minioadmin"
	cfg.Bucket = reconnectBucket
	cfg.UseSSL = false
	cfg.TunnelID = reconnectTunnelID
	cfg.UserID = userID
	cfg.MinFileSizeKB = 256
	cfg.MaxFileSizeKB = 512
	cfg.FlushMs = 50
	cfg.PollIntervalMs = 10
	_ = cfg.Validate()
	return cfg
}

func newS3Store(t *testing.T, cfg Config) objectStore {
	t.Helper()
	store, err := newMinioStore(&cfg)
	if err != nil {
		t.Fatalf("minio store: %v", err)
	}
	return store
}

func s2cPrefix(tunnelID, userID string) string {
	write, _ := buildPrefixes(&Config{TunnelID: tunnelID, UserID: userID}, "server")
	return write
}

func echoAllStreams(ctx context.Context, mux multiplexing.StreamMultiplexer) error {
	for {
		stream, err := mux.AcceptStream(ctx)
		if err != nil {
			return err
		}
		go func(st multiplexing.Stream) {
			defer st.Close()
			_ = st.SetDeadline(time.Now().Add(30 * time.Second))
			buf := make([]byte, 32*1024)
			for {
				n, err := st.Read(buf)
				if n > 0 {
					if _, werr := st.Write(buf[:n]); werr != nil {
						return
					}
				}
				if err != nil {
					return
				}
			}
		}(stream)
	}
}

func newSiloClientMux(t *testing.T, server *s3test.Server, userID string) *tunnel.TunnelMultiplexer {
	t.Helper()
	cfg := s3SiloConfig(server.Endpoint(), userID)
	pipe, err := newPipeWithStore(&cfg, "client", testLogger{}, newS3Store(t, cfg))
	if err != nil {
		t.Fatalf("client pipe: %v", err)
	}
	secured, err := wrapEncryption(pipe, reconnectPassword, "client", testLogger{})
	if err != nil {
		t.Fatalf("client encryption: %v", err)
	}
	mux := tunnel.New(newTransport(secured), testLogger{}, tunnel.Config{
		FlushThreshold: 12000,
		FlushInterval:  5 * time.Millisecond,
		UserID:         userID,
		ExitNodeID:     "42",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	if err := mux.Start(ctx); err != nil {
		cancel()
		t.Fatalf("client mux start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = mux.Stop(stopCtx)
		cancel()
	})
	return mux
}

func roundTripStream(t *testing.T, mux *tunnel.TunnelMultiplexer, payload []byte, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stream, err := mux.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(timeout))

	writeErr := make(chan error, 1)
	go func() {
		_, err := stream.Write(payload)
		writeErr <- err
	}()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("read echo (%d bytes): %v", len(payload), err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("echo mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestSiloReconnectAcceptsSecondClient(t *testing.T) {
	prevRescan := idleRescanInterval
	idleRescanInterval = 300 * time.Millisecond
	t.Cleanup(func() { idleRescanInterval = prevRescan })

	server := s3test.New(t)
	cfg := s3SiloConfig(server.Endpoint(), "")
	cfg.Multiuser = true
	store := newS3Store(t, cfg)

	registry := e2eRegistry(t, "alice")
	multi := newMultiServer(cfg, testLogger{}, registry, reconnectPassword, store, app.Values{
		"mux_flush_ms":        5,
		"mux_flush_threshold": 12000,
	})
	multi.SetExitNodeID("42")
	multi.SetSessionHandler(echoAllStreams)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := multi.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = multi.Stop(stopCtx)
	})
	waitUntil(t, 10*time.Second, "server session", func() bool { return multi.sessionCount() == 1 })

	first := newSiloClientMux(t, server, "alice")
	roundTripStream(t, first, bytes.Repeat([]byte("first-"), 8192), 30*time.Second)

	layers := multi.WalkLayers()
	if len(layers) == 0 || layers[0].Kind != "mux" {
		t.Fatalf("server telemetry is missing: %+v", layers)
	}
	if layers[0].Snapshot.UsefulRecv == 0 || layers[0].Snapshot.UsefulSent == 0 {
		t.Fatalf("server telemetry did not count traffic: %+v", layers[0].Snapshot)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := first.Stop(stopCtx); err != nil {
		stopCancel()
		t.Fatalf("first client stop: %v", err)
	}
	stopCancel()

	batch := appendBatchMessage(nil, []byte("stale-leftover"))
	poisonKey := C2SPrefix(reconnectTunnelID, "alice") + "msg_9999999999999999999_0"
	if _, err := store.PutObject(ctx, cfg.Bucket, poisonKey, bytes.NewReader(batch), int64(len(batch)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"}); err != nil {
		t.Fatalf("put stale object: %v", err)
	}
	waitUntil(t, 15*time.Second, "stale object consumed", func() bool {
		return server.Count(cfg.Bucket, C2SPrefix(reconnectTunnelID, "alice")) == 0
	})

	second := newSiloClientMux(t, server, "alice")
	roundTripStream(t, second, bytes.Repeat([]byte("second-"), 4), 20*time.Second)

	waitUntil(t, 15*time.Second, "stale responses purged", func() bool {
		return server.Count(cfg.Bucket, s2cPrefix(reconnectTunnelID, "alice")) == 0
	})
}

func TestSiloReconnectSurvivesRepeatedClientDrops(t *testing.T) {
	prevRescan := idleRescanInterval
	idleRescanInterval = 200 * time.Millisecond
	prevRetry := sessionRetryDelay
	sessionRetryDelay = 20 * time.Millisecond
	t.Cleanup(func() {
		idleRescanInterval = prevRescan
		sessionRetryDelay = prevRetry
	})

	server := s3test.New(t)
	cfg := s3SiloConfig(server.Endpoint(), "")
	cfg.Multiuser = true
	store := newS3Store(t, cfg)

	registry := e2eRegistry(t, "alice")
	multi := newMultiServer(cfg, testLogger{}, registry, reconnectPassword, store, app.Values{
		"mux_flush_ms":        5,
		"mux_flush_threshold": 12000,
	})
	multi.SetExitNodeID("42")
	multi.SetSessionHandler(echoAllStreams)

	multi.muxCfg.PingInterval = 25 * time.Millisecond
	multi.muxCfg.PingTimeout = 300 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := multi.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = multi.Stop(stopCtx)
	})
	waitUntil(t, 10*time.Second, "server session", func() bool { return multi.sessionCount() == 1 })

	for cycle := 0; cycle < 3; cycle++ {
		multi.mu.Lock()
		session := multi.sessions["alice"]
		multi.mu.Unlock()
		if session == nil {
			t.Fatalf("cycle %d: alice session is missing", cycle)
		}
		before := session.currentMux()

		client := newSiloClientMux(t, server, "alice")
		roundTripStream(t, client, []byte(fmt.Sprintf("cycle-%d-payload", cycle)), 30*time.Second)

		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := client.Stop(stopCtx)
		stopCancel()
		if err != nil {
			t.Fatalf("cycle %d: client stop: %v", cycle, err)
		}

		waitUntil(t, 15*time.Second, "exit session rebuilt after the drop", func() bool {
			now := session.currentMux()
			return now != nil && now != before
		})
	}
}

func TestSiloSmallReplyIsNotHeldForMinBatch(t *testing.T) {
	server := s3test.New(t)
	clientCfg := s3SiloConfig(server.Endpoint(), "alice")
	serverCfg := s3SiloConfig(server.Endpoint(), "alice")

	clientPipe, err := newPipeWithStore(&clientCfg, "client", testLogger{}, newS3Store(t, clientCfg))
	if err != nil {
		t.Fatalf("client pipe: %v", err)
	}
	serverPipe, err := newPipeWithStore(&serverCfg, "server", testLogger{}, newS3Store(t, serverCfg))
	if err != nil {
		t.Fatalf("server pipe: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := clientPipe.Start(ctx); err != nil {
		t.Fatalf("client start: %v", err)
	}
	defer func() { _ = clientPipe.Stop(context.Background()) }()
	if err := serverPipe.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = serverPipe.Stop(context.Background()) }()

	reply := []byte("tiny-response")
	if err := clientPipe.SendMessage(ctx, reply, ""); err != nil {
		t.Fatalf("send request: %v", err)
	}
	if got := receiveWithTimeout(t, serverPipe, 2*time.Second); !bytes.Equal(got, reply) {
		t.Fatalf("server received %q, want %q", got, reply)
	}
	if err := serverPipe.SendMessage(ctx, reply, ""); err != nil {
		t.Fatalf("send reply: %v", err)
	}
	if got := receiveWithTimeout(t, clientPipe, 2*time.Second); !bytes.Equal(got, reply) {
		t.Fatalf("client received %q, want %q", got, reply)
	}
}
