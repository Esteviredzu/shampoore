package silo

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multipipe/bond"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

func echoOneStream(size int) app.SessionHandler {
	return func(ctx context.Context, mux multiplexing.StreamMultiplexer) error {
		stream, err := mux.AcceptStream(ctx)
		if err != nil {
			return err
		}
		defer stream.Close()
		_ = stream.SetDeadline(time.Now().Add(20 * time.Second))
		buf := make([]byte, size)
		if _, err := io.ReadFull(stream, buf); err != nil {
			return err
		}
		if _, err := stream.Write(buf); err != nil {
			return err
		}

		<-ctx.Done()
		return ctx.Err()
	}
}

func e2eSiloConfig(userID string) Config {
	cfg := testSiloConfig("shared-bucket", userID)
	cfg.MinFileSizeKB = 8
	cfg.MaxFileSizeKB = 64
	cfg.FlushMs = 5
	cfg.PollIntervalMs = 10
	return cfg
}

func e2eServerConfig() Config {
	cfg := e2eSiloConfig("")
	cfg.Multiuser = true
	return cfg
}

func e2eRegistry(t *testing.T, userID string) *auth.UserRegistry {
	t.Helper()
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{UserID: userID, Enabled: true}); err != nil {
		t.Fatalf("registry: %v", err)
	}
	return registry
}

func e2eClientMux(t *testing.T, userID, password string, store *fakeStore) *tunnel.TunnelMultiplexer {
	t.Helper()
	cfg := e2eSiloConfig(userID)
	pipe, err := newPipeWithStore(&cfg, "client", testLogger{}, store)
	if err != nil {
		t.Fatalf("client pipe: %v", err)
	}
	secured, err := wrapEncryption(pipe, password, "client", testLogger{})
	if err != nil {
		t.Fatalf("client encryption: %v", err)
	}
	tr := newTransport(secured)

	mux := tunnel.New(tr, testLogger{}, tunnel.Config{
		FlushThreshold: 12000,
		FlushInterval:  5 * time.Millisecond,
	})
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mux.Stop(stopCtx)
	})
	return mux
}

func TestSiloSingleUserSessionAcceptsClientWithoutHello(t *testing.T) {
	store := newFakeStore("shared-bucket")
	registry := e2eRegistry(t, "alice")
	payload := []byte("hello-without-hello")

	server := newMultiServer(e2eServerConfig(), testLogger{}, registry, "silo-e2e-secret", store, app.Values{
		"mux_flush_ms":        5,
		"mux_flush_threshold": 12000,
	})
	server.SetExitNodeID("42")
	server.SetSessionHandler(echoOneStream(len(payload)))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = server.Stop(stopCtx)
	})

	mux := e2eClientMux(t, "alice", "silo-e2e-secret", store)
	if err := mux.Start(ctx); err != nil {
		t.Fatalf("client mux start: %v", err)
	}

	stream, err := mux.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(20 * time.Second))

	if _, err := stream.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("echo = %q, want %q", got, payload)
	}
}

func TestBondOverEncryptedSiloThroughMultiServer(t *testing.T) {
	store := newFakeStore("shared-bucket")
	registry := e2eRegistry(t, "alice")
	payload := bytes.Repeat([]byte("bond-encrypted-"), 512)
	lg := logging.New("silo-e2e", false)

	multi := newMultiServer(e2eServerConfig(), lg, registry, "silo-e2e-secret", store, app.Values{
		"mux_flush_ms":        5,
		"mux_flush_threshold": 12000,
	})
	multi.SetExitNodeID("42")

	bondCfg := tunnel.Config{
		FlushThreshold: 8192,
		FlushInterval:  5 * time.Millisecond,
		Registry:       registry,
	}
	aggregator := bond.NewServer([]string{"0-silo"}, []app.SessionServer{multi}, lg, bondCfg, registry)
	aggregator.SetEncryptionPassword("silo-e2e-secret")
	aggregator.SetExitNodeID("42")
	accepted := make(chan struct{})
	aggregator.SetSessionHandler(func(hctx context.Context, mux multiplexing.StreamMultiplexer) error {
		t.Log("aggregator: waiting for a stream")
		stream, err := mux.AcceptStream(hctx)
		if err != nil {
			t.Logf("aggregator: accept: %v", err)
			return err
		}
		t.Log("aggregator: stream accepted")
		close(accepted)
		defer stream.Close()
		_ = stream.SetDeadline(time.Now().Add(20 * time.Second))
		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(stream, buf); err != nil {
			t.Logf("aggregator: read: %v", err)
			return err
		}
		t.Log("aggregator: payload read, echoing")
		if _, err := stream.Write(buf); err != nil {
			t.Logf("aggregator: write: %v", err)
			return err
		}

		<-hctx.Done()
		return hctx.Err()
	})
	multi.SetTransportSink(aggregator.Sink("0-silo"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := aggregator.Start(ctx); err != nil {
		t.Fatalf("aggregator start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = aggregator.Stop(stopCtx)
	})

	cfg := e2eSiloConfig("alice")
	clientPipe, err := newPipeWithStore(&cfg, "client", lg, store)
	if err != nil {
		t.Fatalf("client pipe: %v", err)
	}
	secured, err := wrapEncryption(clientPipe, "silo-e2e-secret", "client", lg)
	if err != nil {
		t.Fatalf("client encryption: %v", err)
	}

	clientMux, err := bond.NewMux(
		[]transport.Transport{newTransport(secured)},
		[]string{"0-silo"},
		lg,
	)
	if err != nil {
		t.Fatalf("client bond mux: %v", err)
	}
	clientMux.SetIdentity("alice", "42")
	clientMux.SetEncryptionPassword("silo-e2e-secret")
	if err := clientMux.Start(ctx); err != nil {
		t.Fatalf("client bond start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = clientMux.Stop(stopCtx)
	})

	stream, err := clientMux.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(20 * time.Second))

	select {
	case <-accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("aggregator did not accept the stream")
	}

	writeErr := make(chan error, 1)
	go func() {
		_, err := stream.Write(payload)
		writeErr <- err
	}()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}
