package bond

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/socks5"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type authReportingTransport struct {
	transport.Transport
	authenticated bool
	maxSize       int
}

func (t *authReportingTransport) Authenticated() bool { return t.authenticated }
func (t *authReportingTransport) MaxMessageSize() int {
	if t.maxSize > 0 {
		return t.maxSize
	}
	return t.Transport.MaxMessageSize()
}

func TestBondTubeWrappingIsSymmetric(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientEnd, serverEnd := newMemPair()

	clientTube := &authReportingTransport{Transport: clientEnd, authenticated: false}
	serverTube := &authReportingTransport{Transport: serverEnd, authenticated: true}

	log := &captureLogger{}
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{UserID: "alice", Enabled: true, AllowedExits: []string{"42"}}); err != nil {
		t.Fatalf("registry: %v", err)
	}

	srv := NewServer([]string{"0-livekit"}, nil, log, tunnel.Config{
		FlushThreshold: 4096,
		FlushInterval:  2 * time.Millisecond,
	}, registry)
	srv.SetEncryptionPassword("bond-secret")
	srv.SetExitNodeID("42")
	srv.SetSessionHandler(func(hctx context.Context, mux multiplexing.StreamMultiplexer) error {
		return socks5.NewRemoteHandler(hctx, mux, log, 32*1024, true).Run()
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	release, _ := srv.register("0-livekit", "alice", serverTube)
	if release == nil {
		t.Fatal("tube was not accepted by the aggregator")
	}
	t.Cleanup(release)

	mux, err := NewMux([]transport.Transport{clientTube}, []string{"0-livekit"}, log)
	if err != nil {
		t.Fatalf("client mux: %v", err)
	}
	mux.SetIdentity("alice", "42")
	mux.SetEncryptionPassword("bond-secret")
	if err := mux.Start(ctx); err != nil {
		t.Fatalf("bond start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = mux.Stop(stopCtx)
	})

	host, port := startBodySource(t)
	socksAddr := startLocalSOCKS(t, ctx, mux)
	body := fetchThroughSOCKS(t, socksAddr, host, port, 128<<10)
	if len(body) != 128<<10 {
		t.Fatalf("received %d bytes, want %d", len(body), 128<<10)
	}
	if msg, ok := log.contains("SPV2 message reached bond without SecurePipe"); ok {
		t.Fatalf("a tube was encrypted on one side only: %s", msg)
	}
}

func startLocalSOCKS(t *testing.T, ctx context.Context, mux multiplexing.StreamMultiplexer) string {
	t.Helper()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve socks address: %v", err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	local := socks5.NewLocalServer(addr, mux, testLogger{}, true)
	go func() { _ = local.ListenAndServe(ctx) }()
	t.Cleanup(local.Stop)
	return addr
}

type foreignTube struct {
	transport.Transport
	garbage []byte
	mu      sync.Mutex
	sends   int
}

func (f *foreignTube) Start(context.Context) error { return nil }
func (f *foreignTube) Stop(context.Context) error  { return nil }

func (f *foreignTube) Send(context.Context, []byte) error {
	f.mu.Lock()
	f.sends++
	f.mu.Unlock()
	return nil
}

func (f *foreignTube) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
	}
	return append([]byte(nil), f.garbage...), nil
}

func (f *foreignTube) MaxMessageSize() int { return 60000 }
func (f *foreignTube) Authenticated() bool { return true }

var _ = strings.Repeat
