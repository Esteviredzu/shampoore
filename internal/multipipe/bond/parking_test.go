package bond

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/socks5"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type discardEnd struct{}

func (discardEnd) Start(context.Context) error                 { return nil }
func (discardEnd) Stop(context.Context) error                  { return nil }
func (discardEnd) Send(context.Context, []byte) error          { return nil }
func (discardEnd) MaxMessageSize() int                         { return 60000 }
func (discardEnd) BytesDelivered() int64                       { return 0 }
func (discardEnd) BytesAcked() int64                           { return 0 }
func (discardEnd) Receive(ctx context.Context) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }

func TestBondParksTubeWithForeignFraming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	healthyClient, healthyServer := newMemPair()
	garbage := append([]byte("SPV2\x02\x01"), []byte(strings.Repeat("y", 48))...)

	log := &captureLogger{}
	names := []string{"0-raw", "1-foreign"}

	srv := NewServer(names, nil, log, tunnel.Config{
		FlushThreshold: 4096,
		FlushInterval:  2 * time.Millisecond,
	}, nil)
	srv.SetSessionHandler(func(hctx context.Context, mux multiplexing.StreamMultiplexer) error {
		return socks5.NewRemoteHandler(hctx, mux, log, 32*1024, true).Run()
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	serverTubes := []transport.Transport{healthyServer, &foreignTube{garbage: garbage}}
	for i, name := range names {
		release, _ := srv.register(name, "alice", serverTubes[i])
		if release == nil {
			t.Fatalf("tube %s was not accepted", name)
		}
		t.Cleanup(release)
	}

	mux, err := NewMux([]transport.Transport{healthyClient, discardEnd{}}, names, log)
	if err != nil {
		t.Fatalf("client mux: %v", err)
	}
	mux.SetIdentity("alice", "")
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
	body := fetchThroughSOCKS(t, socksAddr, host, port, 64<<10)
	if len(body) != 64<<10 {
		t.Fatalf("received %d bytes, want %d", len(body), 64<<10)
	}
	if _, ok := log.contains("parking the subflow"); !ok {
		t.Fatalf("the foreign tube was not parked; log:\n%s", log.dump())
	}
	conn := mux.session.acquire()
	if conn == nil {
		t.Fatal("bond session disappeared")
	}
	if err := conn.terminalErr(); err != nil {
		t.Fatalf("a foreign tube killed the bond session: %v", err)
	}
}
