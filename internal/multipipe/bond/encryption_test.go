package bond

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

func TestFramingHintIdentifiesBrokenTubes(t *testing.T) {
	if got := framingHint([]byte("SPV2\x02\x01rest")); !strings.Contains(got, "SecurePipe") {
		t.Fatalf("SPV2 hint = %q", got)
	}
	if got := framingHint([]byte("TUN1\x02\x01rest")); !strings.Contains(got, "bond framing") {
		t.Fatalf("TUN1 hint = %q", got)
	}
	if got := framingHint([]byte{0, 0, 0, 0}); got != "" {
		t.Fatalf("unexpected hint for a plain frame: %q", got)
	}
}

func TestBondWrapsPlainTubesWithSecurePipe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	clientT, serverT := newMemPair()
	cfg := tunnel.Config{FlushThreshold: 4096, FlushInterval: 2 * time.Millisecond}
	srv := NewServer([]string{"0-a"}, nil, testLogger{}, cfg, nil)
	srv.SetEncryptionPassword("bond-secret")
	srv.SetSessionHandler(func(hctx context.Context, mux multiplexing.StreamMultiplexer) error {
		stream, err := mux.AcceptStream(hctx)
		if err != nil {
			return err
		}
		defer stream.Close()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(stream, buf); err != nil {
			return err
		}
		_, err = stream.Write(buf)
		return err
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	release, _ := srv.register("0-a", "alice", serverT)
	if release == nil {
		t.Fatal("tube was not accepted by the aggregator")
	}
	defer release()

	clientMux, err := NewMux([]transport.Transport{clientT}, []string{"0-a"}, testLogger{})
	if err != nil {
		t.Fatalf("client mux: %v", err)
	}
	clientMux.SetEncryptionPassword("bond-secret")
	if err := clientMux.Start(ctx); err != nil {
		t.Fatalf("client start: %v", err)
	}
	defer func() { _ = clientMux.Stop(context.Background()) }()

	stream, err := clientMux.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := stream.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(stream, reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if string(reply) != "ping" {
		t.Fatalf("reply = %q, want ping", reply)
	}
}
