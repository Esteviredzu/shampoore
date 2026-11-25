package bond

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/socks5"
	"github.com/Esteviredzu/shampoore/internal/transport"
	"golang.org/x/time/rate"
)

var errTubeCut = fmt.Errorf("%w: test tube was cut", transport.ErrTransportReset)

type linkTube struct {
	transport.Transport
	lim  *rate.Limiter
	cutc chan struct{}
	once sync.Once

	down atomic.Bool
	recv atomic.Int64
}

func newLinkTube(inner transport.Transport, bytesPerSecond int) *linkTube {
	t := &linkTube{Transport: inner, cutc: make(chan struct{})}
	if bytesPerSecond > 0 {
		t.lim = rate.NewLimiter(rate.Limit(bytesPerSecond), 64<<10)
	}
	return t
}

func (t *linkTube) Cut() {
	t.down.Store(true)
	t.once.Do(func() { close(t.cutc) })
}

func (t *linkTube) Send(ctx context.Context, data []byte) error {
	if t.down.Load() {
		return errTubeCut
	}
	if t.lim != nil {
		if err := t.lim.WaitN(ctx, len(data)); err != nil {
			return err
		}
	}
	return t.Transport.Send(ctx, data)
}

func (t *linkTube) Receive(ctx context.Context) ([]byte, error) {
	if t.down.Load() {
		return nil, errTubeCut
	}
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-t.cutc:
			cancel()
		case <-readCtx.Done():
		}
	}()
	data, err := t.Transport.Receive(readCtx)
	if err != nil {
		if t.down.Load() {
			return nil, errTubeCut
		}
		return nil, err
	}
	t.recv.Add(int64(len(data)))
	return data, nil
}

func (t *linkTube) Received() int64 { return t.recv.Load() }

func startSOCKSLocal(t *testing.T, ctx context.Context, mux multiplexing.StreamMultiplexer) string {
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

func TestBondStreamsThirtyMegabytesWhileATubeIsCutAndReconnected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		total      = 32 << 20
		cutAt      = 8 << 20
		perTubeBps = 6 << 20
	)
	names := []string{"0-raw", "1-jellyfin", "2-livekit"}

	clientTubes := make([]transport.Transport, 0, len(names))
	serverTubes := make([]transport.Transport, 0, len(names))
	for range names {
		clientEnd, serverEnd := newMemPair()
		clientTubes = append(clientTubes, newLinkTube(clientEnd, 0))
		serverTubes = append(serverTubes, newLinkTube(serverEnd, perTubeBps))
	}

	srv := NewServer(names, nil, testLogger{}, tunnel.Config{
		FlushThreshold: 8192,
		FlushInterval:  5 * time.Millisecond,
	}, nil)
	srv.SetSessionHandler(func(hctx context.Context, mux multiplexing.StreamMultiplexer) error {
		return socks5.NewRemoteHandler(hctx, mux, testLogger{}, 64*1024, true).Run()
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = srv.Stop(stopCtx)
	})

	releases := make([]func(), 0, len(names))
	for i, name := range names {
		release, _ := srv.register(name, "alice", serverTubes[i])
		if release == nil {
			t.Fatalf("tube %s was not accepted", name)
		}
		releases = append(releases, release)
	}
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
	})

	mux, err := NewMux(clientTubes, names, testLogger{})
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

	socksAddr := startSOCKSLocal(t, ctx, mux)
	host, port := startBodySource(t)

	conn := dialViaSOCKS5(t, socksAddr, host, port)
	_ = conn.SetDeadline(time.Now().Add(90 * time.Second))
	if _, err := fmt.Fprintf(conn, "GET %d\n", total); err != nil {
		t.Fatalf("write request: %v", err)
	}

	cutTube := serverTubes[1].(*linkTube)
	clientCutTube := clientTubes[1].(*linkTube)
	var cutOnce sync.Once
	revivedCh := make(chan *linkTube, 1)
	releaseCh := make(chan func(), 1)

	triggerCut := func() {
		cutOnce.Do(func() {
			clientCutTube.Cut()
			cutTube.Cut()
			go func() {
				time.Sleep(400 * time.Millisecond)

				newClientEnd, newServerEnd := newMemPair()
				newClient := newLinkTube(newClientEnd, 0)
				newServer := newLinkTube(newServerEnd, perTubeBps)

				mux.session.addTube("1-jellyfin", &transportTube{t: newClient})
				release, _ := srv.register("1-jellyfin", "alice", newServer)

				revivedCh <- newServer
				releaseCh <- release
			}()
		})
	}

	body := make([]byte, total)
	read := 0
	start := time.Now()
	for read < total {
		n, readErr := conn.Read(body[read:])
		if readErr != nil {
			t.Fatalf("read after %d of %d bytes: %v", read, total, readErr)
		}
		read += n
		if read >= cutAt {
			triggerCut()
		}
	}
	if elapsed := time.Since(start); elapsed > 90*time.Second {
		t.Fatalf("32 MiB took %s: the bond session stalled", elapsed)
	}

	for i := range body {
		if body[i] != e2ePattern[i%len(e2ePattern)] {
			t.Fatalf("payload mismatch at byte %d", i)
		}
	}

	revived := <-revivedCh
	if release := <-releaseCh; release != nil {
		t.Cleanup(release)
	}

	live := mux.session.acquire()
	if live == nil {
		t.Fatal("bond connection disappeared after the tube reconnect")
	}
	if err := live.terminalErr(); err != nil {
		t.Fatalf("bond session died during the tube reconnect: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && revived.Received() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if revived.Received() == 0 {
		t.Fatal("the reconnected tube never carried data again")
	}

	again := dialViaSOCKS5(t, socksAddr, host, port)
	const smallSize = 128 << 10
	if _, err := fmt.Fprintf(again, "GET %d\n", smallSize); err != nil {
		t.Fatalf("second request: %v", err)
	}
	small := make([]byte, smallSize)
	if _, err := io.ReadFull(again, small); err != nil {
		t.Fatalf("second transfer: %v", err)
	}
	want := bytes.Repeat(e2ePattern, smallSize/len(e2ePattern)+1)[:smallSize]
	if !bytes.Equal(small, want) {
		t.Fatal("second transfer returned the wrong payload")
	}
}
