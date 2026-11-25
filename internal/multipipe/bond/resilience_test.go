package bond

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
	"golang.org/x/time/rate"
)

type delayedStartTransport struct {
	transport.Transport
	gate chan struct{}
}

func (d *delayedStartTransport) Start(ctx context.Context) error {
	select {
	case <-d.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return d.Transport.Start(ctx)
}

type throttledTransport struct {
	transport.Transport
	lim *rate.Limiter
}

func (t *throttledTransport) Send(ctx context.Context, data []byte) error {
	if t.lim != nil {
		if err := t.lim.WaitN(ctx, len(data)); err != nil {
			return err
		}
	}
	return t.Transport.Send(ctx, data)
}

type countingTransport struct {
	transport.Transport
	received atomic.Int64
}

func (c *countingTransport) Receive(ctx context.Context) ([]byte, error) {
	data, err := c.Transport.Receive(ctx)
	if err == nil {
		c.received.Add(int64(len(data)))
	}
	return data, err
}

func readingEcho(ctx context.Context, mux multiplexing.StreamMultiplexer) error {
	for {
		stream, err := mux.AcceptStream(ctx)
		if err != nil {
			return err
		}
		go func(st multiplexing.Stream) {
			defer st.Close()
			buf := make([]byte, 32*1024)
			for {
				n, rerr := st.Read(buf)
				if n > 0 {
					if _, werr := st.Write(buf[:n]); werr != nil {
						return
					}
				}
				if rerr != nil {
					return
				}
			}
		}(stream)
	}
}

func TestBondStartsWithLiveTubesAndRejoinsRecoveredTube(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	cfg := tunnel.Config{FlushThreshold: 4096, FlushInterval: 2 * time.Millisecond}
	names := []string{"fast1", "fast2", "slow_dead"}

	fast1Client, fast1Server := newMemPair()
	fast2Client, fast2Server := newMemPair()
	slowClient, slowServer := newMemPair()

	lim := func() *rate.Limiter { return rate.NewLimiter(rate.Limit(16*1024), 16*1024) }
	clientFast1 := &throttledTransport{Transport: fast1Client, lim: lim()}
	clientFast2 := &throttledTransport{Transport: fast2Client, lim: lim()}
	gate := make(chan struct{})
	clientSlow := &delayedStartTransport{Transport: slowClient, gate: gate}
	serverSlow := &countingTransport{Transport: slowServer}

	srv := NewServer(names, nil, testLogger{}, cfg, nil)
	srv.SetSessionHandler(readingEcho)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	release1, _ := srv.register("fast1", "alice", fast1Server)
	if release1 == nil {
		t.Fatal("fast1 was not accepted")
	}
	defer release1()
	release2, _ := srv.register("fast2", "alice", fast2Server)
	if release2 == nil {
		t.Fatal("fast2 was not accepted")
	}
	defer release2()

	mux, err := NewMux(
		[]transport.Transport{clientFast1, clientFast2, clientSlow},
		names,
		testLogger{},
	)
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}

	start := time.Now()
	if err := mux.Start(ctx); err != nil {
		t.Fatalf("bond start: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("bond start waited for the slow transport: %s", elapsed)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = mux.Stop(stopCtx)
	}()

	conn := mux.session.acquire()
	if conn == nil {
		t.Fatal("bond produced no connection")
	}
	if err := conn.terminalErr(); err != nil {
		t.Fatalf("bond session is dead right after start: %v", err)
	}
	for _, snap := range conn.sched.Snapshot() {
		if snap.Name == "slow_dead" {
			t.Fatal("the slow transport joined the pool before it was started")
		}
	}

	stream, err := mux.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))

	probe := []byte("dns-query")
	if _, err := stream.Write(probe); err != nil {
		t.Fatalf("write before revival: %v", err)
	}
	reply := make([]byte, len(probe))
	if _, err := io.ReadFull(stream, reply); err != nil {
		t.Fatalf("read before revival: %v", err)
	}
	if !bytes.Equal(reply, probe) {
		t.Fatalf("reply = %q, want %q", reply, probe)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("first round trip took %s: traffic did not start immediately", elapsed)
	}

	close(gate)
	release3, done3 := srv.register("slow_dead", "alice", serverSlow)
	if release3 == nil {
		t.Fatal("slow_dead was not accepted after revival")
	}
	defer release3()

	waitFor(t, 10*time.Second, "revived tube joined the client pool", func() bool {
		for _, snap := range mux.session.acquire().sched.Snapshot() {
			if snap.Name == "slow_dead" && !snap.Dead {
				return true
			}
		}
		return false
	})

	payload := bytes.Repeat([]byte("bond-recovery-"), 8192)
	writeErr := make(chan error, 1)
	go func() {
		_, err := stream.Write(payload)
		writeErr <- err
	}()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("read after revival: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write after revival: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("echo mismatch after revival")
	}

	waitFor(t, 10*time.Second, "revived tube carried packets", func() bool {
		for _, snap := range mux.session.acquire().sched.Snapshot() {
			if snap.Name == "slow_dead" {
				return snap.SentBytes > 0
			}
		}
		return false
	})
	waitFor(t, 10*time.Second, "revived tube delivered packets", func() bool {
		return serverSlow.received.Load() > 0
	})

	if err := conn.terminalErr(); err != nil {
		t.Fatalf("bond session died during recovery: %v", err)
	}
	select {
	case <-done3:
		t.Fatal("the aggregate was torn down when the tube was revived")
	default:
	}
}
