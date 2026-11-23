package bond

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type memLink struct {
	a2b chan []byte
	b2a chan []byte
}

type memEnd struct {
	send    chan<- []byte
	recv    <-chan []byte
	stopped atomic.Bool
}

func (m *memEnd) Start(context.Context) error { return nil }
func (m *memEnd) Stop(context.Context) error {
	m.stopped.Store(true)
	return nil
}
func (m *memEnd) Send(ctx context.Context, data []byte) error {
	cp := append([]byte(nil), data...)
	select {
	case m.send <- cp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *memEnd) Receive(ctx context.Context) ([]byte, error) {
	select {
	case data := <-m.recv:
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (m *memEnd) MaxMessageSize() int   { return 60000 }
func (m *memEnd) BytesDelivered() int64 { return 0 }
func (m *memEnd) BytesAcked() int64     { return 0 }

func newMemPair() (client *memEnd, server *memEnd) {
	link := &memLink{a2b: make(chan []byte, 128), b2a: make(chan []byte, 128)}
	return &memEnd{send: link.a2b, recv: link.b2a}, &memEnd{send: link.b2a, recv: link.a2b}
}

func TestServerAggregatesTubesForOneUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientA, serverA := newMemPair()
	clientB, serverB := newMemPair()

	cfg := tunnel.Config{FlushThreshold: 4096, FlushInterval: 2 * time.Millisecond}
	srv := NewServer([]string{"0-a", "1-b"}, nil, testLogger{}, cfg, nil)

	handled := make(chan error, 1)
	layersCh := make(chan []transport.Layer, 1)
	srv.SetSessionHandler(func(hctx context.Context, mux multiplexing.StreamMultiplexer) error {
		stream, err := mux.AcceptStream(hctx)
		if err != nil {
			handled <- err
			return err
		}
		defer stream.Close()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(stream, buf); err != nil {
			handled <- err
			return err
		}
		if string(buf) != "ping" {
			err := fmt.Errorf("unexpected payload %q", buf)
			handled <- err
			return err
		}
		_, err = stream.Write([]byte("pong"))
		layersCh <- srv.WalkLayers()
		handled <- err
		return err
	})

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	releaseA, doneA := srv.register("0-a", "alice", serverA)
	if releaseA == nil || doneA == nil {
		t.Fatal("first tube was not accepted")
	}
	defer releaseA()

	select {
	case err := <-handled:
		t.Fatalf("session started with a single tube: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseB, _ := srv.register("1-b", "alice", serverB)
	if releaseB == nil {
		t.Fatal("second tube was not accepted")
	}
	defer releaseB()

	clientConn := NewBondConn(ctx, []string{"0-a", "1-b"}, []tubeLike{clientA, clientB}, testLogger{})
	defer func() { _ = clientConn.Close() }()
	ct := &connTransport{c: clientConn, maxPayload: clientConn.MaxPayload(), numTubes: 2}
	clientMux := tunnel.New(ct, testLogger{}, cfg)
	if err := clientMux.Start(ctx); err != nil {
		t.Fatalf("client mux start: %v", err)
	}
	defer func() { _ = clientMux.Stop(context.Background()) }()

	stream, err := clientMux.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}

	reply := make([]byte, 4)
	if _, err := io.ReadFull(stream, reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if string(reply) != "pong" {
		t.Fatalf("reply = %q, want pong", reply)
	}

	select {
	case layers := <-layersCh:
		if len(layers) == 0 {
			t.Fatal("aggregator exposed no telemetry layers")
		}
		first := layers[0]
		if first.Kind != "mux" || first.Type != "bond" {
			t.Fatalf("first telemetry layer = %+v, want bond mux", first)
		}
		if first.Snapshot.UsefulRecv < 4 || first.Snapshot.UsefulSent < 4 {
			t.Fatalf("aggregator useful bytes = %+v, want >=4 in both directions",
				first.Snapshot)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("aggregator telemetry was not captured")
	}

	select {
	case err := <-handled:
		if err != nil {
			t.Fatalf("server handler: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not finish")
	}
}

func TestServerRendezvousTimeoutReleasesTubes(t *testing.T) {
	prev := aggregateRendezvousTimeout
	aggregateRendezvousTimeout = 50 * time.Millisecond
	t.Cleanup(func() { aggregateRendezvousTimeout = prev })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, serverA := newMemPair()
	srv := NewServer([]string{"0-a", "1-b"}, nil, testLogger{}, tunnel.Config{}, nil)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	_, doneA := srv.register("0-a", "alice", serverA)
	select {
	case <-doneA:
	case <-time.After(2 * time.Second):
		t.Fatal("partial aggregate was not released after the rendezvous timeout")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !serverA.stopped.Load() {
		if time.Now().After(deadline) {
			t.Fatal("tube transport was not stopped on rendezvous timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestServerRejectsTubeWhenNotStarted(t *testing.T) {
	srv := NewServer([]string{"0-a"}, nil, testLogger{}, tunnel.Config{}, nil)
	_, serverA := newMemPair()

	release, done := srv.register("0-a", "alice", serverA)
	if release != nil || done != nil {
		t.Fatal("sink accepted a tube before the aggregator was started")
	}
	if !serverA.stopped.Load() {
		t.Fatal("rejected tube transport was not stopped")
	}
}
