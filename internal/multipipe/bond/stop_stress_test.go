package bond

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type opaqueTransport struct {
	inner transport.Transport
	hook  func()
}

func (o opaqueTransport) Start(ctx context.Context) error             { return o.inner.Start(ctx) }
func (o opaqueTransport) Stop(ctx context.Context) error              { return o.inner.Stop(ctx) }
func (o opaqueTransport) Send(ctx context.Context, data []byte) error { return o.inner.Send(ctx, data) }
func (o opaqueTransport) Receive(ctx context.Context) ([]byte, error) { return o.inner.Receive(ctx) }
func (o opaqueTransport) MaxMessageSize() int                         { return o.inner.MaxMessageSize() }
func (o opaqueTransport) BytesDelivered() int64                       { return 0 }
func (o opaqueTransport) BytesAcked() int64                           { return 0 }

type blockedStartTransport struct {
	entered chan struct{}
	oncer   atomic.Bool
	stopped atomic.Int32
}

func newBlockedStartTransport() *blockedStartTransport {
	return &blockedStartTransport{entered: make(chan struct{}, 1)}
}

func (b *blockedStartTransport) Start(ctx context.Context) error {
	if b.oncer.CompareAndSwap(false, true) {
		select {
		case b.entered <- struct{}{}:
		default:
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (b *blockedStartTransport) Stop(context.Context) error {
	b.stopped.Add(1)
	return nil
}

func (b *blockedStartTransport) Send(context.Context, []byte) error { return io.ErrClosedPipe }

func (b *blockedStartTransport) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *blockedStartTransport) MaxMessageSize() int   { return 60000 }
func (b *blockedStartTransport) BytesDelivered() int64 { return 0 }
func (b *blockedStartTransport) BytesAcked() int64     { return 0 }

type failingTransport struct{ *memEnd }

func (f failingTransport) Send(context.Context, []byte) error { return errors.New("tube is down") }

type deafTransport struct{ *memEnd }

func (d deafTransport) Send(ctx context.Context, _ []byte) error {
	<-ctx.Done()
	return ctx.Err()
}

func (d deafTransport) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func stressMuxConfig() tunnel.Config {
	return tunnel.Config{FlushThreshold: 4096, FlushInterval: 2 * time.Millisecond}
}

func TestMuxStopSurvivesUncomparableTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientA, serverA := newMemPair()
	mux, err := NewMux(
		[]transport.Transport{opaqueTransport{inner: clientA, hook: func() {}}},
		[]string{"0-jellyfin"},
		testLogger{},
	)
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}
	srv := NewServer([]string{"0-jellyfin"}, nil, testLogger{}, stressMuxConfig(), nil)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()
	release, _ := srv.register("0-jellyfin", "alice", serverA)
	if release == nil {
		t.Fatal("tube was not accepted")
	}
	defer release()

	if err := mux.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := mux.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestMuxStopDuringStartDoesNotHang(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	slow := newBlockedStartTransport()
	mux, err := NewMux([]transport.Transport{slow}, []string{"0-silent"}, testLogger{})
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}

	startErr := make(chan error, 1)
	go func() { startErr <- mux.Start(ctx) }()

	select {
	case <-slow.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("transport never entered Start")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	stopped := make(chan error, 1)
	go func() { stopped <- mux.Stop(stopCtx) }()

	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Mux.Stop hung while Start was still waiting for its first tube")
	}

	select {
	case <-startErr:
	case <-time.After(3 * time.Second):
		t.Fatal("Mux.Start did not return after the stop")
	}
	if slow.stopped.Load() == 0 {
		t.Fatal("the cancelled subflow was not stopped")
	}
}

func TestMuxStartStopStress(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 40; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		liveA, serverA := newMemPair()
		liveB, serverB := newMemPair()
		deadC, serverC := newMemPair()

		srv := NewServer([]string{"0-raw", "1-jellyfin", "2-dead"}, nil, testLogger{}, stressMuxConfig(), nil)
		if err := srv.Start(ctx); err != nil {
			cancel()
			t.Fatalf("iteration %d: server start: %v", i, err)
		}
		releases := make([]func(), 0, 3)
		tubes := []struct {
			name string
			tr   transport.Transport
		}{
			{"0-raw", serverA},
			{"1-jellyfin", serverB},
			{"2-dead", deafTransport{serverC}},
		}
		for _, tube := range tubes {
			release, _ := srv.register(tube.name, "alice", tube.tr)
			if release != nil {
				releases = append(releases, release)
			}
		}

		mux, err := NewMux(
			[]transport.Transport{liveA, failingTransport{liveB}, deadC},
			[]string{"0-raw", "1-jellyfin", "2-dead"},
			testLogger{},
		)
		if err != nil {
			cancel()
			t.Fatalf("iteration %d: new mux: %v", i, err)
		}

		started := make(chan error, 1)
		go func() { started <- mux.Start(ctx) }()

		if i%2 == 0 {
			time.Sleep(time.Duration(i%5) * time.Millisecond)
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := mux.Stop(stopCtx); err != nil {
			stopCancel()
			cancel()
			t.Fatalf("iteration %d: stop: %v", i, err)
		}
		stopCancel()

		select {
		case <-started:
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("iteration %d: start did not return", i)
		}

		for _, release := range releases {
			release()
		}
		stopSrvCtx, stopSrvCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = srv.Stop(stopSrvCtx)
		stopSrvCancel()
		cancel()
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		after := runtime.NumGoroutine()
		if after <= before+20 {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Logf("goroutine dump:\n%s", buf[:n])
			t.Fatalf("goroutines leaked: before=%d after=%d", before, after)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
