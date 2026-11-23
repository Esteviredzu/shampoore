package bond

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

type barrierTransport struct {
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	startErr error

	mu      sync.Mutex
	started bool
	stopped bool
}

func newBarrierTransport(startErr error) *barrierTransport {
	return &barrierTransport{
		entered:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		startErr: startErr,
	}
}

func (b *barrierTransport) Start(ctx context.Context) error {
	select {
	case b.entered <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-b.release:
		if b.startErr != nil {
			return b.startErr
		}
		b.mu.Lock()
		b.started = true
		b.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *barrierTransport) Stop(context.Context) error {
	b.mu.Lock()
	b.stopped = true
	b.mu.Unlock()
	return nil
}

func (b *barrierTransport) unblock() {
	b.once.Do(func() { close(b.release) })
}

func (b *barrierTransport) Send(context.Context, []byte) error { return nil }

func (b *barrierTransport) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *barrierTransport) MaxMessageSize() int { return 65536 }

func (b *barrierTransport) BytesDelivered() int64 { return 0 }
func (b *barrierTransport) BytesAcked() int64     { return 0 }

func (b *barrierTransport) wasStarted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.started
}

func (b *barrierTransport) wasStopped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stopped
}

func TestConnManagerRecreatesFailedConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pathA := newFakeTube()
	pathB := newFakeTube()
	cm := newConnManager(ctx, []string{"0-a", "1-b"}, []tubeLike{pathA, pathB}, testLogger{})
	defer func() { _ = cm.Close() }()

	first := cm.acquire()
	if first == nil {
		t.Fatal("manager returned no connection")
	}
	first.fail(errors.New("all tubes died at once"))
	if first.terminalErr() == nil {
		t.Fatal("failed connection reports no terminal error")
	}

	ct := &connTransport{session: cm, maxPayload: cm.MaxPayload(), numTubes: 2}
	if err := ct.Send(ctx, []byte("after-reset")); err != nil {
		t.Fatalf("send after a dead connection: %v", err)
	}
	second := cm.acquire()
	if second == nil {
		t.Fatal("manager did not recreate the connection")
	}
	if second == first {
		t.Fatal("manager returned the dead connection instead of a fresh one")
	}
	if len(cm.retired) != 1 || cm.retired[0] != first.session {
		t.Fatalf("manager retired %v, want the dead session id %d", cm.retired, first.session)
	}
	if _, ok := second.retiredLocal[first.session]; !ok {
		t.Fatal("fresh connection does not know the retired session id: late frames of the old stream would be delivered")
	}
	if err := second.terminalErr(); err != nil {
		t.Fatalf("recreated connection is already dead: %v", err)
	}
	if !cm.takeReset() {
		t.Fatal("recreation was not signalled to the upper tunnel")
	}
	if cm.takeReset() {
		t.Fatal("recreation reset was reported twice")
	}
	waitFor(t, time.Second, "the fresh connection writes through live subflows", func() bool {
		return countSeq(pathA.sentFrames(), 0)+countSeq(pathB.sentFrames(), 0) > 0
	})
}

func TestMuxStartsSubflowsConcurrently(t *testing.T) {
	paths := []*barrierTransport{
		newBarrierTransport(nil),
		newBarrierTransport(nil),
		newBarrierTransport(nil),
	}
	names := []string{"0-livekit", "1-silo", "2-raw_ws"}
	mux, err := NewMux(
		[]transport.Transport{paths[0], paths[1], paths[2]},
		names,
		testLogger{},
	)
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	started := make(chan error, 1)
	go func() { started <- mux.Start(ctx) }()

	for i, path := range paths {
		select {
		case <-path.entered:
		case <-time.After(2 * time.Second):
			for _, p := range paths {
				p.unblock()
			}
			t.Fatalf("subflow %s did not enter Start: subflows are started sequentially", names[i])
		}
	}
	for _, path := range paths {
		path.unblock()
	}

	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("start: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Mux.Start did not return after every subflow was released")
	}
	for _, path := range paths {
		if !path.wasStarted() {
			t.Fatal("a subflow was not marked as started")
		}
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := mux.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestMuxStartFailureStopsStartedSubflows(t *testing.T) {
	ok := newBarrierTransport(nil)
	bad := newBarrierTransport(errors.New("boom"))
	mux, err := NewMux(
		[]transport.Transport{ok, bad},
		[]string{"0-a", "1-b"},
		testLogger{},
	)
	if err != nil {
		t.Fatalf("new mux: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	started := make(chan error, 1)
	go func() { started <- mux.Start(ctx) }()

	for _, path := range []*barrierTransport{ok, bad} {
		select {
		case <-path.entered:
		case <-time.After(2 * time.Second):
			ok.unblock()
			bad.unblock()
			t.Fatal("a subflow did not enter Start")
		}
	}
	ok.unblock()
	bad.unblock()

	select {
	case err := <-started:
		if err == nil || !strings.Contains(err.Error(), "start transport 1-b") {
			t.Fatalf("start error = %v, want a start transport 1-b failure", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Mux.Start did not return after a subflow failed")
	}
	if !ok.wasStopped() {
		t.Fatal("an already started subflow was not stopped after a start failure")
	}
}
