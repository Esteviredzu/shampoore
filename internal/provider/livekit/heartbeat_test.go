package livekit

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

func TestPushIncomingFiltersHeartbeat(t *testing.T) {
	p := newPipe(nil, "topic", "remote")
	defer func() { _ = p.Stop(context.Background()) }()

	p.PushIncoming(heartbeatPayload, "remote")
	if got := len(p.recvCh); got != 0 {
		t.Fatalf("heartbeat reached the mux queue: %d", got)
	}
	if !p.peerHeartbeat.Load() {
		t.Fatal("heartbeat did not enable peer detection")
	}
	snap := p.ctr.Snapshot()
	if snap.MessagesRecv != 0 || snap.UsefulRecv != 0 {
		t.Fatalf("heartbeat was accounted as payload: %+v", snap)
	}

	p.PushIncoming([]byte("payload"), "remote")
	if got := len(p.recvCh); got != 1 {
		t.Fatalf("payload was not enqueued: %d", got)
	}
}

func TestSignalResetDropsStaleDataAndInterruptsReceive(t *testing.T) {
	p := newPipe(nil, "topic", "")
	defer func() { _ = p.Stop(context.Background()) }()

	p.recvCh <- []byte("stale")

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := p.ReceiveMessage(ctx)
		done <- err
	}()

	p.SignalReset(fmt.Errorf("%w: test reset", transport.ErrTransportReset))

	select {
	case err := <-done:
		if !errors.Is(err, transport.ErrTransportReset) {
			t.Fatalf("receive error = %v, want ErrTransportReset", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReceiveMessage was not interrupted by SignalReset")
	}
}

func TestHeartbeatWatchdogFiresDeadHandler(t *testing.T) {
	p := newPipe(nil, "topic", "remote")
	p.heartbeatInterval = 10 * time.Millisecond
	p.heartbeatTimeout = 40 * time.Millisecond
	p.peerHeartbeat.Store(true)

	var dead atomic.Int32
	p.SetDeadHandler(func() { dead.Add(1) })

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Stop(context.Background()) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && dead.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if dead.Load() == 0 {
		t.Fatal("watchdog did not report a dead connection")
	}

	time.Sleep(60 * time.Millisecond)
	if got := dead.Load(); got != 1 {
		t.Fatalf("dead handler fired %d times, want 1", got)
	}

	p.ResetLiveness()
	if p.deadFired.Load() {
		t.Fatal("ResetLiveness did not re-arm the watchdog")
	}
}

func TestPeerLeftDisarmsWatchdog(t *testing.T) {
	p := newPipe(nil, "topic", "remote")
	p.heartbeatInterval = 10 * time.Millisecond
	p.heartbeatTimeout = 20 * time.Millisecond
	p.peerHeartbeat.Store(true)

	var dead atomic.Int32
	p.SetDeadHandler(func() { dead.Add(1) })

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Stop(context.Background()) }()

	p.PeerLeft()
	if got := p.GetRemoteID(); got != "" {
		t.Fatalf("remote id = %q after PeerLeft", got)
	}
	time.Sleep(100 * time.Millisecond)
	if got := dead.Load(); got != 0 {
		t.Fatalf("watchdog fired %d times after an explicit peer departure", got)
	}
}

func TestPeerJoinedReportsOnlyNewPeers(t *testing.T) {
	p := newPipe(nil, "topic", "")
	defer func() { _ = p.Stop(context.Background()) }()

	if !p.PeerJoined("server-1") {
		t.Fatal("first peer join was not reported as new")
	}
	if p.PeerJoined("server-1") {
		t.Fatal("repeated join of the same peer was reported as new")
	}
	p.PeerLeft()
	if !p.PeerJoined("server-2") {
		t.Fatal("join after departure was not reported as new")
	}
}

func TestHeartbeatWatchdogIgnoresPeerWithoutHeartbeats(t *testing.T) {
	p := newPipe(nil, "topic", "remote")
	p.heartbeatInterval = 10 * time.Millisecond
	p.heartbeatTimeout = 20 * time.Millisecond

	var dead atomic.Int32
	p.SetDeadHandler(func() { dead.Add(1) })

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Stop(context.Background()) }()

	time.Sleep(100 * time.Millisecond)
	if got := dead.Load(); got != 0 {
		t.Fatalf("watchdog fired %d times without peer heartbeat support", got)
	}
}
