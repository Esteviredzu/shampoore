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

func TestPushIncomingAdoptsUnknownSender(t *testing.T) {
	p := newPipe(nil, "topic", "")
	defer func() { _ = p.Stop(context.Background()) }()

	if got := p.GetRemoteID(); got != "" {
		t.Fatalf("remote id before any packet = %q, want empty", got)
	}
	p.PushIncoming([]byte("payload"), "peer-42")
	if got := p.GetRemoteID(); got != "peer-42" {
		t.Fatalf("remote id after incoming data = %q, want peer-42", got)
	}

	p.PushIncoming([]byte("intruder"), "someone-else")
	if got := p.GetRemoteID(); got != "peer-42" {
		t.Fatalf("remote id changed by a foreign sender: %q", got)
	}
	if got := len(p.recvCh); got != 1 {
		t.Fatalf("foreign packet reached the queue: %d", got)
	}
}

func TestSignalResetDropsStaleDataAndInterruptsReceive(t *testing.T) {
	p := newPipe(nil, "topic", "")
	defer func() { _ = p.Stop(context.Background()) }()

	p.recvCh <- []byte("stale")
	p.SignalReset(fmt.Errorf("%w: test reset", transport.ErrTransportReset))
	if got := len(p.recvCh); got != 0 {
		t.Fatalf("stale data was not drained: %d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := p.ReceiveMessage(ctx)
	cancel()
	if !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("receive error = %v, want ErrTransportReset", err)
	}

	blocked := newPipe(nil, "topic", "")
	defer func() { _ = blocked.Stop(context.Background()) }()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := blocked.ReceiveMessage(ctx)
		done <- err
	}()

	blocked.SignalReset(fmt.Errorf("%w: test reset", transport.ErrTransportReset))

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
	p := newPipe(nil, "topic", "")
	p.heartbeatInterval = 10 * time.Millisecond
	p.heartbeatTimeout = 40 * time.Millisecond
	p.PeerJoined("remote")

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
	p := newPipe(nil, "topic", "")
	p.heartbeatInterval = 10 * time.Millisecond
	p.heartbeatTimeout = 20 * time.Millisecond
	p.PeerJoined("remote")

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
	if p.PeerPresent() {
		t.Fatal("peer is still reported present after PeerLeft")
	}
	time.Sleep(100 * time.Millisecond)
	if got := dead.Load(); got != 0 {
		t.Fatalf("watchdog fired %d times after an explicit peer departure", got)
	}
}

func TestPushIncomingRepinsStalePeerBeforeFirstExchange(t *testing.T) {
	p := newPipe(nil, "topic", "relay-server")
	p.SetAdoptFirstPacket(true)
	defer func() { _ = p.Stop(context.Background()) }()

	p.PushIncoming([]byte("hello"), "mobile-client")
	if got := p.GetRemoteID(); got != "mobile-client" {
		t.Fatalf("remote id = %q, want the first real sender", got)
	}
	if got := len(p.recvCh); got != 1 {
		t.Fatalf("first packet from the real client was dropped: queue=%d", got)
	}

	p.PushIncoming([]byte("intruder"), "someone-else")
	if got := p.GetRemoteID(); got != "mobile-client" {
		t.Fatalf("remote id was replaced by a foreign sender: %q", got)
	}
	if got := len(p.recvCh); got != 1 {
		t.Fatalf("foreign packet reached the queue: %d", got)
	}

	q := newPipe(nil, "topic", "alice")
	defer func() { _ = q.Stop(context.Background()) }()
	q.PushIncoming([]byte("hello"), "relay-server")
	if got := q.GetRemoteID(); got != "alice" {
		t.Fatalf("server pin was replaced: %q", got)
	}
	if got := len(q.recvCh); got != 0 {
		t.Fatalf("packet from a foreign sender reached the pinned pipe: %d", got)
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

	p := newPipe(nil, "topic", "")
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
		t.Fatalf("watchdog fired %d times without a peer", got)
	}
}

func TestHeartbeatWatchdogFiresForSilentRemote(t *testing.T) {
	p := newPipe(nil, "topic", "")
	p.heartbeatInterval = 10 * time.Millisecond
	p.heartbeatTimeout = 20 * time.Millisecond

	var dead atomic.Int32
	p.SetDeadHandler(func() { dead.Add(1) })

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Stop(context.Background()) }()

	p.PeerJoined("remote")
	p.PushIncoming([]byte("payload"), "remote")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && dead.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if dead.Load() == 0 {
		t.Fatal("watchdog ignored a silent peer that joined the room")
	}
}

func TestExitSessionWithoutPeerOutlivesHeartbeatTimeout(t *testing.T) {
	p := newPipe(nil, "topic", "")
	p.heartbeatInterval = 5 * time.Millisecond
	p.heartbeatTimeout = 20 * time.Millisecond

	var dead atomic.Int32
	p.SetDeadHandler(func() { dead.Add(1) })

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = p.Stop(context.Background()) }()

	time.Sleep(10 * p.heartbeatTimeout)
	if got := dead.Load(); got != 0 {
		t.Fatalf("watchdog fired %d times in an empty room", got)
	}
	if p.PeerPresent() {
		t.Fatal("peer is reported present in an empty room")
	}

	if !p.PeerJoined("client-1") {
		t.Fatal("first peer join was not reported as new")
	}
	p.PushIncoming(heartbeatPayload, "client-1")
	if !p.PeerPresent() {
		t.Fatal("peer is not reported present after its first packet")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && dead.Load() == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	if got := dead.Load(); got != 1 {
		t.Fatalf("watchdog fired %d times for a silent peer, want 1", got)
	}

	p.PeerLeft()
	time.Sleep(10 * p.heartbeatTimeout)
	if got := dead.Load(); got != 1 {
		t.Fatalf("watchdog fired %d times after PeerLeft, want 1", got)
	}
	if p.PeerPresent() || p.GetRemoteID() != "" {
		t.Fatal("peer state was not cleared on PeerLeft")
	}

	if !p.PeerJoined("client-2") {
		t.Fatal("the room did not accept the next peer after PeerLeft")
	}
	p.PushIncoming([]byte("next"), "client-2")
	select {
	case data := <-p.recvCh:
		if string(data) != "next" {
			t.Fatalf("delivered %q, want next", data)
		}
	default:
		t.Fatal("traffic from the reconnected peer was not delivered")
	}
	time.Sleep(p.heartbeatTimeout / 2)
	if got := dead.Load(); got != 1 {
		t.Fatalf("watchdog fired %d times right after a reconnect, want 1", got)
	}
}
