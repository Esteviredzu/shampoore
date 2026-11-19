package livekit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPushIncomingDropsWhenQueueFull(t *testing.T) {
	p := newPipe(nil, "topic", "")
	defer func() {
		if err := p.Stop(context.Background()); err != nil {
			t.Fatalf("stop: %v", err)
		}
	}()

	for i := 0; i < recvQueueSize; i++ {
		p.recvCh <- []byte{byte(i)}
	}
	if got := len(p.recvCh); got != recvQueueSize {
		t.Fatalf("queue not filled: %d", got)
	}

	started := time.Now()
	p.PushIncoming([]byte("dropped-packet"), "")
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("PushIncoming blocked for %s on a full queue", elapsed)
	}

	if got := p.Dropped(); got != 1 {
		t.Fatalf("dropped=%d, want 1", got)
	}
	if got := len(p.recvCh); got != recvQueueSize {
		t.Fatalf("queue length changed: %d", got)
	}
	snap := p.ctr.Snapshot()
	if snap.MessagesRecv != 0 || snap.UsefulRecv != 0 {
		t.Fatalf("dropped packet was accounted as received: %+v", snap)
	}
}

func TestPushIncomingFiltersForeignSenders(t *testing.T) {
	p := newPipe(nil, "topic", "remote-a")
	defer func() {
		if err := p.Stop(context.Background()); err != nil {
			t.Fatalf("stop: %v", err)
		}
	}()

	p.PushIncoming([]byte("foreign"), "remote-b")
	if got := p.Dropped(); got != 0 {
		t.Fatalf("foreign packet must be ignored, dropped=%d", got)
	}
	if got := len(p.recvCh); got != 0 {
		t.Fatalf("foreign packet was enqueued: %d", got)
	}

	p.PushIncoming([]byte("mine"), "remote-a")
	if got := len(p.recvCh); got != 1 {
		t.Fatalf("own packet was not enqueued: %d", got)
	}
}

func TestPushIncomingConcurrentWithReceiverAndStop(t *testing.T) {
	p := newPipe(nil, "topic", "")

	const (
		writers   = 4
		perWriter = 3000
		totalPkts = writers * perWriter
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received atomic.Int64
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			if _, err := p.ReceiveMessage(ctx); err != nil {
				return
			}
			received.Add(1)
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			payload := []byte{byte(w), 0x01, 0x02, 0x03}
			for i := 0; i < perWriter; i++ {
				p.PushIncoming(payload, "")
			}
		}(w)
	}
	wg.Wait()

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	<-recvDone

	for {
		select {
		case <-p.recvCh:
			received.Add(1)
			continue
		default:
		}
		break
	}

	if got := received.Load() + p.Dropped(); got != totalPkts {
		t.Fatalf("accounting mismatch: received=%d dropped=%d total=%d want=%d",
			received.Load(), p.Dropped(), got, totalPkts)
	}

	before := p.Dropped()
	p.PushIncoming([]byte("late"), "")
	if got := p.Dropped(); got != before {
		t.Fatalf("late packet counted as dropped after stop: %d -> %d", before, got)
	}
}

func TestStopIsIdempotentAndConcurrent(t *testing.T) {
	p := newPipe(nil, "topic", "")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				if err := p.Stop(context.Background()); err != nil {
					t.Errorf("stop: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	if _, err := p.ReceiveMessage(context.Background()); err == nil {
		t.Fatal("expected receive to fail on a stopped pipe")
	}
}
