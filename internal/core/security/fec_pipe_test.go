package security

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

type testPipe struct {
	mu       sync.Mutex
	maxMsg   int
	recvCh   chan []byte
	sent     [][]byte
	received int
}

func newTestPipe() *testPipe {
	return &testPipe{maxMsg: 1 << 20, recvCh: make(chan []byte, 64)}
}

func (p *testPipe) Start(context.Context) error { return nil }
func (p *testPipe) Stop(context.Context) error  { return nil }

func (p *testPipe) SendMessage(_ context.Context, data []byte, _ string) error {
	copied := append([]byte(nil), data...)
	p.mu.Lock()
	p.sent = append(p.sent, copied)
	p.mu.Unlock()
	return nil
}

func (p *testPipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case data := <-p.recvCh:
		p.mu.Lock()
		p.received++
		p.mu.Unlock()
		return data, nil
	}
}

func (p *testPipe) DeleteMessage(context.Context, string) error { return nil }
func (p *testPipe) SessionID() string                           { return "fake" }
func (p *testPipe) MaxMessageSize() int                         { return p.maxMsg }

func (p *testPipe) push(data []byte) {
	p.recvCh <- data
}

func (p *testPipe) receivedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.received
}

func (p *testPipe) sentMessages() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]byte, len(p.sent))
	for i, msg := range p.sent {
		out[i] = append([]byte(nil), msg...)
	}
	return out
}

func makeShard(seq uint32, idx, total int, payload []byte) []byte {
	buf := make([]byte, fecShardHeaderSize+len(payload))
	buf[0] = fecTypeShard
	binary.BigEndian.PutUint32(buf[1:5], seq)
	buf[5] = byte(idx)
	buf[6] = byte(total)
	copy(buf[fecShardHeaderSize:], payload)
	return buf
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRecvGroupsExpireByTTL(t *testing.T) {
	pipe := newTestPipe()
	p, err := NewFECPipe(pipe, 2, 1)
	if err != nil {
		t.Fatalf("NewFECPipe: %v", err)
	}

	var clockMu sync.Mutex
	now := time.Unix(1000, 0)
	p.now = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	p.groupTTL = time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = p.ReceiveMessage(ctx)
	}()

	pipe.push(makeShard(1, 0, 3, []byte("a")))
	waitFor(t, "group 1", func() bool {
		p.recvMu.Lock()
		defer p.recvMu.Unlock()
		_, ok := p.recvGroups[1]
		return ok
	})

	clockMu.Lock()
	now = now.Add(2 * time.Second)
	clockMu.Unlock()

	pipe.push(makeShard(2, 0, 3, []byte("b")))
	waitFor(t, "group 2", func() bool {
		p.recvMu.Lock()
		defer p.recvMu.Unlock()
		_, ok := p.recvGroups[2]
		return ok
	})

	p.recvMu.Lock()
	_, stale := p.recvGroups[1]
	size := len(p.recvGroups)
	p.recvMu.Unlock()

	if stale {
		t.Fatal("expired group 1 is still present")
	}
	if size != 1 {
		t.Fatalf("recvGroups = %d, want 1 after TTL cleanup", size)
	}
}

func TestRecvGroupsBounded(t *testing.T) {
	pipe := newTestPipe()
	p, err := NewFECPipe(pipe, 2, 1)
	if err != nil {
		t.Fatalf("NewFECPipe: %v", err)
	}
	p.maxRecvGroups = 4
	p.groupTTL = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = p.ReceiveMessage(ctx)
	}()

	const packets = 20
	for i := 0; i < packets; i++ {

		pipe.push(makeShard(uint32(i+1), 0, 3, []byte("x")))
	}

	waitFor(t, "all shards processed", func() bool {
		return pipe.receivedCount() == packets
	})

	p.recvMu.Lock()
	size := len(p.recvGroups)
	p.recvMu.Unlock()

	if size > p.maxRecvGroups {
		t.Fatalf("recvGroups = %d, limit is %d", size, p.maxRecvGroups)
	}
	if size == 0 {
		t.Fatal("expected some incomplete groups to remain")
	}
}

func TestFECReassemblesData(t *testing.T) {
	pipe := newTestPipe()
	p, err := NewFECPipe(pipe, 3, 1)
	if err != nil {
		t.Fatalf("NewFECPipe: %v", err)
	}

	data := bytes.Repeat([]byte("z"), 399)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.SendMessage(ctx, data, "peer"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	for _, msg := range pipe.sentMessages() {
		pipe.push(msg)
	}

	got, err := p.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("reassembled data mismatch: got %d bytes, want %d", len(got), len(data))
	}
}
