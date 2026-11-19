package reliability

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type testLogger struct{}

func (testLogger) Trace(...any)                         {}
func (testLogger) Debug(...any)                         {}
func (testLogger) Info(...any)                          {}
func (testLogger) Warn(...any)                          {}
func (testLogger) Error(...any)                         {}
func (testLogger) Fatal(...any)                         {}
func (testLogger) Panic(...any)                         {}
func (testLogger) TraceContext(context.Context, ...any) {}
func (testLogger) DebugContext(context.Context, ...any) {}
func (testLogger) InfoContext(context.Context, ...any)  {}
func (testLogger) WarnContext(context.Context, ...any)  {}
func (testLogger) ErrorContext(context.Context, ...any) {}
func (testLogger) FatalContext(context.Context, ...any) {}
func (testLogger) PanicContext(context.Context, ...any) {}

type fakePipe struct {
	mu       sync.Mutex
	maxMsg   int
	sent     int
	received chan []byte
}

func newFakePipe() *fakePipe {
	return &fakePipe{maxMsg: 1 << 20, received: make(chan []byte, 16)}
}

func (p *fakePipe) Start(context.Context) error { return nil }
func (p *fakePipe) Stop(context.Context) error  { return nil }

func (p *fakePipe) SendMessage(_ context.Context, _ []byte, _ string) error {
	p.mu.Lock()
	p.sent++
	p.mu.Unlock()
	return nil
}

func (p *fakePipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case data := <-p.received:
		return data, nil
	}
}

func (p *fakePipe) DeleteMessage(context.Context, string) error { return nil }
func (p *fakePipe) SessionID() string                           { return "fake" }
func (p *fakePipe) MaxMessageSize() int                         { return p.maxMsg }

func (p *fakePipe) sentCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent
}

func newTestSession(t *testing.T, chunkSize int) (*ReliableSession, *fakePipe) {
	t.Helper()
	pipe := newFakePipe()
	s, err := NewReliableSession(pipe, testLogger{}, Config{ChunkSize: chunkSize})
	if err != nil {
		t.Fatalf("NewReliableSession: %v", err)
	}
	s.ctx = context.Background()
	s.ownSessionID = uuid.NewString()
	return s, pipe
}

func TestProcessChunkRejectsTooManyChunks(t *testing.T) {
	s, _ := newTestSession(t, 512)

	s.processChunk(&DecodedPacket{
		Kind:        "data",
		SessionID:   uuid.NewString(),
		Sequence:    0,
		ChunkIndex:  0,
		TotalChunks: s.maxChunksPerMessage + 1,
		Data:        []byte("x"),
	})

	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	if len(s.incomingChunks) != 0 {
		t.Fatalf("incomingChunks = %d, want 0", len(s.incomingChunks))
	}
	if s.incomingBytes != 0 {
		t.Fatalf("incomingBytes = %d, want 0", s.incomingBytes)
	}
}

func TestProcessChunkRejectsOversizedMessage(t *testing.T) {
	s, _ := newTestSession(t, 512)

	s.processChunk(&DecodedPacket{
		Kind:        "data",
		SessionID:   uuid.NewString(),
		Sequence:    0,
		ChunkIndex:  0,
		TotalChunks: 1,
		Data:        make([]byte, maxAssembledMessageSize+1),
	})

	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	if len(s.incomingChunks) != 0 {
		t.Fatalf("incomingChunks = %d, want 0", len(s.incomingChunks))
	}
	if s.incomingBytes != 0 {
		t.Fatalf("incomingBytes = %d, want 0", s.incomingBytes)
	}
}

func TestProcessChunkRejectsCumulativeOverflow(t *testing.T) {
	s, _ := newTestSession(t, 512)

	sessionID := uuid.NewString()
	part := make([]byte, maxAssembledMessageSize/2+1)
	for i := 0; i < 2; i++ {
		s.processChunk(&DecodedPacket{
			Kind:        "data",
			SessionID:   sessionID,
			Sequence:    0,
			ChunkIndex:  i,
			TotalChunks: 3,
			Data:        part,
		})
	}

	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	if len(s.incomingChunks) != 0 {
		t.Fatalf("partial message was not dropped: %d", len(s.incomingChunks))
	}
	if s.incomingBytes != 0 {
		t.Fatalf("incomingBytes = %d, want 0", s.incomingBytes)
	}
}

func TestProcessChunkAssemblesMessage(t *testing.T) {
	s, _ := newTestSession(t, 512)

	want := bytes.Repeat([]byte("abc"), 300)
	chunks := [][]byte{want[:400], want[400:800], want[800:]}
	sessionID := uuid.NewString()

	for i, chunk := range chunks {
		s.processChunk(&DecodedPacket{
			Kind:        "data",
			SessionID:   sessionID,
			Sequence:    0,
			ChunkIndex:  i,
			TotalChunks: len(chunks),
			Data:        chunk,
		})
	}

	got, err := s.Receive(context.Background())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("assembled message mismatch: got %d bytes, want %d", len(got), len(want))
	}

	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	if s.incomingBytes != 0 {
		t.Fatalf("incomingBytes = %d, want 0 after assembly", s.incomingBytes)
	}
}

func TestDeliveryDoesNotHoldRecvMu(t *testing.T) {
	s, _ := newTestSession(t, 512)

	for i := 0; i < cap(s.receiveQueue); i++ {
		s.receiveQueue <- receivedPacket{}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.processChunk(&DecodedPacket{
			Kind:        "data",
			SessionID:   uuid.NewString(),
			Sequence:    0,
			ChunkIndex:  0,
			TotalChunks: 1,
			Data:        []byte("blocked"),
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		s.recvMu.Lock()
		advanced := s.nextReceiveSequence == 1
		s.recvMu.Unlock()
		if advanced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("processChunk did not advance the receive window")
		}
		time.Sleep(time.Millisecond)
	}

	lockDeadline := time.Now().Add(time.Second)
	for {
		if s.recvMu.TryLock() {
			s.recvMu.Unlock()
			break
		}
		if time.Now().After(lockDeadline) {
			t.Fatal("recvMu stayed locked during blocking delivery")
		}
		time.Sleep(time.Millisecond)
	}

	<-s.receiveQueue

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processChunk did not finish after the queue was drained")
	}
}

func TestAckBatchingEnabledByDefault(t *testing.T) {
	if got := DefaultConfig().AckBatchMs; got <= 0 {
		t.Fatalf("DefaultConfig().AckBatchMs = %d, want > 0", got)
	}
	s, _ := newTestSession(t, 512)
	if s.cfg.AckBatchMs != defaultAckBatchMs {
		t.Fatalf("AckBatchMs = %d, want %d", s.cfg.AckBatchMs, defaultAckBatchMs)
	}
}

func TestUnexpectedRemoteSessionsBounded(t *testing.T) {
	s, _ := newTestSession(t, 512)

	for i := 0; i < maxUnexpectedRemoteSessions*4; i++ {
		s.remoteMu.Lock()
		s.lastSessionChange = time.Time{}
		s.remoteMu.Unlock()
		if !s.acceptRemoteSession(uuid.NewString()) {
			t.Fatalf("session %d was rejected", i)
		}
	}

	s.remoteMu.Lock()
	size := len(s.unexpectedRemoteSessions)
	s.remoteMu.Unlock()
	if size > maxUnexpectedRemoteSessions {
		t.Fatalf("unexpectedRemoteSessions = %d, limit is %d",
			size, maxUnexpectedRemoteSessions)
	}
}

func TestConcurrentStartStop(t *testing.T) {
	pipe := newFakePipe()
	s, err := NewReliableSession(pipe, testLogger{}, Config{ChunkSize: 512})
	if err != nil {
		t.Fatalf("NewReliableSession: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(start bool) {
			defer wg.Done()
			if start {
				_ = s.Start(ctx)
				return
			}
			_ = s.Stop(ctx)
		}(i%2 == 0)
	}
	wg.Wait()

	if err := s.Stop(ctx); err != nil {
		t.Fatalf("final Stop: %v", err)
	}
}
func TestEncryptedSessionRejectsUnauthenticatedPackets(t *testing.T) {
	pipe := newFakePipe()
	s, err := NewReliableSession(pipe, testLogger{}, Config{
		ChunkSize:          512,
		EncryptionPassword: "secret",
		Role:               "server",
	})
	if err != nil {
		t.Fatalf("NewReliableSession: %v", err)
	}
	s.ctx = context.Background()
	s.ownSessionID = uuid.NewString()

	codec, err := NewReliableCodec(512)
	if err != nil {
		t.Fatalf("NewReliableCodec: %v", err)
	}
	attacker := uuid.NewString()

	s.pipe = pipe

	s.sendMu.Lock()
	s.pendingPackets[0] = &pendingPacket{
		sequence: 0,
		data:     []byte("pending"),
		keys:     []string{"d:pending"},
	}
	s.sendMu.Unlock()

	ack, err := codec.EncodeACK(s.ownSessionID, attacker, 0)
	if err != nil {
		t.Fatalf("EncodeACK: %v", err)
	}
	s.handleRaw(ack)

	s.remoteMu.Lock()
	remote := s.remoteSessionID
	s.remoteMu.Unlock()
	if remote != "" {
		t.Fatalf("remote session was registered from an unauthenticated ACK: %s", remote)
	}

	s.sendMu.Lock()
	_, stillPending := s.pendingPackets[0]
	s.sendMu.Unlock()
	if !stillPending {
		t.Fatal("unauthenticated ACK released a pending packet")
	}

	chunks, err := codec.Encode(attacker, 0, []byte("spoofed data"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	s.handleRaw(chunks[0])

	s.remoteMu.Lock()
	remote = s.remoteSessionID
	s.remoteMu.Unlock()
	if remote != "" {
		t.Fatalf("remote session was registered from unauthenticated data: %s", remote)
	}
}

func TestUnencryptedSessionStillAcceptsPackets(t *testing.T) {
	pipe := newFakePipe()
	s, err := NewReliableSession(pipe, testLogger{}, Config{ChunkSize: 512})
	if err != nil {
		t.Fatalf("NewReliableSession: %v", err)
	}
	s.ctx = context.Background()
	s.ownSessionID = uuid.NewString()
	s.pipe = pipe

	codec, err := NewReliableCodec(512)
	if err != nil {
		t.Fatalf("NewReliableCodec: %v", err)
	}
	sender := uuid.NewString()
	ack, err := codec.EncodeACK(s.ownSessionID, sender, 0)
	if err != nil {
		t.Fatalf("EncodeACK: %v", err)
	}
	s.handleRaw(ack)

	s.remoteMu.Lock()
	remote := s.remoteSessionID
	s.remoteMu.Unlock()
	if remote != sender {
		t.Fatalf("remote session = %q, want %q", remote, sender)
	}
}
