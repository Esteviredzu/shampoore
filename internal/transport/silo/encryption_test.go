package silo

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
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

var _ logging.ContextLogger = testLogger{}

type fakePipe struct {
	mu     sync.Mutex
	maxMsg int
	sent   [][]byte
	recvCh chan []byte
}

func newFakePipe() *fakePipe {
	return &fakePipe{maxMsg: 1 << 20, recvCh: make(chan []byte, 64)}
}

func (p *fakePipe) Start(context.Context) error { return nil }
func (p *fakePipe) Stop(context.Context) error  { return nil }

func (p *fakePipe) SendMessage(_ context.Context, data []byte, _ string) error {
	cp := append([]byte(nil), data...)
	p.mu.Lock()
	p.sent = append(p.sent, cp)
	p.mu.Unlock()
	return nil
}

func (p *fakePipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case data := <-p.recvCh:
		return data, nil
	}
}

func (p *fakePipe) DeleteMessage(context.Context, string) error { return nil }
func (p *fakePipe) SessionID() string                           { return "fake" }
func (p *fakePipe) MaxMessageSize() int                         { return p.maxMsg }

func (p *fakePipe) push(data []byte) {
	p.recvCh <- data
}

func (p *fakePipe) sentMessages() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]byte, len(p.sent))
	for i, msg := range p.sent {
		out[i] = append([]byte(nil), msg...)
	}
	return out
}

func TestWrapEncryption(t *testing.T) {
	inner := newFakePipe()

	plain, err := wrapEncryption(inner, "", "client", testLogger{})
	if err != nil {
		t.Fatalf("wrap without password: %v", err)
	}
	if plain != inner {
		t.Fatal("wrap without password returned a different pipe")
	}

	secured, err := wrapEncryption(inner, "secret", "client", testLogger{})
	if err != nil {
		t.Fatalf("wrap with password: %v", err)
	}
	if _, ok := secured.(*security.SecurePipe); !ok {
		t.Fatalf("wrapped pipe type = %T, want *security.SecurePipe", secured)
	}
}

func TestSiloEncryptedRoundTrip(t *testing.T) {
	clientInner := newFakePipe()
	serverInner := newFakePipe()

	clientPipe, err := wrapEncryption(clientInner, "secret", "client", testLogger{})
	if err != nil {
		t.Fatalf("client wrap: %v", err)
	}
	serverPipe, err := wrapEncryption(serverInner, "secret", "server", testLogger{})
	if err != nil {
		t.Fatalf("server wrap: %v", err)
	}

	client := newTransport(clientPipe)
	server := newTransport(serverPipe)
	ctx := context.Background()

	if err := client.Send(ctx, []byte("hello silo")); err != nil {
		t.Fatalf("client send: %v", err)
	}
	sent := clientInner.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("client sent %d frames, want 1", len(sent))
	}
	if bytes.Contains(sent[0], []byte("hello silo")) {
		t.Fatal("silo transport sent plaintext over the encrypted pipe")
	}
	serverInner.push(sent[0])

	got, err := server.Receive(ctx)
	if err != nil {
		t.Fatalf("server receive: %v", err)
	}
	if !bytes.Equal(got, []byte("hello silo")) {
		t.Fatalf("server got %q", got)
	}
}

func TestConfigValidateEncryptionPassword(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AccessKey = "key"
	cfg.SecretKey = "secret"
	cfg.EncryptionPassword = "   "
	if err := cfg.Validate(); err == nil {
		t.Fatal("blank encryption password was accepted")
	}
}
