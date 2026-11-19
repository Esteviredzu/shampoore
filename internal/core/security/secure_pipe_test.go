package security

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/logging"
)

type noopLogger struct{}

func (noopLogger) Trace(...any)                         {}
func (noopLogger) Debug(...any)                         {}
func (noopLogger) Info(...any)                          {}
func (noopLogger) Warn(...any)                          {}
func (noopLogger) Error(...any)                         {}
func (noopLogger) Fatal(...any)                         {}
func (noopLogger) Panic(...any)                         {}
func (noopLogger) TraceContext(context.Context, ...any) {}
func (noopLogger) DebugContext(context.Context, ...any) {}
func (noopLogger) InfoContext(context.Context, ...any)  {}
func (noopLogger) WarnContext(context.Context, ...any)  {}
func (noopLogger) ErrorContext(context.Context, ...any) {}
func (noopLogger) FatalContext(context.Context, ...any) {}
func (noopLogger) PanicContext(context.Context, ...any) {}

var _ logging.ContextLogger = noopLogger{}

func mustSecurePipe(t *testing.T, inner *testPipe, password, role string) *SecurePipe {
	t.Helper()
	p, err := NewSecurePipeWithRole(inner, password, role, noopLogger{})
	if err != nil {
		t.Fatalf("NewSecurePipeWithRole(%s): %v", role, err)
	}
	return p
}

func TestSecurePipeDirectionalRoundTrip(t *testing.T) {
	ctx := context.Background()
	clientInner := newTestPipe()
	serverInner := newTestPipe()
	client := mustSecurePipe(t, clientInner, "secret", RoleClient)
	server := mustSecurePipe(t, serverInner, "secret", RoleServer)

	if err := client.SendMessage(ctx, []byte("hello server"), ""); err != nil {
		t.Fatalf("client send: %v", err)
	}
	sent := clientInner.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("client sent %d frames, want 1", len(sent))
	}
	serverInner.push(sent[0])

	got, err := server.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("server receive: %v", err)
	}
	if !bytes.Equal(got, []byte("hello server")) {
		t.Fatalf("server got %q", got)
	}

	if err := server.SendMessage(ctx, []byte("hello client"), ""); err != nil {
		t.Fatalf("server send: %v", err)
	}
	sent = serverInner.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("server sent %d frames, want 1", len(sent))
	}
	clientInner.push(sent[0])

	got, err = client.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("client receive: %v", err)
	}
	if !bytes.Equal(got, []byte("hello client")) {
		t.Fatalf("client got %q", got)
	}
}

func TestSecurePipeRejectsReflectedPacket(t *testing.T) {
	ctx := context.Background()
	clientInner := newTestPipe()
	client := mustSecurePipe(t, clientInner, "secret", RoleClient)

	if err := client.SendMessage(ctx, []byte("reflected"), ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	blob := clientInner.sentMessages()[0]
	clientInner.push(blob)

	recvCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := client.ReceiveMessage(recvCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reflected packet was accepted, ReceiveMessage err = %v", err)
	}
}

func TestSecurePipeRejectsReplay(t *testing.T) {
	ctx := context.Background()
	clientInner := newTestPipe()
	serverInner := newTestPipe()
	client := mustSecurePipe(t, clientInner, "secret", RoleClient)
	server := mustSecurePipe(t, serverInner, "secret", RoleServer)

	if err := client.SendMessage(ctx, []byte("once"), ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	blob := clientInner.sentMessages()[0]
	serverInner.push(blob)
	serverInner.push(blob)

	got, err := server.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("first receive: %v", err)
	}
	if !bytes.Equal(got, []byte("once")) {
		t.Fatalf("first receive got %q", got)
	}

	recvCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := server.ReceiveMessage(recvCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("replayed packet was accepted, ReceiveMessage err = %v", err)
	}
}

func TestSecurePipeRejectsWrongPassword(t *testing.T) {
	ctx := context.Background()
	clientInner := newTestPipe()
	serverInner := newTestPipe()
	client := mustSecurePipe(t, clientInner, "secret", RoleClient)
	server := mustSecurePipe(t, serverInner, "different", RoleServer)

	if err := client.SendMessage(ctx, []byte("data"), ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	serverInner.push(clientInner.sentMessages()[0])

	recvCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := server.ReceiveMessage(recvCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("packet with wrong password was accepted, err = %v", err)
	}
}

func TestSecurePipeRejectsSaltRollback(t *testing.T) {
	ctx := context.Background()
	client1Inner := newTestPipe()
	client2Inner := newTestPipe()
	serverInner := newTestPipe()
	client1 := mustSecurePipe(t, client1Inner, "secret", RoleClient)
	client2 := mustSecurePipe(t, client2Inner, "secret", RoleClient)
	server := mustSecurePipe(t, serverInner, "secret", RoleServer)

	if err := client1.SendMessage(ctx, []byte("first session"), ""); err != nil {
		t.Fatalf("client1 send: %v", err)
	}
	firstBlob := client1Inner.sentMessages()[0]
	serverInner.push(firstBlob)
	if got, err := server.ReceiveMessage(ctx); err != nil || !bytes.Equal(got, []byte("first session")) {
		t.Fatalf("first receive = %q, %v", got, err)
	}

	if err := client2.SendMessage(ctx, []byte("second session"), ""); err != nil {
		t.Fatalf("client2 send: %v", err)
	}
	serverInner.push(client2Inner.sentMessages()[0])
	if got, err := server.ReceiveMessage(ctx); err != nil || !bytes.Equal(got, []byte("second session")) {
		t.Fatalf("second receive = %q, %v", got, err)
	}

	serverInner.push(firstBlob)
	recvCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := server.ReceiveMessage(recvCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retired-salt frame was accepted, err = %v", err)
	}
}

func TestSecurePipeLegacyRoleAutoDetection(t *testing.T) {
	ctx := context.Background()
	aInner := newTestPipe()
	bInner := newTestPipe()
	a, err := NewSecurePipe(aInner, "secret", noopLogger{})
	if err != nil {
		t.Fatalf("NewSecurePipe(a): %v", err)
	}
	b, err := NewSecurePipe(bInner, "secret", noopLogger{})
	if err != nil {
		t.Fatalf("NewSecurePipe(b): %v", err)
	}

	if err := a.SendMessage(ctx, []byte("legacy"), ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	bInner.push(aInner.sentMessages()[0])
	got, err := b.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("legacy receive: %v", err)
	}
	if !bytes.Equal(got, []byte("legacy")) {
		t.Fatalf("legacy receive got %q", got)
	}
}
