package security

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

type memTransport struct {
	sendCh chan []byte
	recvCh chan []byte
}

func (m *memTransport) Start(context.Context) error { return nil }
func (m *memTransport) Stop(context.Context) error  { return nil }

func (m *memTransport) Send(ctx context.Context, data []byte) error {
	cp := append([]byte(nil), data...)
	select {
	case m.sendCh <- cp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *memTransport) Receive(ctx context.Context) ([]byte, error) {
	select {
	case data := <-m.recvCh:
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *memTransport) MaxMessageSize() int   { return 1 << 20 }
func (m *memTransport) BytesDelivered() int64 { return 0 }
func (m *memTransport) BytesAcked() int64     { return 0 }

func TestEnsureSecureTransportIsIdempotent(t *testing.T) {
	a2b := make(chan []byte, 8)
	b2a := make(chan []byte, 8)
	client := &memTransport{sendCh: a2b, recvCh: b2a}
	server := &memTransport{sendCh: b2a, recvCh: a2b}

	plain, err := EnsureSecureTransport(client, "", RoleClient, nil)
	if err != nil {
		t.Fatalf("no password: %v", err)
	}
	if plain != transport.Transport(client) {
		t.Fatal("transport without a password was wrapped")
	}

	wrapped, err := EnsureSecureTransport(client, "test-secret", RoleClient, nil)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	if !IsAuthenticated(wrapped) {
		t.Fatal("wrapped transport does not report Authenticated")
	}
	again, err := EnsureSecureTransport(wrapped, "test-secret", RoleClient, nil)
	if err != nil {
		t.Fatalf("second wrap: %v", err)
	}
	if again != wrapped {
		t.Fatal("an already encrypted transport was wrapped twice")
	}

	serverWrapped, err := EnsureSecureTransport(server, "test-secret", RoleServer, nil)
	if err != nil {
		t.Fatalf("server wrap: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := []byte("plain-payload")
	if err := wrapped.Send(ctx, payload); err != nil {
		t.Fatalf("client send: %v", err)
	}
	cipher := <-a2b
	if !bytes.HasPrefix(cipher, []byte("SPV2")) {
		t.Fatalf("wire message does not start with SPV2: %x", cipher[:min(len(cipher), 4)])
	}
	if bytes.Contains(cipher, payload) {
		t.Fatal("plaintext leaked onto the wire")
	}
	a2b <- cipher
	got, err := serverWrapped.Receive(ctx)
	if err != nil {
		t.Fatalf("server receive: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("server got %q, want %q", got, payload)
	}

	reply := []byte("reply-payload")
	if err := serverWrapped.Send(ctx, reply); err != nil {
		t.Fatalf("server send: %v", err)
	}
	cipher = <-b2a
	b2a <- cipher
	got, err = wrapped.Receive(ctx)
	if err != nil {
		t.Fatalf("client receive: %v", err)
	}
	if !bytes.Equal(got, reply) {
		t.Fatalf("client got %q, want %q", got, reply)
	}
}
