package silo

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multipipe/bond"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

func roundTripSilo(t *testing.T, password string) {
	t.Helper()

	store := newFakeStore("shared-bucket")
	cfg := testSiloConfig("shared-bucket", "alice")
	cfg.MinFileSizeKB = 8
	cfg.MaxFileSizeKB = 64

	clientPipe, err := newPipeWithStore(&cfg, "client", testLogger{}, store)
	if err != nil {
		t.Fatalf("client pipe: %v", err)
	}
	serverPipe, err := newPipeWithStore(&cfg, "server", testLogger{}, store)
	if err != nil {
		t.Fatalf("server pipe: %v", err)
	}

	var client transport.Pipe = clientPipe
	var server transport.Pipe = serverPipe
	if password != "" {
		client, err = wrapEncryption(clientPipe, password, "client", testLogger{})
		if err != nil {
			t.Fatalf("client encryption: %v", err)
		}
		server, err = wrapEncryption(serverPipe, password, "server", testLogger{})
		if err != nil {
			t.Fatalf("server encryption: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("client start: %v", err)
	}
	defer func() { _ = client.Stop(context.Background()) }()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop(context.Background()) }()

	payloads := [][]byte{
		[]byte("a"),
		bytes.Repeat([]byte("b"), 7),
		bytes.Repeat([]byte("c"), 8),
		bytes.Repeat([]byte("d"), 100),
		bytes.Repeat([]byte("e"), 4096),
		bytes.Repeat([]byte("f"), 60000),
	}

	sendAndReceive := func(from, to transport.Pipe) {
		for i, payload := range payloads {
			if err := from.SendMessage(ctx, payload, ""); err != nil {
				t.Fatalf("send %d: %v", i, err)
			}
		}
		for i, want := range payloads {
			got, err := to.ReceiveMessage(ctx)
			if err != nil {
				t.Fatalf("receive %d: %v", i, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("message %d: got %d bytes, want %d", i, len(got), len(want))
			}
		}
	}

	sendAndReceive(client, server)
	sendAndReceive(server, client)
}

func TestSiloClientServerRoundTrip(t *testing.T) {
	roundTripSilo(t, "")
}

func TestSiloClientServerRoundTripEncrypted(t *testing.T) {
	roundTripSilo(t, "silo-test-secret")
}

func TestBondOverSiloPipes(t *testing.T) {
	store := newFakeStore("shared-bucket")
	cfg := testSiloConfig("shared-bucket", "alice")
	cfg.MinFileSizeKB = 8
	cfg.MaxFileSizeKB = 64

	clientPipe, err := newPipeWithStore(&cfg, "client", testLogger{}, store)
	if err != nil {
		t.Fatalf("client pipe: %v", err)
	}
	serverPipe, err := newPipeWithStore(&cfg, "server", testLogger{}, store)
	if err != nil {
		t.Fatalf("server pipe: %v", err)
	}

	store.putObject("default/users/alice/c2s/msg_0000000000000000", make([]byte, 16), time.Now().Add(-time.Hour))

	names := []string{"0-silo"}
	clientMux, err := bond.NewMux([]transport.Transport{newTransport(clientPipe)}, names, testLogger{})
	if err != nil {
		t.Fatalf("client bond mux: %v", err)
	}
	serverMux, err := bond.NewMux([]transport.Transport{newTransport(serverPipe)}, names, testLogger{})
	if err != nil {
		t.Fatalf("server bond mux: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := serverMux.Start(ctx); err != nil {
		t.Fatalf("server bond start: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = serverMux.Stop(stopCtx)
	}()
	if err := clientMux.Start(ctx); err != nil {
		t.Fatalf("client bond start: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = clientMux.Stop(stopCtx)
	}()

	acceptCh := make(chan multiplexing.Stream, 1)
	acceptErr := make(chan error, 1)
	go func() {
		stream, err := serverMux.AcceptStream(ctx)
		if err != nil {
			acceptErr <- err
			return
		}
		acceptCh <- stream
	}()

	clientStream, err := clientMux.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer clientStream.Close()

	var serverStream multiplexing.Stream
	select {
	case serverStream = <-acceptCh:
	case err := <-acceptErr:
		t.Fatalf("accept stream: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("accept stream timed out")
	}
	defer serverStream.Close()

	payload := bytes.Repeat([]byte("shampoore-bond-"), 8192)
	writeErr := make(chan error, 1)
	go func() {
		_, err := clientStream.Write(payload)
		writeErr <- err
	}()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(serverStream, got); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	reply := []byte("pong-over-silo")
	if _, err := serverStream.Write(reply); err != nil {
		t.Fatalf("write reply: %v", err)
	}
	replyBuf := make([]byte, len(reply))
	if _, err := io.ReadFull(clientStream, replyBuf); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if !bytes.Equal(replyBuf, reply) {
		t.Fatalf("reply = %q, want %q", replyBuf, reply)
	}
	if store.hasObject("default/users/alice/c2s/msg_0000000000000000") {
		t.Fatal("stale corrupt object was not cleaned up")
	}
}
