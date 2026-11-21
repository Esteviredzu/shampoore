package socks5

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func runExitProbe(t *testing.T, handler *RemoteHandler, meta map[string]any) (*netPipeStream, <-chan struct{}) {
	t.Helper()

	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })

	stream := &netPipeStream{
		conn: server,
		id:   "probe-test-stream",
		meta: meta,
	}
	done := make(chan struct{})
	go func() {
		handler.handleStream(stream)
		close(done)
	}()

	return &netPipeStream{conn: client, id: stream.id, meta: meta}, done
}

func TestRemoteHandlerAnswersExitProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)
	handler.SetGeoFilter(newBlockingGeoFilter(t, "0.0.0.0/0"), false)

	client, done := runExitProbe(t, handler, map[string]any{"probe": "exit"})
	defer client.Close()

	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, len(remoteProbeReply))
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatalf("read probe reply: %v", err)
	}
	if string(buf) != remoteProbeReply {
		t.Fatalf("probe reply = %q, want %q", string(buf), remoteProbeReply)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe handler did not close the stream")
	}
}

func TestRemoteHandlerRejectsUnknownProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)

	client, done := runExitProbe(t, handler, map[string]any{"probe": "bogus"})
	defer client.Close()

	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := client.Read(buf); err == nil {
		t.Fatal("unknown probe must not receive a reply")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("unknown probe handler did not close the stream")
	}
}

func TestRemoteHandlerExitProbeBypassesGeoFilter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)
	handler.SetGeoFilter(newBlockingGeoFilter(t, "0.0.0.0/0"), true)

	client, done := runExitProbe(t, handler, map[string]any{"probe": "exit"})
	defer client.Close()

	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, len(remoteProbeReply))
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatalf("probe was blocked by geofilter: %v", err)
	}
	if string(buf) != remoteProbeReply {
		t.Fatalf("probe reply = %q, want %q", string(buf), remoteProbeReply)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe handler did not close the stream")
	}
}
