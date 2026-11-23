package socks5

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestRemoteHandlerExitThenNetProbeBothAnswer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	prev := remoteProbeNetTarget
	remoteProbeNetTarget = ln.Addr().String()
	t.Cleanup(func() { remoteProbeNetTarget = prev })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)

	readReply := func(probe string) string {
		t.Helper()
		client, done := runExitProbe(t, handler, map[string]any{"probe": probe})
		defer client.Close()
		if err := client.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		buf := make([]byte, len(remoteProbeReply))
		if _, err := io.ReadFull(client, buf); err != nil {
			t.Fatalf("%s probe reply: %v", probe, err)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s probe handler did not close the stream", probe)
		}
		return string(buf)
	}

	if got := readReply(remoteProbeExit); got != remoteProbeReply {
		t.Fatalf("exit probe reply = %q", got)
	}
	if got := readReply(remoteProbeNet); got != remoteProbeReply {
		t.Fatalf("net probe reply = %q", got)
	}
}

func TestRemoteHandlerNetProbeDialsTarget(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		_ = conn.Close()
	}()

	prev := remoteProbeNetTarget
	remoteProbeNetTarget = ln.Addr().String()
	t.Cleanup(func() { remoteProbeNetTarget = prev })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)

	client, done := runExitProbe(t, handler, map[string]any{"probe": remoteProbeNet})
	defer client.Close()

	if err := client.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, len(remoteProbeReply))
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatalf("read net probe reply: %v", err)
	}
	if string(buf) != remoteProbeReply {
		t.Fatalf("net probe reply = %q, want %q", string(buf), remoteProbeReply)
	}

	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("exit node did not dial the probe target")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("net probe handler did not close the stream")
	}
}

func TestRemoteHandlerNetProbeReportsDialFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	prev := remoteProbeNetTarget
	remoteProbeNetTarget = addr
	t.Cleanup(func() { remoteProbeNetTarget = prev })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)

	client, _ := runExitProbe(t, handler, map[string]any{"probe": remoteProbeNet})
	defer client.Close()

	if err := client.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	reply, readErr := io.ReadAll(io.LimitReader(client, 256))
	if readErr != nil && len(reply) == 0 {
		t.Fatalf("read net probe reply: %v", readErr)
	}
	if !strings.HasPrefix(string(reply), remoteProbeErrPrefix) {
		t.Fatalf("net probe reply = %q, want an err: prefix", string(reply))
	}
}
