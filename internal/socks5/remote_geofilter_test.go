package socks5

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/geofilter"
)

func newBlockingGeoFilter(t *testing.T, cidrs string) *geofilter.Filter {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "blocked.txt"), []byte(cidrs+"\n"), 0o600); err != nil {
		t.Fatalf("write rule: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "rules.state.json"), []byte("{\n  \"blocked\": true\n}\n"), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}
	filter := geofilter.NewFilter(geofilter.ActionBlock)
	if err := filter.LoadDir(dir); err != nil {
		t.Fatalf("load rules: %v", err)
	}
	rules := filter.Rules()
	if len(rules) != 1 || !rules[0].Enabled {
		t.Fatalf("geo rule was not loaded as enabled: %+v", rules)
	}
	if action, ok := filter.MatchAction(net.ParseIP("127.0.0.1")); ok && action != geofilter.ActionBlock {
		t.Fatalf("rule action = %q, want %q", action, geofilter.ActionBlock)
	}
	return filter
}

func listenProbe(t *testing.T) (net.Listener, <-chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen probe: %v", err)
	}
	accepted := make(chan net.Conn, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})
	return ln, accepted
}

func probeTarget(t *testing.T, ln net.Listener) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split probe addr: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse probe port: %v", err)
	}
	return host, port
}

func assertNoAccept(t *testing.T, accepted <-chan net.Conn) {
	t.Helper()
	select {
	case conn := <-accepted:
		_ = conn.Close()
		t.Fatal("blocked destination reached the network")
	case <-time.After(200 * time.Millisecond):
	}
}

func runRemoteStream(t *testing.T, handler *RemoteHandler, meta map[string]any) *netPipeStream {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	stream := &netPipeStream{conn: server, id: "geo-test-stream", meta: meta}
	done := make(chan struct{})
	go func() {
		handler.handleStream(stream)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleStream did not return")
	}
	if !stream.Closed() {
		t.Fatal("stream was not closed")
	}
	return stream
}

func TestRemoteHandlerBlocksTCPDestinationByGeoIP(t *testing.T) {
	ln, accepted := listenProbe(t)
	host, port := probeTarget(t, ln)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)
	handler.SetGeoFilter(newBlockingGeoFilter(t, "127.0.0.0/8"), false)

	runRemoteStream(t, handler, map[string]any{"host": host, "port": float64(port)})
	assertNoAccept(t, accepted)
}

func TestRemoteHandlerBlocksUDPDestinationByGeoIP(t *testing.T) {
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer pc.Close()
	host, portStr, err := net.SplitHostPort(pc.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)
	handler.SetGeoFilter(newBlockingGeoFilter(t, "127.0.0.0/8"), false)

	runRemoteStream(t, handler, map[string]any{
		"host": host,
		"port": float64(port),
		"udp":  true,
	})

	if err := pc.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	if _, _, err := pc.ReadFrom(buf); err == nil {
		t.Fatal("blocked UDP destination received a datagram")
	}
}

func TestRemoteHandlerBlocksWhenAnyResolvedIPMatches(t *testing.T) {
	ln, accepted := listenProbe(t)
	_, port := probeTarget(t, ln)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)
	handler.SetGeoFilter(newBlockingGeoFilter(t, "2001:db8::/32"), false)
	handler.lookupIP = func(context.Context, string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("2001:db8::1")}, nil
	}

	runRemoteStream(t, handler, map[string]any{
		"host": "dual-stack.example",
		"port": float64(port),
	})
	assertNoAccept(t, accepted)
}

func TestRemoteHandlerFailsClosedWhenGeoResolutionFails(t *testing.T) {
	ln, accepted := listenProbe(t)
	_, port := probeTarget(t, ln)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)
	handler.SetGeoFilter(newBlockingGeoFilter(t, "203.0.113.0/24"), false)
	handler.lookupIP = func(context.Context, string) ([]net.IP, error) {
		return nil, errors.New("dns unavailable")
	}

	runRemoteStream(t, handler, map[string]any{
		"host": "unresolvable.example",
		"port": float64(port),
	})
	assertNoAccept(t, accepted)
}

func TestRemoteHandlerAllowsUnmatchedDestination(t *testing.T) {
	echoAddr, stopEcho := startEchoServer(t)
	defer stopEcho()
	host, port := echoTarget(t, echoAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := NewRemoteHandler(ctx, nil, testLogger{}, 4096, false)
	handler.SetGeoFilter(newBlockingGeoFilter(t, "203.0.113.0/24"), false)

	client, server := net.Pipe()
	stream := &netPipeStream{
		conn: server,
		id:   "geo-allowed-stream",
		meta: map[string]any{"host": host, "port": float64(port)},
	}
	done := make(chan struct{})
	go func() {
		handler.handleStream(stream)
		close(done)
	}()

	payload := []byte("allowed-through-geo-filter")
	if _, err := client.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf) != string(payload) {
		t.Fatalf("echo = %q, want %q", buf, payload)
	}
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleStream did not return")
	}
}
