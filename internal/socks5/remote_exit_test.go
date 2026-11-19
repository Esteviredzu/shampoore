package socks5

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
)

type identityTestMux struct {
	user string
	exit string
}

func (m *identityTestMux) Start(context.Context) error { return nil }
func (m *identityTestMux) Stop(context.Context) error  { return nil }

func (m *identityTestMux) OpenStream(context.Context, map[string]any) (multiplexing.Stream, error) {
	return nil, errors.New("not implemented")
}

func (m *identityTestMux) AcceptStream(context.Context) (multiplexing.Stream, error) {
	return nil, errors.New("not implemented")
}

func (m *identityTestMux) SessionIdentity() (string, string) { return m.user, m.exit }

type netPipeStream struct {
	conn   net.Conn
	id     string
	meta   map[string]any
	closed atomic.Bool
}

func (s *netPipeStream) Read(p []byte) (int, error)  { return s.conn.Read(p) }
func (s *netPipeStream) Write(p []byte) (int, error) { return s.conn.Write(p) }
func (s *netPipeStream) ID() string                  { return s.id }
func (s *netPipeStream) Metadata() map[string]any    { return s.meta }
func (s *netPipeStream) Closed() bool                { return s.closed.Load() }
func (s *netPipeStream) LocalAddr() net.Addr         { return s.conn.LocalAddr() }
func (s *netPipeStream) RemoteAddr() net.Addr        { return s.conn.RemoteAddr() }

func (s *netPipeStream) Close() error {
	s.closed.Store(true)
	return s.conn.Close()
}

func (s *netPipeStream) SetDeadline(t time.Time) error      { return s.conn.SetDeadline(t) }
func (s *netPipeStream) SetReadDeadline(t time.Time) error  { return s.conn.SetReadDeadline(t) }
func (s *netPipeStream) SetWriteDeadline(t time.Time) error { return s.conn.SetWriteDeadline(t) }

func startEchoServer(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo: %v", err)
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
		wg.Wait()
	}
}

type testSocksProxy struct {
	ln net.Listener

	mu        sync.Mutex
	connects  int
	requested []string
}

func startTestSocksProxy(t *testing.T) *testSocksProxy {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	p := &testSocksProxy{ln: ln}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

func (p *testSocksProxy) addr() string { return p.ln.Addr().String() }

func (p *testSocksProxy) stats() (int, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.connects, append([]string(nil), p.requested...)
}

func (p *testSocksProxy) serve() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(conn)
	}
}

func (p *testSocksProxy) handle(conn net.Conn) {
	defer conn.Close()

	head := make([]byte, 2)
	if _, err := io.ReadFull(conn, head); err != nil {
		return
	}
	methods := make([]byte, int(head[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	if _, err := conn.Write([]byte{socksVersion, socks5AuthNone}); err != nil {
		return
	}

	req := make([]byte, 4)
	if _, err := io.ReadFull(conn, req); err != nil {
		return
	}
	if req[0] != socksVersion || req[1] != socks5CmdConnect {
		_, _ = conn.Write([]byte{socksVersion, 0x07, 0x00, atypIPv4, 0, 0, 0, 0, 0, 0})
		return
	}
	host, err := readSocks5Addr(conn, req[3])
	if err != nil {
		return
	}
	var portBuf [2]byte
	if _, err := io.ReadFull(conn, portBuf[:]); err != nil {
		return
	}
	target := net.JoinHostPort(host, strconv.Itoa(int(portBuf[0])<<8|int(portBuf[1])))

	p.mu.Lock()
	p.connects++
	p.requested = append(p.requested, target)
	p.mu.Unlock()

	upstream, err := net.DialTimeout("tcp", target, 3*time.Second)
	if err != nil {
		_, _ = conn.Write([]byte{socksVersion, 0x05, 0x00, atypIPv4, 0, 0, 0, 0, 0, 0})
		return
	}
	defer upstream.Close()
	if _, err := conn.Write([]byte{socksVersion, socks5RepSuccess, 0x00, atypIPv4, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, conn); done <- struct{}{} }()
	go func() { _, _ = io.Copy(conn, upstream); done <- struct{}{} }()
	<-done
}

func echoTarget(t *testing.T, echoAddr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(echoAddr)
	if err != nil {
		t.Fatalf("split echo addr: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse echo port: %v", err)
	}
	return host, port
}

func roundTripExitStream(t *testing.T, exitNodeID string) (*testSocksProxy, string) {
	t.Helper()
	echoAddr, stopEcho := startEchoServer(t)
	proxy := startTestSocksProxy(t)
	host, port := echoTarget(t, echoAddr)

	ctx, cancel := context.WithCancel(context.Background())
	handler := NewRemoteHandler(ctx, &identityTestMux{user: "alice", exit: exitNodeID}, testLogger{}, 4096, true)
	handler.SetExitUpstreams(map[string]string{"exit-germany": "socks5://" + proxy.addr()})

	client, server := net.Pipe()
	stream := &netPipeStream{
		conn: server,
		id:   "exit-stream",
		meta: map[string]any{"host": host, "port": float64(port)},
	}
	done := make(chan struct{})
	go func() {
		handler.handleStream(stream)
		close(done)
	}()

	payload := []byte("payload-through-exit")
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
	cancel()
	stopEcho()
	return proxy, echoAddr
}

func TestRemoteHandlerRoutesThroughExitUpstream(t *testing.T) {
	proxy, echoAddr := roundTripExitStream(t, "exit-germany")
	connects, requested := proxy.stats()
	if connects != 1 {
		t.Fatalf("upstream connections = %d, want 1", connects)
	}
	if len(requested) != 1 || requested[0] != echoAddr {
		t.Fatalf("upstream requested %v, want [%s]", requested, echoAddr)
	}
}

func TestRemoteHandlerDirectExitDoesNotUseUpstream(t *testing.T) {
	proxy, _ := roundTripExitStream(t, "local")
	if connects, _ := proxy.stats(); connects != 0 {
		t.Fatalf("upstream was used for local exit: %d connections", connects)
	}
}

func TestRemoteHandlerUnknownExitDoesNotUseUpstream(t *testing.T) {
	proxy, _ := roundTripExitStream(t, "exit-unknown")
	if connects, _ := proxy.stats(); connects != 0 {
		t.Fatalf("upstream was used for unknown exit: %d connections", connects)
	}
}
