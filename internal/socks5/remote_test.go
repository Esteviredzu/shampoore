package socks5

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
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

type blockingStream struct {
	id        string
	meta      map[string]any
	closed    chan struct{}
	closeOnce sync.Once
}

func newBlockingStream(meta map[string]any) *blockingStream {
	return &blockingStream{
		id:     "test-stream",
		meta:   meta,
		closed: make(chan struct{}),
	}
}

func (s *blockingStream) Read([]byte) (int, error) {
	<-s.closed
	return 0, io.EOF
}

func (s *blockingStream) Write(p []byte) (int, error) { return len(p), nil }

func (s *blockingStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (s *blockingStream) ID() string               { return s.id }
func (s *blockingStream) Metadata() map[string]any { return s.meta }

func (s *blockingStream) Closed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

func (s *blockingStream) LocalAddr() net.Addr              { return testAddr("local") }
func (s *blockingStream) RemoteAddr() net.Addr             { return testAddr("remote") }
func (s *blockingStream) SetDeadline(time.Time) error      { return nil }
func (s *blockingStream) SetReadDeadline(time.Time) error  { return nil }
func (s *blockingStream) SetWriteDeadline(time.Time) error { return nil }

func TestHandleStreamUnblocksWhenStreamCloses(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	_, portStr, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := NewRemoteHandler(ctx, nil, testLogger{}, 128, false)

	stream := newBlockingStream(map[string]any{
		"host": "127.0.0.1",
		"port": float64(port),
	})

	done := make(chan struct{})
	go func() {
		handler.handleStream(stream)
		close(done)
	}()

	var serverConn net.Conn
	select {
	case serverConn = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("target connection was not accepted")
	}
	defer serverConn.Close()

	time.Sleep(50 * time.Millisecond)

	if err := stream.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleStream did not return after the client stream closed")
	}

	if err := serverConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := serverConn.Read(buf); err == nil {
		t.Fatal("expected the target connection to be closed")
	}
}
