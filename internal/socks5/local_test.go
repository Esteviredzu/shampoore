package socks5

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestLocalHandshakeDeadline(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	s := &LocalServer{
		logger:           testLogger{},
		handshakeTimeout: 40 * time.Millisecond,
	}

	done := make(chan struct{})
	go func() {
		s.handle(context.Background(), server)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handle did not return after the handshake deadline")
	}
}

func TestNormalizeListen(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"   ", ""},
		{"127.0.0.1:1080", "127.0.0.1:1080"},
		{" 127.0.0.1:1080 ", "127.0.0.1:1080"},
		{":1080", "127.0.0.1:1080"},
		{"0.0.0.0:1080", "127.0.0.1:1080"},
		{"[::]:1080", "127.0.0.1:1080"},
		{"*:1080", "127.0.0.1:1080"},
		{"1080", "127.0.0.1:1080"},
		{"[::1]:9050", "[::1]:9050"},
		{"localhost:1080", "localhost:1080"},
		{"not-an-address", "not-an-address"},
	}
	for _, tc := range cases {
		if got := NormalizeListen(tc.in); got != tc.want {
			t.Fatalf("NormalizeListen(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	type accepted struct {
		conn net.Conn
		err  error
	}
	ch := make(chan accepted, 1)
	go func() {
		conn, err := ln.Accept()
		ch <- accepted{conn: conn, err: err}
	}()

	client, err = net.Dial("tcp4", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	res := <-ch
	if res.err != nil {
		client.Close()
		t.Fatalf("accept: %v", res.err)
	}
	return client, res.conn
}

func TestPipeHalfClose(t *testing.T) {
	appConn, proxyConn := tcpPair(t)
	defer appConn.Close()
	defer proxyConn.Close()

	remoteConn, targetConn := tcpPair(t)
	defer remoteConn.Close()
	defer targetConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &LocalServer{
		logger:        testLogger{},
		halfCloseIdle: 500 * time.Millisecond,
	}

	pipeDone := make(chan struct{})
	go func() {
		s.pipe(ctx, proxyConn, remoteConn)
		close(pipeDone)
	}()

	targetErr := make(chan error, 1)
	go func() {
		data, err := io.ReadAll(targetConn)
		if err != nil {
			targetErr <- err
			return
		}
		if string(data) != "ping" {
			targetErr <- fmt.Errorf("target got %q, want %q", data, "ping")
			return
		}
		if _, err := targetConn.Write([]byte("pong")); err != nil {
			targetErr <- err
			return
		}
		targetErr <- targetConn.(*net.TCPConn).CloseWrite()
	}()

	if err := appConn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := appConn.Write([]byte("ping")); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	if err := appConn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("client close write: %v", err)
	}

	select {
	case err := <-targetErr:
		if err != nil {
			t.Fatalf("target: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("target did not observe the half-close")
	}

	select {
	case <-pipeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("pipe did not finish after both directions closed")
	}

	_ = proxyConn.Close()
	_ = remoteConn.Close()

	resp, err := io.ReadAll(appConn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if string(resp) != "pong" {
		t.Fatalf("client got %q, want %q", resp, "pong")
	}
}

func TestPipeHalfCloseIdleTimeout(t *testing.T) {
	appConn, proxyConn := tcpPair(t)
	defer appConn.Close()
	defer proxyConn.Close()

	remoteConn, targetConn := tcpPair(t)
	defer remoteConn.Close()
	defer targetConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const idle = 80 * time.Millisecond
	s := &LocalServer{
		logger:        testLogger{},
		halfCloseIdle: idle,
	}

	pipeDone := make(chan struct{})
	go func() {
		s.pipe(ctx, proxyConn, remoteConn)
		close(pipeDone)
	}()

	readDone := make(chan struct{})
	go func() {
		_, _ = io.ReadAll(targetConn)
		close(readDone)
	}()

	if _, err := appConn.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := appConn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}

	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("target did not receive the half-close")
	}

	start := time.Now()
	select {
	case <-pipeDone:
	case <-time.After(time.Second):
		t.Fatal("pipe did not stop after the half-close idle timeout")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("pipe stopped too late: %s", elapsed)
	}
}
