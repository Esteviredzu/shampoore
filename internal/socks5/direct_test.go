package socks5

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestDirectLocalServerWithoutMux(t *testing.T) {
	echoLn, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoLn.Close()
	go func() {
		for {
			conn, err := echoLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewLocalServer("127.0.0.1:0", nil, testLogger{}, false)
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe(ctx) }()
	defer srv.Stop()

	var addr string
	deadline := time.Now().Add(2 * time.Second)
	for {
		srv.lnMu.Lock()
		ln := srv.ln
		srv.lnMu.Unlock()
		if ln != nil {
			addr = ln.Addr().String()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socks listener did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := conn.Write([]byte{socksVersion, 1, methodNoAuth}); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil || reply[1] != methodNoAuth {
		t.Fatalf("negotiation reply = %v, %v", reply, err)
	}

	host, portStr, _ := net.SplitHostPort(echoLn.Addr().String())
	port, _ := strconv.Atoi(portStr)
	req := []byte{socksVersion, cmdConnect, 0x00, atypIPv4}
	req = append(req, net.ParseIP(host).To4()...)
	req = append(req, byte(port>>8), byte(port))
	if _, err := conn.Write(req); err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		t.Fatal(err)
	}
	if head[0] != socksVersion || head[1] != repSuccess {
		t.Fatalf("connect reply = %v", head)
	}
	// Consume BND.ADDR/BND.PORT.
	bound := make([]byte, 6)
	if _, err := io.ReadFull(conn, bound); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "ping" {
		t.Fatalf("echo = %q", got)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server stopped: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}
}
