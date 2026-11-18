package netdial

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
)

func TestDialContextCallsProtector(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Close()
	}()

	var calls atomic.Int32
	SetProtector(func(fd int) bool {
		if fd < 0 {
			t.Errorf("invalid fd: %d", fd)
		}
		calls.Add(1)
		return true
	})
	defer SetProtector(nil)

	conn, err := DialContext(context.Background(), "tcp4", ln.Addr().String())
	if err != nil {
		t.Fatalf("direct dial: %v", err)
	}
	conn.Close()

	if calls.Load() == 0 {
		t.Fatal("socket protector was not called")
	}
}

func TestDialContextAbortsWhenProtectFails(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	SetProtector(func(int) bool { return false })
	defer SetProtector(nil)

	if conn, err := DialContext(context.Background(), "tcp4", ln.Addr().String()); err == nil {
		conn.Close()
		t.Fatal("dial must fail when the socket cannot be protected")
	}
}
