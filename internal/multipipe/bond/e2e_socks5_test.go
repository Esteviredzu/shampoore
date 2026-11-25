package bond

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/socks5"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

var e2ePattern = bytes.Repeat([]byte("shampoore-bond-e2e"), 4096)

func startBodySource(t *testing.T) (host string, port int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 64)
				n, _ := c.Read(buf)
				fields := strings.Fields(strings.TrimSpace(string(buf[:n])))
				size := 1024
				if len(fields) >= 2 {
					if parsed, parseErr := strconv.Atoi(fields[1]); parseErr == nil && parsed > 0 {
						size = parsed
					}
				}
				for written := 0; written < size; {
					chunk := e2ePattern
					if remaining := size - written; remaining < len(chunk) {
						chunk = chunk[:remaining]
					}
					if _, writeErr := c.Write(chunk); writeErr != nil {
						return
					}
					written += len(chunk)
				}
			}(conn)
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return addr.IP.String(), addr.Port
}

func dialViaSOCKS5(t *testing.T, proxyAddr, host string, port int) net.Conn {
	t.Helper()
	var conn net.Conn
	var err error
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err = net.DialTimeout("tcp", proxyAddr, time.Second)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial socks5 %s: %v", proxyAddr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	if _, err = conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("socks5 greeting: %v", err)
	}
	reply := make([]byte, 2)
	if _, err = io.ReadFull(conn, reply); err != nil {
		t.Fatalf("socks5 greeting reply: %v", err)
	}
	if reply[0] != 0x05 || reply[1] != 0x00 {
		t.Fatalf("socks5 unexpected greeting reply %v", reply)
	}

	ip := net.ParseIP(host).To4()
	if ip == nil {
		t.Fatalf("host %q is not an IPv4 address", host)
	}
	req := []byte{0x05, 0x01, 0x00, 0x01}
	req = append(req, ip...)
	req = append(req, byte(port>>8), byte(port&0xff))
	if _, err = conn.Write(req); err != nil {
		t.Fatalf("socks5 connect: %v", err)
	}
	header := make([]byte, 4)
	if _, err = io.ReadFull(conn, header); err != nil {
		t.Fatalf("socks5 connect reply: %v", err)
	}
	if header[1] != 0x00 {
		t.Fatalf("socks5 connect failed with code %d", header[1])
	}
	switch header[3] {
	case 0x01:
		_, err = io.ReadFull(conn, make([]byte, 4+2))
	case 0x03:
		length := make([]byte, 1)
		if _, err = io.ReadFull(conn, length); err == nil {
			_, err = io.ReadFull(conn, make([]byte, int(length[0])+2))
		}
	case 0x04:
		_, err = io.ReadFull(conn, make([]byte, 16+2))
	default:
		t.Fatalf("socks5 unexpected address type %d", header[3])
	}
	if err != nil {
		t.Fatalf("socks5 bound address: %v", err)
	}
	return conn
}

func fetchThroughSOCKS(t *testing.T, proxyAddr, host string, port, size int) []byte {
	t.Helper()
	conn := dialViaSOCKS5(t, proxyAddr, host, port)
	if _, err := fmt.Fprintf(conn, "GET %d\n", size); err != nil {
		t.Fatalf("write request: %v", err)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	for i := range body {
		if body[i] != e2ePattern[i%len(e2ePattern)] {
			t.Fatalf("body mismatch at byte %d", i)
		}
	}
	return body
}

func startBondPath(t *testing.T, ctx context.Context, names []string, dead map[string]bool) string {
	t.Helper()

	clientTubes := make([]transport.Transport, 0, len(names))
	serverTubes := make([]transport.Transport, 0, len(names))
	for _, name := range names {
		clientEnd, serverEnd := newMemPair()
		if dead[name] {
			clientTubes = append(clientTubes, deafTransport{clientEnd})
			serverTubes = append(serverTubes, deafTransport{serverEnd})
			continue
		}
		clientTubes = append(clientTubes, clientEnd)
		serverTubes = append(serverTubes, serverEnd)
	}

	srv := NewServer(names, nil, testLogger{}, tunnel.Config{
		FlushThreshold: 8192,
		FlushInterval:  5 * time.Millisecond,
	}, nil)
	srv.SetSessionHandler(func(hctx context.Context, mux multiplexing.StreamMultiplexer) error {
		return socks5.NewRemoteHandler(hctx, mux, testLogger{}, 32*1024, true).Run()
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(stopCtx)
	})

	for i, name := range names {
		release, _ := srv.register(name, "alice", serverTubes[i])
		if release == nil {
			t.Fatalf("tube %s was not accepted by the aggregator", name)
		}
		t.Cleanup(release)
	}

	mux, err := NewMux(clientTubes, names, testLogger{})
	if err != nil {
		t.Fatalf("client mux: %v", err)
	}
	mux.SetIdentity("alice", "")
	if err := mux.Start(ctx); err != nil {
		t.Fatalf("client bond start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mux.Stop(stopCtx)
	})

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve socks address: %v", err)
	}
	socksAddr := probe.Addr().String()
	_ = probe.Close()

	local := socks5.NewLocalServer(socksAddr, mux, testLogger{}, true)
	go func() { _ = local.ListenAndServe(ctx) }()
	t.Cleanup(local.Stop)
	return socksAddr
}

func TestBondSocks5SingleTubeE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	host, port := startBodySource(t)
	socksAddr := startBondPath(t, ctx, []string{"0-raw"}, nil)

	started := time.Now()
	body := fetchThroughSOCKS(t, socksAddr, host, port, 256<<10)
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("first payload took %s: the bond handshake is stuck", elapsed)
	}
	if len(body) != 256<<10 {
		t.Fatalf("received %d bytes, want %d", len(body), 256<<10)
	}
}

func TestBondSocks5ThreeLiveOneDeadE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	host, port := startBodySource(t)
	names := []string{"0-raw", "1-jellyfin", "2-livekit", "3-dead"}
	socksAddr := startBondPath(t, ctx, names, map[string]bool{"3-dead": true})

	started := time.Now()
	body := fetchThroughSOCKS(t, socksAddr, host, port, 512<<10)
	if elapsed := time.Since(started); elapsed > 20*time.Second {
		t.Fatalf("payload with a dead tube took %s", elapsed)
	}
	if len(body) != 512<<10 {
		t.Fatalf("received %d bytes, want %d", len(body), 512<<10)
	}

	body = fetchThroughSOCKS(t, socksAddr, host, port, 128<<10)
	if len(body) != 128<<10 {
		t.Fatalf("second transfer received %d bytes", len(body))
	}
}
