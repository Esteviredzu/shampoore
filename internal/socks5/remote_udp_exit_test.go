package socks5

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

func startUDPEchoServer(t *testing.T) (string, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp echo: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 65535)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = pc.WriteTo(buf[:n], addr)
		}
	}()
	return pc.LocalAddr().String(), func() {
		_ = pc.Close()
		<-done
	}
}

type testSocksUDPProxy struct {
	tcp net.Listener

	mu         sync.Mutex
	associates int
	datagrams  int
}

func startTestSocksUDPProxy(t *testing.T) *testSocksUDPProxy {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp proxy: %v", err)
	}
	p := &testSocksUDPProxy{tcp: ln}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

func (p *testSocksUDPProxy) addr() string { return p.tcp.Addr().String() }

func (p *testSocksUDPProxy) stats() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.associates, p.datagrams
}

func (p *testSocksUDPProxy) serve() {
	for {
		conn, err := p.tcp.Accept()
		if err != nil {
			return
		}
		go p.handle(conn)
	}
}

func (p *testSocksUDPProxy) handle(conn net.Conn) {
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
	if req[0] != socksVersion || req[1] != socks5CmdUDP {
		_, _ = conn.Write([]byte{socksVersion, 0x07, 0x00, atypIPv4, 0, 0, 0, 0, 0, 0})
		return
	}
	if _, err := readSocks5Addr(conn, req[3]); err != nil {
		return
	}
	var portBuf [2]byte
	if _, err := io.ReadFull(conn, portBuf[:]); err != nil {
		return
	}

	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return
	}
	defer pc.Close()

	local := pc.LocalAddr().(*net.UDPAddr).IP.To4()
	reply := []byte{socksVersion, socks5RepSuccess, 0x00, atypIPv4}
	reply = append(reply, local...)
	reply = append(reply, byte(pc.LocalAddr().(*net.UDPAddr).Port>>8),
		byte(pc.LocalAddr().(*net.UDPAddr).Port))
	if _, err := conn.Write(reply); err != nil {
		return
	}

	p.mu.Lock()
	p.associates++
	p.mu.Unlock()

	buf := make([]byte, 65535)
	for {
		n, clientAddr, err := pc.ReadFromUDP(buf)
		if err != nil {
			return
		}
		payload, target, ok := splitSocksUDPDatagram(buf[:n])
		if !ok {
			continue
		}
		p.mu.Lock()
		p.datagrams++
		p.mu.Unlock()

		upstream, err := net.DialTimeout("udp", target, 2*time.Second)
		if err != nil {
			continue
		}
		_ = upstream.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = upstream.Write(payload)
		resp := make([]byte, 65535)
		rn, err := upstream.Read(resp)
		_ = upstream.Close()
		if err != nil {
			continue
		}
		header, err := encodeSocks5Addr(target)
		if err != nil {
			continue
		}
		out := make([]byte, 0, 3+len(header)+rn)
		out = append(out, 0, 0, 0)
		out = append(out, header...)
		out = append(out, resp[:rn]...)
		_, _ = pc.WriteToUDP(out, clientAddr)
	}
}

func splitSocksUDPDatagram(pkt []byte) ([]byte, string, bool) {
	if len(pkt) < 4 || pkt[0] != 0 || pkt[1] != 0 || pkt[2] != 0 {
		return nil, "", false
	}
	r := bytes.NewReader(pkt[4:])
	host, err := readSocks5Addr(r, pkt[3])
	if err != nil {
		return nil, "", false
	}
	var port [2]byte
	if _, err := io.ReadFull(r, port[:]); err != nil {
		return nil, "", false
	}
	target := net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port[:]))))
	payload := make([]byte, r.Len())
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, "", false
	}
	return payload, target, true
}

func TestRemoteHandlerRoutesUDPThroughExitUpstream(t *testing.T) {
	echoAddr, stopEcho := startUDPEchoServer(t)
	defer stopEcho()
	proxy := startTestSocksUDPProxy(t)
	host, portStr, err := net.SplitHostPort(echoAddr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := NewRemoteHandler(ctx, &identityTestMux{user: "alice", exit: "exit-germany"}, testLogger{}, 4096, true)
	handler.SetExitUpstreams(map[string]string{"exit-germany": "socks5://" + proxy.addr()})

	client, server := net.Pipe()
	stream := &netPipeStream{
		conn: server,
		id:   "udp-exit-stream",
		meta: map[string]any{"host": host, "port": float64(port), "udp": true},
	}
	done := make(chan struct{})
	go func() {
		handler.handleStream(stream)
		close(done)
	}()

	payload := []byte("udp-payload")
	frame := make([]byte, 2+len(payload))
	binary.BigEndian.PutUint16(frame[:2], uint16(len(payload)))
	copy(frame[2:], payload)
	if _, err := client.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}

	hdr := make([]byte, 2)
	readErr := make(chan error, 1)
	buf := make([]byte, len(payload))
	go func() {
		if _, err := io.ReadFull(client, hdr); err != nil {
			readErr <- err
			return
		}
		n := int(binary.BigEndian.Uint16(hdr))
		if n > len(buf) {
			n = len(buf)
		}
		_, err := io.ReadFull(client, buf[:n])
		if err == nil && string(buf[:n]) != string(payload) {
			err = io.ErrUnexpectedEOF
		}
		readErr <- err
	}()
	select {
	case err := <-readErr:
		if err != nil {
			t.Fatalf("read udp echo: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for udp echo")
	}
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handleStream did not return")
	}

	associates, datagrams := proxy.stats()
	if associates != 1 {
		t.Fatalf("udp associates = %d, want 1", associates)
	}
	if datagrams < 1 {
		t.Fatal("upstream did not relay any datagrams")
	}
}
