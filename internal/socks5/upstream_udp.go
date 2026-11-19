package socks5

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

func dialSocks5UDP(ctx context.Context, upstream *socks5Upstream, target string) (net.Conn, error) {
	if upstream == nil {
		return nil, errors.New("socks5 upstream is not configured")
	}
	control, err := dialUpstream(ctx, upstream)
	if err != nil {
		return nil, err
	}
	if err := socks5Handshake(control, upstream, socks5CmdUDP, "0.0.0.0:0"); err != nil {
		_ = control.Close()
		return nil, err
	}
	relayAddr, err := readSocks5Reply(control)
	if err != nil {
		_ = control.Close()
		return nil, err
	}
	upstreamHost, _, splitErr := net.SplitHostPort(upstream.address)
	if splitErr != nil {
		upstreamHost = upstream.address
	}
	relayHost, relayPort, err := net.SplitHostPort(relayAddr)
	if err != nil {
		_ = control.Close()
		return nil, fmt.Errorf("socks5 udp: invalid relay address %q: %w", relayAddr, err)
	}
	if relayHost == "" || relayHost == "0.0.0.0" || relayHost == "::" {
		relayHost = upstreamHost
	}
	relay, err := net.ResolveUDPAddr("udp", net.JoinHostPort(relayHost, relayPort))
	if err != nil {
		_ = control.Close()
		return nil, fmt.Errorf("socks5 udp: resolve relay: %w", err)
	}
	udp, err := net.DialUDP("udp", nil, relay)
	if err != nil {
		_ = control.Close()
		return nil, fmt.Errorf("socks5 udp: dial relay: %w", err)
	}
	header, err := encodeSocks5Addr(target)
	if err != nil {
		_ = control.Close()
		_ = udp.Close()
		return nil, err
	}
	_ = control.SetDeadline(time.Time{})
	_ = udp.SetDeadline(time.Time{})
	conn := &socks5UDPConn{
		control:      control,
		udp:          udp,
		relay:        relay,
		targetHeader: header,
	}
	go conn.watchControl()
	return conn, nil
}

type socks5UDPConn struct {
	control      net.Conn
	udp          *net.UDPConn
	relay        *net.UDPAddr
	targetHeader []byte

	closeOnce sync.Once
}

func (c *socks5UDPConn) Write(p []byte) (int, error) {
	if len(p)+len(c.targetHeader)+3 > 65507 {
		return 0, errors.New("socks5 udp: datagram is too large")
	}
	out := make([]byte, 3, 3+len(c.targetHeader)+len(p))
	out = append(out, c.targetHeader...)
	out = append(out, p...)
	if _, err := c.udp.Write(out); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *socks5UDPConn) Read(p []byte) (int, error) {
	buf := make([]byte, 65535)
	for {
		n, err := c.udp.Read(buf)
		if err != nil {
			return 0, err
		}
		payload, ok := decodeSocks5UDPPacket(buf[:n])
		if !ok || len(payload) == 0 {
			continue
		}
		return copy(p, payload), nil
	}
}

func (c *socks5UDPConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		udpErr := c.udp.Close()
		ctrlErr := c.control.Close()
		if udpErr != nil {
			err = udpErr
		} else {
			err = ctrlErr
		}
	})
	return err
}

func (c *socks5UDPConn) LocalAddr() net.Addr  { return c.udp.LocalAddr() }
func (c *socks5UDPConn) RemoteAddr() net.Addr { return c.relay }

func (c *socks5UDPConn) SetDeadline(t time.Time) error {
	_ = c.udp.SetDeadline(t)
	return c.control.SetDeadline(t)
}

func (c *socks5UDPConn) SetReadDeadline(t time.Time) error {
	return c.udp.SetReadDeadline(t)
}

func (c *socks5UDPConn) SetWriteDeadline(t time.Time) error {
	return c.udp.SetWriteDeadline(t)
}

func (c *socks5UDPConn) watchControl() {
	_, _ = io.Copy(io.Discard, c.control)
	_ = c.Close()
}

func decodeSocks5UDPPacket(pkt []byte) ([]byte, bool) {
	if len(pkt) < 4 {
		return nil, false
	}
	if pkt[0] != 0x00 || pkt[1] != 0x00 || pkt[2] != 0x00 {
		return nil, false
	}
	r := bytes.NewReader(pkt[4:])
	if _, err := readSocks5Addr(r, pkt[3]); err != nil {
		return nil, false
	}
	var portBuf [2]byte
	if _, err := io.ReadFull(r, portBuf[:]); err != nil {
		return nil, false
	}
	payload := make([]byte, r.Len())
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, false
	}
	return payload, true
}
