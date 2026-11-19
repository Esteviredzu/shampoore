package socks5

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	socks5AuthNone     byte = 0x00
	socks5AuthUserPass byte = 0x02
	socks5CmdConnect   byte = 0x01
	socks5CmdUDP       byte = 0x03
	socks5RepSuccess   byte = 0x00

	upstreamDialTimeout      = 15 * time.Second
	upstreamHandshakeTimeout = 10 * time.Second
)

type socks5Upstream struct {
	address  string
	username string
	password string
}

func parseSocks5Upstream(raw string) (*socks5Upstream, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "local") {
		return nil, nil
	}
	if !strings.Contains(raw, "://") {
		raw = "socks5://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("socks5 upstream %q: %w", raw, err)
	}
	if u.Scheme != "socks5" && u.Scheme != "socks5h" {
		return nil, fmt.Errorf("socks5 upstream %q: unsupported scheme %q", raw, u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("socks5 upstream %q: host is required", raw)
	}
	if _, _, err := net.SplitHostPort(u.Host); err != nil {
		return nil, fmt.Errorf("socks5 upstream %q: host must include a port: %w", raw, err)
	}
	up := &socks5Upstream{address: u.Host}
	if u.User != nil {
		up.username = u.User.Username()
		up.password, _ = u.User.Password()
	}
	return up, nil
}

func dialSocks5TCP(ctx context.Context, upstream *socks5Upstream, target string) (net.Conn, error) {
	if upstream == nil {
		return nil, errors.New("socks5 upstream is not configured")
	}
	conn, err := dialUpstream(ctx, upstream)
	if err != nil {
		return nil, err
	}
	if err := socks5Handshake(conn, upstream, socks5CmdConnect, target); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, err := readSocks5Reply(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func dialUpstream(ctx context.Context, upstream *socks5Upstream) (net.Conn, error) {
	dialer := net.Dialer{Timeout: upstreamDialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", upstream.address)
	if err != nil {
		return nil, fmt.Errorf("socks5 upstream %s: %w", upstream.address, err)
	}
	deadline := time.Now().Add(upstreamHandshakeTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
	return conn, nil
}

func socks5Handshake(conn net.Conn, upstream *socks5Upstream, cmd byte, target string) error {
	methods := []byte{socks5AuthNone}
	if upstream.username != "" || upstream.password != "" {
		methods = []byte{socks5AuthNone, socks5AuthUserPass}
	}
	req := append([]byte{socksVersion, byte(len(methods))}, methods...)
	if err := writeAll(conn, req); err != nil {
		return fmt.Errorf("socks5 handshake: %w", err)
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("socks5 handshake: %w", err)
	}
	if resp[0] != socksVersion || resp[1] == methodNoAccept {
		return errors.New("socks5 handshake: no acceptable authentication method")
	}
	if resp[1] == socks5AuthUserPass {
		if err := socks5UserPassAuth(conn, upstream); err != nil {
			return err
		}
	}

	addr, err := encodeSocks5Addr(target)
	if err != nil {
		return err
	}
	req = append([]byte{socksVersion, cmd, 0x00}, addr...)
	if err := writeAll(conn, req); err != nil {
		return fmt.Errorf("socks5 request: %w", err)
	}
	return nil
}

func socks5UserPassAuth(conn net.Conn, upstream *socks5Upstream) error {
	user := []byte(upstream.username)
	pass := []byte(upstream.password)
	if len(user) > 255 || len(pass) > 255 {
		return errors.New("socks5 upstream: credentials are too long")
	}
	req := make([]byte, 0, 3+len(user)+len(pass))
	req = append(req, 0x01, byte(len(user)))
	req = append(req, user...)
	req = append(req, byte(len(pass)))
	req = append(req, pass...)
	if err := writeAll(conn, req); err != nil {
		return fmt.Errorf("socks5 auth: %w", err)
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("socks5 auth: %w", err)
	}
	if resp[1] != 0x00 {
		return errors.New("socks5 upstream: authentication failed")
	}
	return nil
}

func readSocks5Reply(conn net.Conn) (string, error) {
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return "", fmt.Errorf("socks5 reply: %w", err)
	}
	if head[0] != socksVersion {
		return "", errors.New("socks5 reply: bad version")
	}
	if head[1] != socks5RepSuccess {
		return "", fmt.Errorf("socks5 reply: upstream error code 0x%02x", head[1])
	}
	host, err := readSocks5Addr(conn, head[3])
	if err != nil {
		return "", err
	}
	port := make([]byte, 2)
	if _, err := io.ReadFull(conn, port); err != nil {
		return "", fmt.Errorf("socks5 reply: %w", err)
	}
	return net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port)))), nil
}

func encodeSocks5Addr(target string) ([]byte, error) {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return nil, fmt.Errorf("socks5: invalid target %q: %w", target, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("socks5: invalid target port %q", portStr)
	}
	var out []byte
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			out = append([]byte{atypIPv4}, v4...)
		} else {
			out = append([]byte{atypIPv6}, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return nil, errors.New("socks5: target host is too long")
		}
		out = append([]byte{atypDomain, byte(len(host))}, host...)
	}
	out = append(out, byte(port>>8), byte(port))
	return out, nil
}

func readSocks5Addr(r io.Reader, atyp byte) (string, error) {
	switch atyp {
	case atypIPv4:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return net.IP(buf).String(), nil
	case atypIPv6:
		buf := make([]byte, 16)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return net.IP(buf).String(), nil
	case atypDomain:
		lb := make([]byte, 1)
		if _, err := io.ReadFull(r, lb); err != nil {
			return "", err
		}
		buf := make([]byte, int(lb[0]))
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return string(buf), nil
	default:
		return "", fmt.Errorf("socks5: unsupported address type %d", atyp)
	}
}
