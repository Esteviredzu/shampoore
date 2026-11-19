package socks5

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Esteviredzu/shampoore/internal/geofilter"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/netdial"
)

const (
	socksVersion = 0x05

	methodNoAuth   = 0x00
	methodNoAccept = 0xFF

	cmdConnect = 0x01

	atypIPv4   = 0x01
	atypDomain = 0x03
	atypIPv6   = 0x04

	repSuccess              = 0x00
	repGeneralFailure       = 0x01
	repConnectionNotAllowed = 0x02
	repHostUnreachable      = 0x04
	repCommandNotSupported  = 0x07
	repAddressNotSupported  = 0x08

	maxConcurrentConns   = 1024
	defaultHandshakeTime = 10 * time.Second
	defaultRequestTime   = 10 * time.Second
	defaultHalfCloseIdle = 5 * time.Second
)

type LocalServer struct {
	listen   string
	mux      multiplexing.StreamMultiplexer
	logger   logging.ContextLogger
	ipv4Only bool

	cfgMu        sync.RWMutex
	filter       *geofilter.Filter
	blockUnknown bool

	lnMu sync.Mutex
	ln   net.Listener

	sem chan struct{}

	stopOnce sync.Once
	stopCh   chan struct{}

	handshakeTimeout time.Duration
	requestTimeout   time.Duration
	halfCloseIdle    time.Duration
}

func NewLocalServer(listen string, m multiplexing.StreamMultiplexer, log logging.ContextLogger, ipv4Only bool) *LocalServer {
	return &LocalServer{
		listen:   listen,
		mux:      m,
		logger:   log,
		ipv4Only: ipv4Only,
		sem:      make(chan struct{}, maxConcurrentConns),
		stopCh:   make(chan struct{}),
	}
}

func (s *LocalServer) SetGeoFilter(filter *geofilter.Filter, blockUnknown bool) {
	s.cfgMu.Lock()
	s.filter = filter
	s.blockUnknown = blockUnknown
	s.cfgMu.Unlock()
}

func (s *LocalServer) routeFor(ip net.IP) (bypass bool, block bool) {
	s.cfgMu.RLock()
	filter := s.filter
	blockUnknown := s.blockUnknown
	s.cfgMu.RUnlock()
	if filter == nil {
		return false, false
	}
	if action, ok := filter.MatchAction(ip); ok {
		if action == geofilter.ActionBypass {
			return true, false
		}
		return false, true
	}
	if blockUnknown {
		return false, true
	}
	return false, false
}

func (s *LocalServer) handshakeDeadline() time.Duration {
	if s.handshakeTimeout > 0 {
		return s.handshakeTimeout
	}
	return defaultHandshakeTime
}

func (s *LocalServer) requestDeadline() time.Duration {
	if s.requestTimeout > 0 {
		return s.requestTimeout
	}
	return defaultRequestTime
}

func (s *LocalServer) idleTimeout() time.Duration {
	if s.halfCloseIdle > 0 {
		return s.halfCloseIdle
	}
	return defaultHalfCloseIdle
}

func (s *LocalServer) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.listen)
	if err != nil {
		return err
	}
	s.lnMu.Lock()
	s.ln = ln
	s.lnMu.Unlock()
	s.logger.Info("socks5: listening on ", s.listen)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	if s.sem == nil {
		s.sem = make(chan struct{}, maxConcurrentConns)
	}
	if s.stopCh == nil {
		s.stopCh = make(chan struct{})
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case s.sem <- struct{}{}:
		case <-ctx.Done():
			_ = conn.Close()
			return nil
		case <-s.stopCh:
			_ = conn.Close()
			return nil
		}
		go func(c net.Conn) {
			defer func() { <-s.sem }()
			s.handle(ctx, c)
		}(conn)
	}
}

func (s *LocalServer) Stop() {
	s.stopOnce.Do(func() {
		if s.stopCh != nil {
			close(s.stopCh)
		}
	})
	s.lnMu.Lock()
	ln := s.ln
	s.lnMu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
}

func (s *LocalServer) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	if d := s.handshakeDeadline(); d > 0 {
		_ = conn.SetDeadline(time.Now().Add(d))
	}
	if err := s.negotiate(conn); err != nil {
		s.logger.Warn("socks5: negotiate: ", err)
		return
	}

	if d := s.requestDeadline(); d > 0 {
		_ = conn.SetDeadline(time.Now().Add(d))
	}
	host, port, err := s.readRequest(conn)
	_ = conn.SetDeadline(time.Time{})
	if err != nil {
		s.logger.Warn("socks5: request: ", err)
		return
	}

	ip, host, ok := s.destinationIP(conn, host)
	if !ok {
		return
	}

	bypass, block := s.routeFor(ip)
	if block {
		s.logger.Warn("socks5: blocked destination by GeoIP filter: ", host)
		s.sendReply(conn, repConnectionNotAllowed)
		return
	}
	if bypass {
		s.logger.Info("socks5: bypassing tunnel for ", host)
		s.handleDirect(ctx, conn, host, port)
		return
	}
	if s.mux == nil {
		s.handleDirect(ctx, conn, host, port)
		return
	}

	stream, err := s.mux.OpenStream(ctx, map[string]any{
		"host": host,
		"port": port,
	})
	if err != nil {
		s.logger.Warn("socks5: open stream: ", err)
		s.sendReply(conn, repGeneralFailure)
		return
	}
	defer func() {
		if err := stream.Close(); err != nil {
			s.logger.Debug("socks5: close stream: ", err)
		}
	}()

	if err := s.sendReply(conn, repSuccess); err != nil {
		return
	}
	s.pipe(ctx, conn, stream)
}

func (s *LocalServer) destinationIP(conn net.Conn, host string) (net.IP, string, bool) {
	ip := net.ParseIP(host)
	if !s.ipv4Only {
		if ip == nil {
			if resolved, err := resolveIPv4(host); err == nil {
				ip = net.ParseIP(resolved)
			}
		}
		return ip, host, true
	}
	switch {
	case ip == nil:
		resolved, err := resolveIPv4(host)
		if err != nil {
			s.logger.Warn("socks5: resolve ", host, ": ", err)
			s.sendReply(conn, repHostUnreachable)
			return nil, host, false
		}
		ip = net.ParseIP(resolved)
		host = resolved
	case ip.To4() == nil:
		s.logger.Warn("socks5: ipv6 destination rejected: ", host)
		s.sendReply(conn, repAddressNotSupported)
		return nil, host, false
	}
	return ip, host, true
}

func (s *LocalServer) handleDirect(ctx context.Context, conn net.Conn, host string, port int) {
	network := "tcp"
	if s.ipv4Only {
		network = "tcp4"
	}
	target := net.JoinHostPort(host, strconv.Itoa(port))

	direct, err := netdial.DialContext(ctx, network, target)
	if err != nil {
		s.logger.Warn("socks5: direct dial ", target, ": ", err)
		s.sendReply(conn, repGeneralFailure)
		return
	}
	defer func() {
		if err := direct.Close(); err != nil {
			s.logger.Debug("socks5: close direct conn: ", err)
		}
	}()

	if err := s.sendReply(conn, repSuccess); err != nil {
		return
	}
	s.pipe(ctx, conn, direct)
}

func (s *LocalServer) pipe(ctx context.Context, conn net.Conn, remote net.Conn) {
	idle := s.idleTimeout()

	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
			_ = remote.Close()
		case <-stopWatch:
		}
	}()
	defer close(stopWatch)

	var halfClosed atomic.Bool

	var idleMu sync.Mutex
	var idleTimer *time.Timer
	idleArmed := false
	touch := func() {
		idleMu.Lock()
		if idleArmed && idleTimer != nil {
			idleTimer.Reset(idle)
		}
		idleMu.Unlock()
	}
	armIdle := func() {
		idleMu.Lock()
		idleArmed = true
		idleTimer = time.AfterFunc(idle, func() {
			_ = conn.Close()
			_ = remote.Close()
		})
		idleMu.Unlock()
	}
	defer func() {
		idleMu.Lock()
		if idleTimer != nil {
			idleTimer.Stop()
		}
		idleArmed = false
		idleMu.Unlock()
	}()

	copyHalf := func(dst, src net.Conn) error {
		buf, put := getCopyBuf(copyBufSize)
		defer put()
		for {
			if halfClosed.Load() {
				_ = src.SetReadDeadline(time.Now().Add(idle))
			}
			n, err := src.Read(buf)
			if n > 0 {
				if werr := writeAll(dst, buf[:n]); werr != nil {
					return werr
				}
				touch()
			}
			if err != nil {
				return err
			}
		}
	}

	type direction int
	const (
		dirClient direction = iota
		dirRemote
	)
	done := make(chan direction, 2)

	go func() {
		err := copyHalf(remote, conn)
		if err != nil && ctx.Err() == nil {
			s.logger.Debug("socks5: copy client->remote: ", err)
		}
		closeWriteConn(remote)
		done <- dirClient
	}()
	go func() {
		err := copyHalf(conn, remote)
		if err != nil && ctx.Err() == nil {
			s.logger.Debug("socks5: copy remote->client: ", err)
		}
		closeWriteConn(conn)
		done <- dirRemote
	}()

	<-done
	halfClosed.Store(true)
	armIdle()

	select {
	case <-done:
	case <-ctx.Done():
		_ = conn.Close()
		_ = remote.Close()
		<-done
	}
}

func writeAll(dst net.Conn, p []byte) error {
	for len(p) > 0 {
		n, err := dst.Write(p)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

type closeWriter interface {
	CloseWrite() error
}

func closeWriteConn(c net.Conn) {
	if c == nil {
		return
	}
	if cw, ok := c.(closeWriter); ok {
		_ = cw.CloseWrite()
	}
}

func (s *LocalServer) negotiate(conn net.Conn) error {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}
	if header[0] != socksVersion {
		return errors.New("bad version")
	}
	nmethods := int(header[1])
	methods := make([]byte, nmethods)
	if _, err := io.ReadFull(conn, methods); err != nil {
		return err
	}
	for _, m := range methods {
		if m == methodNoAuth {
			_, err := conn.Write([]byte{socksVersion, methodNoAuth})
			return err
		}
	}
	conn.Write([]byte{socksVersion, methodNoAccept})
	return errors.New("no auth method")
}

func (s *LocalServer) readRequest(conn net.Conn) (string, int, error) {
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return "", 0, err
	}
	if head[0] != socksVersion {
		return "", 0, errors.New("bad version")
	}
	if head[1] != cmdConnect {
		s.sendReply(conn, repCommandNotSupported)
		return "", 0, errors.New("only CONNECT supported")
	}

	var host string
	switch head[3] {
	case atypIPv4:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return "", 0, err
		}
		host = net.IP(buf).String()
	case atypDomain:
		lb := make([]byte, 1)
		if _, err := io.ReadFull(conn, lb); err != nil {
			return "", 0, err
		}
		domain := make([]byte, int(lb[0]))
		if _, err := io.ReadFull(conn, domain); err != nil {
			return "", 0, err
		}
		host = string(domain)
	case atypIPv6:
		buf := make([]byte, 16)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return "", 0, err
		}
		host = net.IP(buf).String()
	default:
		s.sendReply(conn, repAddressNotSupported)
		return "", 0, errors.New("bad atyp")
	}

	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBuf); err != nil {
		return "", 0, err
	}
	port := int(binary.BigEndian.Uint16(portBuf))

	return host, port, nil
}

func (s *LocalServer) sendReply(conn net.Conn, code byte) error {
	_, err := conn.Write([]byte{
		socksVersion, code, 0x00, atypIPv4,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00,
	})
	return err
}

func resolveIPv4(host string) (string, error) {
	addrs, err := net.LookupIP(host)
	if err != nil {
		return "", err
	}
	for _, a := range addrs {
		if v4 := a.To4(); v4 != nil {
			return v4.String(), nil
		}
	}
	return "", fmt.Errorf("no IPv4 for %s", host)
}
