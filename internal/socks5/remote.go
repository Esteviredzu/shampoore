package socks5

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/geofilter"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
)

const (
	remoteResolveTimeout = 10 * time.Second

	remoteProbeExit       = "exit"
	remoteProbeNet        = "net"
	remoteProbeReply      = "ok\n"
	remoteProbeErrPrefix  = "err:"
	remoteProbeNetTimeout = 3 * time.Second
	remoteProbeMinSpacing = 100 * time.Millisecond

	remoteProbeNetMinSpacing = time.Second
)

var remoteProbeNetTarget = "1.1.1.1:443"

func dialTCP(ctx context.Context, target string, ipv4Only bool, upstream string) (net.Conn, error) {
	if upstream != "" {
		up, err := parseSocks5Upstream(upstream)
		if err != nil {
			return nil, err
		}
		if up != nil {
			return dialSocks5TCP(ctx, up, target)
		}
	}
	network := "tcp"
	if ipv4Only {
		network = "tcp4"
	}
	dialer := net.Dialer{Timeout: 30 * time.Second}
	return dialer.DialContext(ctx, network, target)
}

func dialUDP(ctx context.Context, target, upstream string) (net.Conn, error) {
	if upstream != "" {
		up, err := parseSocks5Upstream(upstream)
		if err != nil {
			return nil, err
		}
		if up != nil {
			return dialSocks5UDP(ctx, up, target)
		}
	}
	dialer := net.Dialer{Timeout: 30 * time.Second}
	return dialer.DialContext(ctx, "udp", target)
}

type RemoteHandler struct {
	mux       multiplexing.StreamMultiplexer
	logger    logging.ContextLogger
	chunkSize int
	ipv4Only  bool

	cfgMu         sync.RWMutex
	filter        *geofilter.Filter
	blockUnknown  bool
	exitUpstreams map[string]string
	lookupIP      func(context.Context, string) ([]net.IP, error)

	mu      sync.Mutex
	streams map[multiplexing.Stream]struct{}

	probeMu       sync.Mutex
	lastExitProbe time.Time
	lastNetProbe  time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (s *RemoteHandler) SetGeoFilter(filter *geofilter.Filter, blockUnknown bool) {
	s.cfgMu.Lock()
	s.filter = filter
	s.blockUnknown = blockUnknown
	s.cfgMu.Unlock()
}

func (s *RemoteHandler) SetExitUpstreams(upstreams map[string]string) {
	out := make(map[string]string, len(upstreams))
	for id, raw := range upstreams {
		id = strings.TrimSpace(id)
		raw = strings.TrimSpace(raw)
		if id == "" || raw == "" || strings.EqualFold(raw, "local") {
			continue
		}
		out[id] = raw
	}
	s.cfgMu.Lock()
	s.exitUpstreams = out
	s.cfgMu.Unlock()
}

func (s *RemoteHandler) ExitUpstreams() map[string]string {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if len(s.exitUpstreams) == 0 {
		return nil
	}
	out := make(map[string]string, len(s.exitUpstreams))
	for id, raw := range s.exitUpstreams {
		out[id] = raw
	}
	return out
}

func (s *RemoteHandler) upstreamFor(exitNodeID string) string {
	exitNodeID = strings.TrimSpace(exitNodeID)
	if exitNodeID == "" || strings.EqualFold(exitNodeID, "local") {
		return ""
	}
	s.cfgMu.RLock()
	upstream := s.exitUpstreams[exitNodeID]
	s.cfgMu.RUnlock()
	return strings.TrimSpace(upstream)
}

func (s *RemoteHandler) sessionIdentity() (userID, exitNodeID string) {
	if s.mux == nil {
		return "", ""
	}
	if identity, ok := s.mux.(interface {
		SessionIdentity() (string, string)
	}); ok {
		return identity.SessionIdentity()
	}
	return "", ""
}

func (s *RemoteHandler) shouldBlock(ip net.IP) bool {
	s.cfgMu.RLock()
	filter := s.filter
	blockUnknown := s.blockUnknown
	s.cfgMu.RUnlock()
	if filter == nil {
		return false
	}
	if action, ok := filter.MatchAction(ip); ok {
		return action != geofilter.ActionBypass
	}
	return blockUnknown
}

func lookupHostIPs(ctx context.Context, host string) ([]net.IP, error) {
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, addr := range addrs {
		if addr.IP == nil {
			continue
		}
		ips = append(ips, addr.IP)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no IP addresses for %s", host)
	}
	return ips, nil
}

func (s *RemoteHandler) resolveDestinationIPs(ctx context.Context, host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	lookup := s.lookupIP
	if lookup == nil {
		lookup = lookupHostIPs
	}
	rctx, cancel := context.WithTimeout(ctx, remoteResolveTimeout)
	defer cancel()
	return lookup(rctx, host)
}

func (s *RemoteHandler) geoActive() bool {
	s.cfgMu.RLock()
	filter := s.filter
	blockUnknown := s.blockUnknown
	s.cfgMu.RUnlock()
	if filter == nil {
		return false
	}
	if blockUnknown {
		return true
	}
	for _, rule := range filter.Rules() {
		if rule.Enabled && len(rule.CIDRs) > 0 {
			return true
		}
	}
	return false
}

func (s *RemoteHandler) blockedDestination(ctx context.Context, host string) (net.IP, bool) {
	if !s.geoActive() {
		return nil, false
	}
	ips, err := s.resolveDestinationIPs(ctx, host)
	if err != nil {
		s.logger.Warn("remote: resolve destination ", host, " for GeoIP check: ", err)
		return nil, true
	}
	for _, ip := range ips {
		if s.shouldBlock(ip) {
			return ip, true
		}
	}
	return nil, false
}

func (s *RemoteHandler) blockedConnIP(conn net.Conn) net.IP {
	if conn == nil {
		return nil
	}
	addr := conn.RemoteAddr()
	if addr == nil {
		return nil
	}
	var ip net.IP
	switch a := addr.(type) {
	case *net.TCPAddr:
		ip = a.IP
	case *net.UDPAddr:
		ip = a.IP
	default:
		if host, _, err := net.SplitHostPort(addr.String()); err == nil {
			ip = net.ParseIP(host)
		}
	}
	if ip == nil || !s.shouldBlock(ip) {
		return nil
	}
	return ip
}

func NewRemoteHandler(
	ctx context.Context,
	m multiplexing.StreamMultiplexer,
	logger logging.ContextLogger,
	chunkSize int,
	ipv4Only bool,
) *RemoteHandler {
	if chunkSize <= 0 {
		chunkSize = 32768
	}
	sctx, cancel := context.WithCancel(ctx)
	return &RemoteHandler{
		mux:       m,
		logger:    logger,
		chunkSize: chunkSize,
		ipv4Only:  ipv4Only,
		lookupIP:  lookupHostIPs,
		streams:   make(map[multiplexing.Stream]struct{}),
		ctx:       sctx,
		cancel:    cancel,
	}
}

func (s *RemoteHandler) Run() error {
	s.logger.Info("remote: waiting for incoming streams...")

	for {
		stream, err := s.mux.AcceptStream(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			return err
		}

		s.mu.Lock()
		s.streams[stream] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go func(st multiplexing.Stream) {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				delete(s.streams, st)
				s.mu.Unlock()
			}()
			s.handleStream(st)
		}(stream)
	}
}

func (s *RemoteHandler) Stop() {
	s.cancel()
	s.mu.Lock()
	streams := make([]multiplexing.Stream, 0, len(s.streams))
	for st := range s.streams {
		streams = append(streams, st)
	}
	s.mu.Unlock()

	for _, st := range streams {
		s.closeStream(st)
	}
	s.wg.Wait()
}

func (s *RemoteHandler) closeStream(stream multiplexing.Stream) {
	if err := stream.Close(); err != nil {
		s.logger.Debug("remote: close stream ", stream.ID(), ": ", err)
	}
}

func (s *RemoteHandler) handleProbe(stream multiplexing.Stream, probe string) {
	defer s.closeStream(stream)

	if probe != remoteProbeExit && probe != remoteProbeNet {
		s.logger.Warn("remote: ", stream.ID(), " unknown probe ", probe)
		return
	}

	s.probeMu.Lock()
	now := time.Now()
	if probe == remoteProbeNet {
		if !s.lastNetProbe.IsZero() && now.Sub(s.lastNetProbe) < remoteProbeNetMinSpacing {
			s.probeMu.Unlock()
			s.logger.Debug("remote: ", stream.ID(), " net probe rate limited")
			return
		}
		s.lastNetProbe = now
		s.probeMu.Unlock()
	} else {
		if !s.lastExitProbe.IsZero() && now.Sub(s.lastExitProbe) < remoteProbeMinSpacing {
			s.probeMu.Unlock()
			s.logger.Debug("remote: ", stream.ID(), " probe rate limited")
			return
		}
		s.lastExitProbe = now
		s.probeMu.Unlock()
	}

	if probe == remoteProbeNet {
		reply := s.probeInternet()
		if _, err := stream.Write([]byte(reply)); err != nil {
			s.logger.Warn("remote: ", stream.ID(), " net probe write: ", err)
		}
		return
	}

	if _, err := stream.Write([]byte(remoteProbeReply)); err != nil {
		s.logger.Warn("remote: ", stream.ID(), " probe write: ", err)
	}
}

func (s *RemoteHandler) probeInternet() string {
	target := strings.TrimSpace(remoteProbeNetTarget)
	if target == "" {
		return remoteProbeReply
	}
	ctx, cancel := context.WithTimeout(s.ctx, remoteProbeNetTimeout)
	defer cancel()
	conn, err := dialTCP(ctx, target, s.ipv4Only, "")
	if err != nil {
		return remoteProbeErrPrefix + err.Error() + "\n"
	}
	_ = conn.Close()
	return remoteProbeReply
}

func (s *RemoteHandler) handleStream(stream multiplexing.Stream) {
	streamID := stream.ID()
	meta := stream.Metadata()

	if probe, _ := meta["probe"].(string); probe != "" {
		s.handleProbe(stream, probe)
		return
	}

	host, _ := meta["host"].(string)

	var port int
	switch v := meta["port"].(type) {
	case float64:
		port = int(v)
	case int:
		port = v
	case int64:
		port = int(v)
	}

	if host == "" || port == 0 {
		s.logger.Warn("remote: ", streamID, " invalid metadata")
		s.closeStream(stream)
		return
	}

	if blockedIP, blocked := s.blockedDestination(s.ctx, host); blocked {
		if blockedIP != nil {
			s.logger.Warn("remote: ", streamID, " blocked destination by GeoIP filter: ",
				host, " (", blockedIP, ")")
		} else {
			s.logger.Warn("remote: ", streamID, " blocked destination: GeoIP check failed for ", host)
		}
		s.closeStream(stream)
		return
	}

	if s.ipv4Only {
		if ip := net.ParseIP(host); ip != nil && ip.To4() == nil {
			s.logger.Warn("remote: ", streamID, " ipv6 destination rejected: ", host)
			s.closeStream(stream)
			return
		}
	}

	if isUDP, _ := meta["udp"].(bool); isUDP {
		_, exitNodeID := s.sessionIdentity()
		upstream := s.upstreamFor(exitNodeID)
		if upstream != "" {
			s.logger.Info("remote udp: ", streamID, " exit=", exitNodeID, " via ", upstream)
		}
		s.handleUDPStream(stream, host, port, upstream)
		return
	}

	target := net.JoinHostPort(host, strconv.Itoa(port))
	_, exitNodeID := s.sessionIdentity()
	upstream := s.upstreamFor(exitNodeID)
	if upstream != "" {
		s.logger.Info("remote: ", streamID, " -> ", target, " exit=", exitNodeID, " via ", upstream)
	} else {
		s.logger.Info("remote: ", streamID, " -> ", target)
	}

	conn, err := dialTCP(s.ctx, target, s.ipv4Only, upstream)
	if err != nil {
		s.logger.Warn("remote: ", streamID, " connect failed: ", err)
		s.closeStream(stream)
		return
	}
	if upstream == "" {
		if blockedIP := s.blockedConnIP(conn); blockedIP != nil {
			s.logger.Warn("remote: ", streamID, " blocked connected IP by GeoIP filter: ",
				blockedIP, " (", target, ")")
			_ = conn.Close()
			s.closeStream(stream)
			return
		}
	}

	s.logger.Debug("remote: ", streamID, " connected")

	var closeOnce sync.Once
	cleanup := func() {
		closeOnce.Do(func() {
			if err := conn.Close(); err != nil {
				s.logger.Debug("remote: ", streamID, " close conn: ", err)
			}
			if err := stream.Close(); err != nil {
				s.logger.Debug("remote: ", streamID, " close stream: ", err)
			}
		})
	}
	defer func() {
		cleanup()
		s.logger.Debug("remote: ", streamID, " closed")
	}()

	stopWatch := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-s.ctx.Done():
			cleanup()
		case <-stopWatch:
		}
	}()
	defer func() {
		close(stopWatch)
		<-watchDone
	}()

	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		buf, put := getCopyBuf(s.chunkSize)
		defer put()
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if _, werr := stream.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		buf, put := getCopyBuf(s.chunkSize)
		defer put()
		for {
			n, err := stream.Read(buf)
			if n > 0 {
				if _, werr := conn.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	<-done
	cleanup()
	<-done
}

func (s *RemoteHandler) handleUDPStream(stream multiplexing.Stream, host string, port int, upstream string) {
	streamID := stream.ID()

	target := net.JoinHostPort(host, strconv.Itoa(port))

	conn, err := dialUDP(s.ctx, target, upstream)
	if err != nil {
		s.logger.Warn("remote udp: ", streamID, " dial ", target, ": ", err)
		s.closeStream(stream)
		return
	}
	if upstream == "" {
		if blockedIP := s.blockedConnIP(conn); blockedIP != nil {
			s.logger.Warn("remote udp: ", streamID, " blocked connected IP by GeoIP filter: ",
				blockedIP, " (", target, ")")
			_ = conn.Close()
			s.closeStream(stream)
			return
		}
	}

	s.logger.Debug("remote udp: ", streamID, " -> ", target)

	var closeOnce sync.Once
	cleanup := func() {
		closeOnce.Do(func() {
			if err := conn.Close(); err != nil {
				s.logger.Debug("remote udp: ", streamID, " close conn: ", err)
			}
			if err := stream.Close(); err != nil {
				s.logger.Debug("remote udp: ", streamID, " close stream: ", err)
			}
		})
	}
	defer func() {
		cleanup()
		s.logger.Debug("remote udp: ", streamID, " closed")
	}()

	stopWatch := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-s.ctx.Done():
			cleanup()
		case <-stopWatch:
		}
	}()
	defer func() {
		close(stopWatch)
		<-watchDone
	}()

	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		hdr := make([]byte, 2)
		buf := make([]byte, 65535)
		for {
			if _, err := io.ReadFull(stream, hdr); err != nil {
				return
			}
			n := int(binary.BigEndian.Uint16(hdr))
			if n == 0 {
				continue
			}
			if n > len(buf) {
				return
			}
			if _, err := io.ReadFull(stream, buf[:n]); err != nil {
				return
			}
			if _, err := conn.Write(buf[:n]); err != nil {
				return
			}
		}
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 65535)
		out := make([]byte, 2+65535)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			if n == 0 {
				continue
			}
			binary.BigEndian.PutUint16(out[:2], uint16(n))
			copy(out[2:], buf[:n])
			if _, err := stream.Write(out[:2+n]); err != nil {
				return
			}
		}
	}()

	<-done
	cleanup()
	<-done
}
