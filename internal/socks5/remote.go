package socks5

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/geofilter"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
)

func dialTCP(target string, ipv4Only bool) (net.Conn, error) {
	network := "tcp"
	if ipv4Only {
		network = "tcp4"
	}
	return net.DialTimeout(network, target, 30*time.Second)
}

type RemoteHandler struct {
	mux          multiplexing.StreamMultiplexer
	logger       logging.ContextLogger
	chunkSize    int
	ipv4Only     bool
	filter       *geofilter.Filter
	blockUnknown bool

	mu      sync.Mutex
	streams map[multiplexing.Stream]struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (s *RemoteHandler) SetGeoFilter(filter *geofilter.Filter, blockUnknown bool) {
	s.filter = filter
	s.blockUnknown = blockUnknown
}

func (s *RemoteHandler) shouldBlock(ip net.IP) bool {
	if s.filter == nil {
		return false
	}
	if s.filter.Match(ip) {
		return true
	}
	if s.blockUnknown {
		return true
	}
	return false
}

func NewRemoteHandler(
	ctx context.Context,
	m multiplexing.StreamMultiplexer,
	logger logging.ContextLogger,
	chunkSize int,
	ipv4Only bool,
) *RemoteHandler {
	if chunkSize <= 0 {
		chunkSize = 8000
	}
	sctx, cancel := context.WithCancel(ctx)
	return &RemoteHandler{
		mux:       m,
		logger:    logger,
		chunkSize: chunkSize,
		ipv4Only:  ipv4Only,
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
	for st := range s.streams {
		st.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *RemoteHandler) handleStream(stream multiplexing.Stream) {
	streamID := stream.ID()
	meta := stream.Metadata()

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
		stream.Close()
		return
	}

	ip := net.ParseIP(host)
	if ip == nil {
		if resolved, err := resolveIPv4(host); err == nil {
			ip = net.ParseIP(resolved)
		}
	}
	if ip != nil && s.shouldBlock(ip) {
		s.logger.Warn("remote: ", streamID, " blocked destination by GeoIP filter: ", host)
		stream.Close()
		return
	}

	if s.ipv4Only {
		if ip != nil && ip.To4() == nil {
			s.logger.Warn("remote: ", streamID, " ipv6 destination rejected: ", host)
			stream.Close()
			return
		}
	}

	if isUDP, _ := meta["udp"].(bool); isUDP {
		s.handleUDPStream(stream, host, port)
		return
	}

	target := net.JoinHostPort(host, strconv.Itoa(port))
	s.logger.Info("remote: ", streamID, " -> ", target)

	conn, err := dialTCP(target, s.ipv4Only)
	if err != nil {
		s.logger.Warn("remote: ", streamID, " connect failed: ", err)
		stream.Close()
		return
	}

	s.logger.Debug("remote: ", streamID, " connected")

	defer func() {
		conn.Close()
		stream.Close()
		s.logger.Debug("remote: ", streamID, " closed")
	}()

	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-s.ctx.Done():
			conn.Close()
		case <-stopWatch:
		}
	}()
	defer close(stopWatch)

	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		buf := make([]byte, s.chunkSize)
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
		buf := make([]byte, s.chunkSize)
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
}

func (s *RemoteHandler) handleUDPStream(stream multiplexing.Stream, host string, port int) {
	streamID := stream.ID()
	defer stream.Close()

	target := net.JoinHostPort(host, strconv.Itoa(port))

	conn, err := net.Dial("udp", target)
	if err != nil {
		s.logger.Warn("remote udp: ", streamID, " dial ", target, ": ", err)
		return
	}
	defer conn.Close()

	s.logger.Debug("remote udp: ", streamID, " -> ", target)

	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		hdr := make([]byte, 2)
		for {
			if _, err := io.ReadFull(stream, hdr); err != nil {
				return
			}
			n := int(binary.BigEndian.Uint16(hdr))
			if n == 0 {
				continue
			}
			buf := make([]byte, n)
			if _, err := io.ReadFull(stream, buf); err != nil {
				return
			}
			if _, err := conn.Write(buf); err != nil {
				return
			}
		}
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 65535)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			if n == 0 {
				continue
			}
			out := make([]byte, 2+n)
			binary.BigEndian.PutUint16(out[:2], uint16(n))
			copy(out[2:], buf[:n])
			if _, err := stream.Write(out); err != nil {
				return
			}
		}
	}()

	<-done
	conn.Close()
	<-done
}
