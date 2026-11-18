package muxproxy

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"

	M "github.com/xjasonlyu/tun2socks/v2/metadata"
	"github.com/xjasonlyu/tun2socks/v2/proxy"

	"github.com/Esteviredzu/shampoore/internal/geofilter"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/netdial"
)

var (
	mu           sync.RWMutex
	defaultMux   multiplexing.StreamMultiplexer
	defaultV4    = true
	filter       *geofilter.Filter
	blockUnknown bool
)

func SetMux(m multiplexing.StreamMultiplexer, ipv4Only bool) {
	mu.Lock()
	defaultMux = m
	defaultV4 = ipv4Only
	mu.Unlock()
}

func SetGeoFilter(f *geofilter.Filter, unknown bool) {
	mu.Lock()
	filter = f
	blockUnknown = unknown
	mu.Unlock()
}

func ClearIf(m multiplexing.StreamMultiplexer) {
	mu.Lock()
	defer mu.Unlock()
	if defaultMux == m {
		defaultMux = nil
		filter = nil
		blockUnknown = false
	}
}

func snapshot() (multiplexing.StreamMultiplexer, bool, *geofilter.Filter, bool) {
	mu.RLock()
	defer mu.RUnlock()
	return defaultMux, defaultV4, filter, blockUnknown
}

func routeAction(f *geofilter.Filter, unknown bool, ip net.IP) (geofilter.Action, bool) {
	if f == nil {
		return "", false
	}
	if action, ok := f.MatchAction(ip); ok {
		return action, true
	}
	if unknown {
		return geofilter.ActionBlock, true
	}
	return "", false
}

type MuxProxy struct{}

func (p *MuxProxy) DialContext(ctx context.Context, meta *M.Metadata) (net.Conn, error) {
	addr := meta.DestinationAddress()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("muxproxy: split host:port: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("muxproxy: parse port: %w", err)
	}

	mux, ipv4Only, f, unknown := snapshot()
	ip := net.ParseIP(host)

	if ipv4Only && ip != nil && ip.To4() == nil {
		return nil, fmt.Errorf("muxproxy: ipv6 rejected: %s", host)
	}

	if action, matched := routeAction(f, unknown, ip); matched {
		if action != geofilter.ActionBypass {
			return nil, fmt.Errorf("muxproxy: destination blocked by rule: %s", host)
		}
		network := "tcp"
		if ipv4Only {
			network = "tcp4"
		}
		return netdial.DialContext(ctx, network, addr)
	}

	if mux == nil {
		return nil, fmt.Errorf("muxproxy: mux not initialized")
	}

	stream, err := mux.OpenStream(ctx, map[string]any{
		"host": host,
		"port": port,
	})
	if err != nil {
		return nil, fmt.Errorf("muxproxy: open stream: %w", err)
	}
	return stream, nil
}

func (p *MuxProxy) DialUDP(meta *M.Metadata) (net.PacketConn, error) {
	addr := meta.DestinationAddress()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("muxproxy: udp split: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("muxproxy: udp parse port: %w", err)
	}

	mux, ipv4Only, f, unknown := snapshot()
	ip := net.ParseIP(host)

	if ipv4Only && ip != nil && ip.To4() == nil {
		return nil, fmt.Errorf("muxproxy: udp ipv6 rejected: %s", host)
	}

	if action, matched := routeAction(f, unknown, ip); matched {
		if action != geofilter.ActionBypass {
			return nil, fmt.Errorf("muxproxy: udp destination blocked by rule: %s", host)
		}
		network := "udp"
		if ipv4Only {
			network = "udp4"
		}
		return netdial.ListenPacket(context.Background(), network, ":0")
	}

	if mux == nil {
		return nil, fmt.Errorf("muxproxy: mux not initialized")
	}

	stream, err := mux.OpenStream(context.Background(), map[string]any{
		"host": host,
		"port": port,
		"udp":  true,
	})
	if err != nil {
		return nil, fmt.Errorf("muxproxy: udp open stream: %w", err)
	}

	return newDatagramStream(stream, datagramAddr(addr)), nil
}

func Parse(_ *url.URL) (proxy.Proxy, error) {
	return &MuxProxy{}, nil
}

func init() {
	proxy.RegisterProtocol("mux", Parse)
}
