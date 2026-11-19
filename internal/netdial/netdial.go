package netdial

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	dialTimeout      = 30 * time.Second
	resolveTimeout   = 5 * time.Second
	idleConnTimeout  = 90 * time.Second
	tlsHandshakeTime = 10 * time.Second
)

type Protector func(fd int) bool

var (
	mu        sync.RWMutex
	protector Protector

	defaultTransportOnce sync.Once
)

func SetProtector(p Protector) {
	mu.Lock()
	protector = p
	mu.Unlock()
}

func Control(_, _ string, c syscall.RawConn) error {
	mu.RLock()
	p := protector
	mu.RUnlock()
	if p == nil {
		return nil
	}
	var protectErr error
	if err := c.Control(func(fd uintptr) {
		if !p(int(fd)) {
			protectErr = errors.New("netdial: socket protect failed")
		}
	}); err != nil {
		return err
	}
	return protectErr
}

func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d := net.Dialer{
		Timeout: dialTimeout,
		Control: Control,
	}
	return d.DialContext(ctx, network, address)
}

func ListenPacket(ctx context.Context, network, address string) (net.PacketConn, error) {
	lc := net.ListenConfig{Control: Control}
	return lc.ListenPacket(ctx, network, address)
}

func NewHTTPTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTime,
		ExpectContinueTimeout: time.Second,
	}
}

func InstallDefaultTransport() {
	defaultTransportOnce.Do(func() {
		http.DefaultTransport = NewHTTPTransport()
	})
}

func ResolveIPv4(ctx context.Context, host string) []string {
	host = strings.TrimSpace(host)
	if host == "" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return []string{v4.String()}
		}
		return nil
	}

	rctx := ctx
	if rctx == nil {
		rctx = context.Background()
	}
	if _, ok := rctx.Deadline(); !ok {
		var cancel context.CancelFunc
		rctx, cancel = context.WithTimeout(rctx, resolveTimeout)
		defer cancel()
	}

	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {

			return DialContext(ctx, network, address)
		},
	}
	ips, err := resolver.LookupIP(rctx, "ip4", host)
	if err != nil {

		ips, err = net.DefaultResolver.LookupIP(rctx, "ip4", host)
	}
	if err != nil {
		return nil
	}
	seen := make(map[string]struct{}, len(ips))
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		v4 := ip.To4()
		if v4 == nil {
			continue
		}
		s := v4.String()
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
