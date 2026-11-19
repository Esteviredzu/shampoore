package telemost

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/netdial"
)

func Bootstrap(ctx context.Context, cfg *Config) (*bootstrapResult, error) {
	return bootstrap(ctx, cfg)
}

type bypassCache struct {
	mu  sync.RWMutex
	ips map[string]struct{}
}

func (c *bypassCache) replace(hosts []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resolved := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		for _, ip := range netdial.ResolveIPv4(ctx, host) {
			resolved[ip] = struct{}{}
		}
	}

	c.mu.Lock()
	c.ips = resolved
	c.mu.Unlock()
}

func (c *bypassCache) clear() {
	c.mu.Lock()
	c.ips = nil
	c.mu.Unlock()
}

func (c *bypassCache) list() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.ips) == 0 {
		return nil
	}
	out := make([]string, 0, len(c.ips))
	for ip := range c.ips {
		out = append(out, ip)
	}
	return out
}

func telemostHosts(boot *bootstrapResult) []string {
	hosts := []string{
		"telemost.yandex.ru",
		"cloud-api.yandex.ru",
	}
	if boot == nil || boot.info == nil {
		return hosts
	}
	if u, err := url.Parse(boot.info.MediaServerURL); err == nil && u.Hostname() != "" {
		hosts = append(hosts, u.Hostname())
	}
	for _, ice := range boot.info.ICEServers {
		for _, raw := range ice.URLs {
			if h := iceHostFromURL(raw); h != "" {
				hosts = append(hosts, h)
			}
		}
	}
	return hosts
}

func (p *Pipe) updateBypassHosts(boot *bootstrapResult) {
	p.bypass.replace(telemostHosts(boot))
}

func (p *Pipe) BypassHosts() []string {
	p.mu.RLock()
	boot := p.boot
	p.mu.RUnlock()
	bypass := telemostHosts(boot)
	bypass = append(bypass, p.bypass.list()...)
	return bypass
}
