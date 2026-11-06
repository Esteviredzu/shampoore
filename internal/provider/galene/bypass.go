package galene

import (
	"net/url"
	"strings"

	"github.com/pion/webrtc/v4"
)

func (p *Provider) BypassHosts() []string {
	seen := map[string]bool{}
	var out []string
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}

	if u, err := url.Parse(p.cfg.GroupURL); err == nil && u.Hostname() != "" {
		add(u.Hostname())
	}

	for _, srv := range p.iceServers {
		for _, raw := range srv.URLs {
			if h := iceHostFromURL(raw); h != "" {
				add(h)
			}
		}
	}
	return out
}

func (p *Provider) ConnectedIPs() []string {
	var ips []string
	seen := map[string]bool{}

	addPC := func(pc *webrtc.PeerConnection) {
		if pc == nil {
			return
		}
		for _, sender := range pc.GetSenders() {
			if tr := sender.Transport(); tr != nil {
				if ice := tr.ICETransport(); ice != nil {
					if pair, err := ice.GetSelectedCandidatePair(); err == nil &&
						pair != nil && pair.Remote != nil {
						if addr := pair.Remote.Address; addr != "" && !seen[addr] {
							seen[addr] = true
							ips = append(ips, addr)
						}
					}
				}
			}
		}
		for _, receiver := range pc.GetReceivers() {
			if tr := receiver.Transport(); tr != nil {
				if ice := tr.ICETransport(); ice != nil {
					if pair, err := ice.GetSelectedCandidatePair(); err == nil &&
						pair != nil && pair.Remote != nil {
						if addr := pair.Remote.Address; addr != "" && !seen[addr] {
							seen[addr] = true
							ips = append(ips, addr)
						}
					}
				}
			}
		}
	}

	addPC(p.pubPC)

	p.subPCmu.Lock()
	subPC := p.subPC
	p.subPCmu.Unlock()
	addPC(subPC)

	return ips
}

func (p *Provider) ConnectedIPsFunc() func() []string {
	return p.ConnectedIPs
}

func iceHostFromURL(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	for _, p := range []string{"stuns:", "stun:", "turns:", "turn:"} {
		if strings.HasPrefix(s, p) {
			s = s[len(p):]
			if i := strings.LastIndexByte(s, '@'); i >= 0 {
				s = s[i+1:]
			}
			if i := strings.IndexAny(s, "?#"); i >= 0 {
				s = s[:i]
			}
			if i := strings.IndexByte(s, ':'); i >= 0 {
				return s[:i]
			}
			return s
		}
	}
	if u, err := url.Parse(s); err == nil {
		return u.Hostname()
	}
	return ""
}
