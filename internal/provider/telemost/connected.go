package telemost

import "github.com/pion/webrtc/v4"

func (s *signaling) ConnectedIPs() []string {
	var ips []string
	seen := map[string]bool{}

	add := func(pc *webrtc.PeerConnection) {
		if pc == nil {
			return
		}
		for _, sender := range pc.GetSenders() {
			if tr := sender.Transport(); tr != nil {
				if ice := tr.ICETransport(); ice != nil {
					if pair, err := ice.GetSelectedCandidatePair(); err == nil && pair != nil && pair.Remote != nil {
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
					if pair, err := ice.GetSelectedCandidatePair(); err == nil && pair != nil && pair.Remote != nil {
						if addr := pair.Remote.Address; addr != "" && !seen[addr] {
							seen[addr] = true
							ips = append(ips, addr)
						}
					}
				}
			}
		}
	}

	s.pcMu.RLock()
	pubPC, subPC := s.pubPC, s.subPC
	s.pcMu.RUnlock()

	add(pubPC)
	add(subPC)
	return ips
}

func (p *Pipe) ConnectedIPs() []string {
	p.mu.RLock()
	sig := p.sig
	p.mu.RUnlock()

	var out []string
	seen := make(map[string]struct{})
	if sig != nil {
		for _, ip := range sig.ConnectedIPs() {
			if _, ok := seen[ip]; ok {
				continue
			}
			seen[ip] = struct{}{}
			out = append(out, ip)
		}
	}
	for _, ip := range p.bypass.list() {
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		out = append(out, ip)
	}
	return out
}
