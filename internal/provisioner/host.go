package provisioner

import (
	"net"
	"strings"
)

func LocalHostIPs() []string {
	seen := map[string]struct{}{"127.0.0.1": {}, "::1": {}}
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil {
				continue
			}
			seen[ip.String()] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for ip := range seen {
		out = append(out, ip)
	}
	return out
}

func IsLocalHost(host string, localIPs []string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	if host == "" {
		return false
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	if len(localIPs) == 0 {
		localIPs = LocalHostIPs()
	}
	for _, ip := range localIPs {
		if strings.Trim(strings.TrimSpace(ip), "[]") == host {
			return true
		}
	}
	return false
}
