package panel

import (
	"fmt"
	"net"
	"strings"
)

func ValidateAdminListen(addr string, allowPublic bool) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return fmt.Errorf("panel: admin listen address is empty")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("panel: invalid admin listen %q: %w", addr, err)
	}
	if strings.TrimSpace(port) == "" {
		return fmt.Errorf("panel: admin listen %q has no port", addr)
	}
	if isLoopbackHost(host) || allowPublic {
		return nil
	}
	return fmt.Errorf("panel: admin listen %q is not loopback; use 127.0.0.1:8080 over an SSH tunnel or pass -allow-public-admin to override", addr)
}

func IsLoopbackListen(addr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return false
	}
	return isLoopbackHost(host)
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
		return ip.IsLoopback()
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return false
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return false
		}
	}
	return true
}
