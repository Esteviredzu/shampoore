package netdial

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"time"
)

const dialTimeout = 30 * time.Second

type Protector func(fd int) bool

var (
	mu        sync.RWMutex
	protector Protector
)

func SetProtector(p Protector) {
	mu.Lock()
	protector = p
	mu.Unlock()
}

func control(_, _ string, c syscall.RawConn) error {
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
		Control: control,
	}
	return d.DialContext(ctx, network, address)
}

func ListenPacket(ctx context.Context, network, address string) (net.PacketConn, error) {
	lc := net.ListenConfig{Control: control}
	return lc.ListenPacket(ctx, network, address)
}
