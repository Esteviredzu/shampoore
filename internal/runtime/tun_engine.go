//go:build linux || android

package runtime

import (
	"fmt"
	"strconv"
	"sync"

	"golang.org/x/sys/unix"
	gvisorstack "gvisor.dev/gvisor/pkg/tcpip/stack"

	t2score "github.com/xjasonlyu/tun2socks/v2/core"
	"github.com/xjasonlyu/tun2socks/v2/core/device"
	"github.com/xjasonlyu/tun2socks/v2/core/device/fdbased"
	"github.com/xjasonlyu/tun2socks/v2/tunnel"

	"github.com/Esteviredzu/shampoore/internal/runtime/muxproxy"
)

type tunEngine struct {
	device device.Device
	stack  *gvisorstack.Stack

	once sync.Once
}

func newTunEngine(fd, mtu int) (*tunEngine, error) {
	if fd < 0 {
		return nil, fmt.Errorf("tun: invalid fd %d", fd)
	}
	if mtu <= 0 {
		mtu = 1500
	}

	dev, err := fdbased.Open(strconv.Itoa(fd), uint32(mtu), 0)
	if err != nil {
		return nil, fmt.Errorf("tun: open fd %d: %w", fd, err)
	}

	tunnel.T().SetProxy(&muxproxy.MuxProxy{})
	stack, err := t2score.CreateStack(&t2score.Config{
		LinkEndpoint:     dev,
		TransportHandler: tunnel.T(),
	})
	if err != nil {
		dev.Close()
		return nil, fmt.Errorf("tun: create stack: %w", err)
	}

	return &tunEngine{device: dev, stack: stack}, nil
}

func (e *tunEngine) stop() {
	if e == nil {
		return
	}
	e.once.Do(func() {
		if e.device != nil {
			e.device.Close()
		}
		if e.stack != nil {
			e.stack.Close()
			e.stack.Wait()
		}
	})
}

func fdAlive(fd int) bool {
	if fd < 0 {
		return false
	}
	_, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	return err == nil
}
