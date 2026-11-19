package netdial

import (
	"net"
	"sync/atomic"
	"testing"
)

func TestPionNetListenUDPProtectsSocket(t *testing.T) {
	network, err := NewPionNet()
	if err != nil {
		t.Fatalf("new pion net: %v", err)
	}

	var calls atomic.Int32
	SetProtector(func(fd int) bool {
		if fd < 0 {
			t.Errorf("invalid fd: %d", fd)
		}
		calls.Add(1)
		return true
	})
	defer SetProtector(nil)

	conn, err := network.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer conn.Close()

	if calls.Load() == 0 {
		t.Fatal("pion net listener did not protect its socket")
	}
}
