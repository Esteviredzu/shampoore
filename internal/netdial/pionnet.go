package netdial

import (
	"context"
	"errors"
	"net"

	"github.com/pion/transport/v5"
	"github.com/pion/transport/v5/stdnet"
)

type Net struct {
	*stdnet.Net
}

func NewPionNet() (*Net, error) {
	base, err := stdnet.NewNet()
	if err != nil {
		return nil, err
	}
	return &Net{Net: base}, nil
}

func addrOrAny(addr net.Addr) string {
	if addr == nil {
		return ":0"
	}
	return addr.String()
}

func (n *Net) ListenPacket(network, address string) (net.PacketConn, error) {
	lc := net.ListenConfig{Control: Control}
	return lc.ListenPacket(context.Background(), network, address)
}

func (n *Net) ListenUDP(network string, laddr *net.UDPAddr) (transport.UDPConn, error) {
	lc := net.ListenConfig{Control: Control}
	pc, err := lc.ListenPacket(context.Background(), network, addrOrAny(laddr))
	if err != nil {
		return nil, err
	}
	uc, ok := pc.(*net.UDPConn)
	if !ok {
		return nil, errors.Join(errors.New("netdial: listener is not a UDP connection"), pc.Close())
	}
	return uc, nil
}

func (n *Net) ListenTCP(network string, laddr *net.TCPAddr) (transport.TCPListener, error) {
	lc := net.ListenConfig{Control: Control}
	ln, err := lc.Listen(context.Background(), network, addrOrAny(laddr))
	if err != nil {
		return nil, err
	}
	tl, ok := ln.(*net.TCPListener)
	if !ok {
		return nil, errors.Join(errors.New("netdial: listener is not a TCP listener"), ln.Close())
	}
	return tcpListener{TCPListener: tl}, nil
}

func (n *Net) Dial(network, address string) (net.Conn, error) {
	d := net.Dialer{Control: Control}
	return d.Dial(network, address)
}

func (n *Net) DialUDP(network string, laddr, raddr *net.UDPAddr) (transport.UDPConn, error) {
	d := net.Dialer{Control: Control}
	if laddr != nil {
		d.LocalAddr = laddr
	}
	c, err := d.Dial(network, addrOrAny(raddr))
	if err != nil {
		return nil, err
	}
	uc, ok := c.(*net.UDPConn)
	if !ok {
		return nil, errors.Join(errors.New("netdial: connection is not a UDP connection"), c.Close())
	}
	return uc, nil
}

func (n *Net) DialTCP(network string, laddr, raddr *net.TCPAddr) (transport.TCPConn, error) {
	d := net.Dialer{Control: Control}
	if laddr != nil {
		d.LocalAddr = laddr
	}
	c, err := d.Dial(network, addrOrAny(raddr))
	if err != nil {
		return nil, err
	}
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return nil, errors.Join(errors.New("netdial: connection is not a TCP connection"), c.Close())
	}
	return tc, nil
}

type tcpListener struct {
	*net.TCPListener
}

func (l tcpListener) AcceptTCP() (transport.TCPConn, error) {
	return l.TCPListener.AcceptTCP()
}

var _ transport.Net = (*Net)(nil)
