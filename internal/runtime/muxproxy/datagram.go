package muxproxy

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
)

type datagramAddr string

func (a datagramAddr) Network() string { return "udp" }
func (a datagramAddr) String() string  { return string(a) }

type datagramStream struct {
	stream multiplexing.Stream
	remote net.Addr

	readMu sync.Mutex
}

func newDatagramStream(stream multiplexing.Stream, remote net.Addr) *datagramStream {
	return &datagramStream{stream: stream, remote: remote}
}

func (d *datagramStream) LocalAddr() net.Addr  { return datagramAddr("udp") }
func (d *datagramStream) RemoteAddr() net.Addr { return d.remote }
func (d *datagramStream) Close() error         { return d.stream.Close() }

func (d *datagramStream) SetDeadline(t time.Time) error {
	if err := d.stream.SetReadDeadline(t); err != nil {
		return err
	}
	return d.stream.SetWriteDeadline(t)
}

func (d *datagramStream) SetReadDeadline(t time.Time) error  { return d.stream.SetReadDeadline(t) }
func (d *datagramStream) SetWriteDeadline(t time.Time) error { return d.stream.SetWriteDeadline(t) }

func (d *datagramStream) WriteTo(p []byte, _ net.Addr) (int, error) {
	if len(p) > 65535 {
		return 0, errors.New("muxproxy: datagram too large")
	}
	buf := make([]byte, 2+len(p))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(p)))
	copy(buf[2:], p)
	if _, err := d.stream.Write(buf); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (d *datagramStream) ReadFrom(p []byte) (int, net.Addr, error) {
	d.readMu.Lock()
	defer d.readMu.Unlock()

	hdr := make([]byte, 2)
	if _, err := io.ReadFull(d.stream, hdr); err != nil {
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint16(hdr))
	if n == 0 {
		return 0, d.remote, nil
	}
	if n > len(p) {
		discard := make([]byte, n)
		_, _ = io.ReadFull(d.stream, discard)
		return 0, nil, errors.New("muxproxy: datagram too large for buffer")
	}
	if _, err := io.ReadFull(d.stream, p[:n]); err != nil {
		return 0, nil, err
	}
	return n, d.remote, nil
}
