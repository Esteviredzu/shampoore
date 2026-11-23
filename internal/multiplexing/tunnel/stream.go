package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const maxQueuedBytes = 64 * 1024 * 1024

type TunnelStream struct {
	mux      *TunnelMultiplexer
	id       StreamID
	idStr    string
	metadata map[string]any

	recvMu     sync.Mutex
	recvCond   *sync.Cond
	recvQueue  [][]byte
	recvBytes  int
	recvClosed bool
	recvErr    error

	readBuf []byte

	dlMu          sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
	readTimer     *time.Timer

	ackCh   chan error
	ackOnce sync.Once

	closed    atomic.Bool
	closeOnce sync.Once
}

func newTunnelStream(mux *TunnelMultiplexer, id StreamID, metadata map[string]any) *TunnelStream {
	meta := make(map[string]any, len(metadata))
	for k, v := range metadata {
		meta[k] = v
	}
	s := &TunnelStream{
		mux:      mux,
		id:       id,
		idStr:    uuid.UUID(id).String(),
		metadata: meta,
		ackCh:    make(chan error, 1),
	}
	s.recvCond = sync.NewCond(&s.recvMu)
	return s
}

func (s *TunnelStream) notifyAck(err error) {
	s.ackOnce.Do(func() {
		select {
		case s.ackCh <- err:
		default:
		}
	})
}

func (s *TunnelStream) ID() string               { return s.idStr }
func (s *TunnelStream) Metadata() map[string]any { return s.metadata }
func (s *TunnelStream) Closed() bool             { return s.closed.Load() }

func (s *TunnelStream) Read(b []byte) (int, error) {
	s.recvMu.Lock()
	if len(s.readBuf) > 0 {
		n := copy(b, s.readBuf)
		s.readBuf = s.readBuf[n:]
		s.recvMu.Unlock()
		return n, nil
	}

	for len(s.recvQueue) == 0 && !s.recvClosed {
		if s.readExpiredLocked() {
			s.recvMu.Unlock()
			return 0, os.ErrDeadlineExceeded
		}
		s.recvCond.Wait()
	}
	if len(s.recvQueue) == 0 {
		err := s.recvErr
		s.recvMu.Unlock()
		if err != nil {
			return 0, err
		}
		return 0, io.EOF
	}
	data := s.recvQueue[0]
	s.recvQueue[0] = nil
	s.recvQueue = s.recvQueue[1:]
	s.recvBytes -= len(data)

	n := copy(b, data)
	if n < len(data) {
		s.readBuf = data[n:]
	}
	s.recvMu.Unlock()
	return n, nil
}

func (s *TunnelStream) Write(b []byte) (int, error) {
	if s.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	ctx := context.Background()
	if deadline, ok := s.writeDeadlineValue(); ok {
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		if !deadline.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
	}
	if err := s.mux.writeDataCtx(ctx, s.id, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (s *TunnelStream) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.recvMu.Lock()
		s.recvClosed = true
		if s.recvErr == nil {
			s.recvErr = io.EOF
		}
		s.recvCond.Broadcast()
		s.recvMu.Unlock()
		err = s.mux.closeStream(s.id)
	})
	return err
}

func (s *TunnelStream) deliverData(data []byte) {
	s.recvMu.Lock()
	if s.recvClosed {
		s.recvMu.Unlock()
		return
	}
	if s.recvBytes+len(data) > maxQueuedBytes {
		s.recvClosed = true
		s.recvErr = errors.New("tunnel: receive queue overflow")
		s.recvCond.Broadcast()
		s.recvMu.Unlock()

		if s.mux != nil {
			s.mux.mu.Lock()
			delete(s.mux.streams, s.id)
			s.mux.mu.Unlock()
		}
		return
	}
	s.recvBytes += len(data)
	s.recvQueue = append(s.recvQueue, data)
	s.recvCond.Signal()
	s.recvMu.Unlock()
}

func (s *TunnelStream) deliverEOF(err error) {
	s.notifyAck(nil)
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.recvMu.Lock()
		s.recvClosed = true
		s.recvErr = err
		s.recvCond.Broadcast()
		s.recvMu.Unlock()
	})
}

func (s *TunnelStream) abort(err error) {
	if err == nil {
		err = io.EOF
	}
	s.notifyAck(err)
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.recvMu.Lock()
		s.recvClosed = true
		s.recvErr = err
		s.recvQueue = nil
		s.recvBytes = 0
		s.readBuf = nil
		s.recvCond.Broadcast()
		s.recvMu.Unlock()
	})
}

func (s *TunnelStream) LocalAddr() net.Addr  { return dummyAddr("tunnel") }
func (s *TunnelStream) RemoteAddr() net.Addr { return dummyAddr("tunnel") }

func (s *TunnelStream) SetDeadline(t time.Time) error {
	if err := s.SetReadDeadline(t); err != nil {
		return err
	}
	return s.SetWriteDeadline(t)
}

func (s *TunnelStream) SetReadDeadline(t time.Time) error {
	s.dlMu.Lock()
	s.readDeadline = t
	old := s.readTimer
	s.readTimer = nil
	if !t.IsZero() {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		s.readTimer = time.AfterFunc(d, s.wakeRead)
	}
	s.dlMu.Unlock()
	if old != nil {
		old.Stop()
	}
	if !t.IsZero() && !time.Now().Before(t) {
		s.wakeRead()
	}
	return nil
}

func (s *TunnelStream) SetWriteDeadline(t time.Time) error {
	s.dlMu.Lock()
	s.writeDeadline = t
	s.dlMu.Unlock()
	return nil
}

func (s *TunnelStream) readExpiredLocked() bool {
	s.dlMu.Lock()
	defer s.dlMu.Unlock()
	if s.readDeadline.IsZero() {
		return false
	}
	return !time.Now().Before(s.readDeadline)
}

func (s *TunnelStream) writeDeadlineValue() (time.Time, bool) {
	s.dlMu.Lock()
	defer s.dlMu.Unlock()
	return s.writeDeadline, !s.writeDeadline.IsZero()
}

func (s *TunnelStream) wakeRead() {
	s.recvMu.Lock()
	s.recvCond.Broadcast()
	s.recvMu.Unlock()
}

type dummyAddr string

func (d dummyAddr) Network() string { return string(d) }
func (d dummyAddr) String() string  { return string(d) }
