package kcp

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	kcp "github.com/xtaci/kcp-go/v5"

	"github.com/Esteviredzu/shampoore-core/internal/core/security"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

const resetCooldown = 2 * time.Second

type Config struct {
	Conv     uint32
	MTU      int
	Window   int
	NoDelay  int
	Interval int
	Resend   int
	NC       bool

	FECEnabled      bool
	FECDataShards   int
	FECParityShards int

	EncryptionPassword string
}

func DefaultConfig() Config {
	return Config{
		Window:   64,
		NoDelay:  1,
		Interval: 10,
		Resend:   2,
		NC:       true,
	}
}

type pendingChunk struct {
	data []byte
	key  string
}

type Session struct {
	pipe   transport.Pipe
	cfg    Config
	logger logging.ContextLogger

	conv uint32
	kcp  *kcp.KCP

	kcpMu       sync.Mutex
	lastResetAt time.Time

	outCount int
	outMu    sync.Mutex

	pendingOutput []pendingChunk

	bytesDelivered atomic.Int64
	bytesSent      atomic.Int64

	ctr transport.Counters

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool

	mu     sync.Mutex
	closed bool
}

func NewSession(
	pipe transport.Pipe,
	logger logging.ContextLogger,
	cfg Config,
) (*Session, error) {
	if cfg.Window <= 0 {
		cfg.Window = 64
	}
	if cfg.Conv == 0 {
		cfg.Conv = 1
	}

	if cfg.EncryptionPassword != "" {
		wrapped, err := security.WrapPipe(pipe, cfg.EncryptionPassword, logger)
		if err != nil {
			return nil, err
		}
		pipe = wrapped
		logger.Info("kcp: encryption enabled")
	}

	if cfg.FECEnabled {
		fecPipe, err := security.NewFECPipe(pipe, cfg.FECDataShards, cfg.FECParityShards)
		if err != nil {
			return nil, err
		}
		pipe = fecPipe
		logger.Info("kcp: FEC enabled; data_shards=", cfg.FECDataShards,
			" parity_shards=", cfg.FECParityShards)
	}

	mtuCeiling := pipe.MaxMessageSize() - 24
	if mtuCeiling < 64 {
		return nil, errors.New("kcp: MTU too small")
	}
	if cfg.MTU <= 0 {
		cfg.MTU = mtuCeiling
	}
	if cfg.MTU > mtuCeiling {
		logger.Info("kcp: MTU clamped ", cfg.MTU, " -> ", mtuCeiling)
		cfg.MTU = mtuCeiling
	}

	const maxKCPMTU = 1200
	if cfg.MTU > maxKCPMTU {
		cfg.MTU = maxKCPMTU
	}

	s := &Session{
		pipe:   pipe,
		cfg:    cfg,
		logger: logger,
		conv:   cfg.Conv,
	}

	k := kcp.NewKCP(s.conv, s.output)
	if k == nil {
		return nil, errors.New("kcp: failed to create KCP instance")
	}
	k.SetMtu(cfg.MTU)
	k.WndSize(cfg.Window, cfg.Window)
	k.NoDelay(cfg.NoDelay, cfg.Interval, cfg.Resend, boolToInt(cfg.NC))
	s.kcp = k

	return s, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (s *Session) MaxMessageSize() int {
	return s.pipe.MaxMessageSize()
}

func (s *Session) BytesDelivered() int64 {
	return s.bytesDelivered.Load()
}

func (s *Session) BytesAcked() int64 {
	s.kcpMu.Lock()
	wait := s.kcp.WaitSnd()
	s.kcpMu.Unlock()
	acked := s.bytesSent.Load() - int64(wait*s.cfg.MTU)
	if acked < 0 {
		return 0
	}
	return acked
}

func (s *Session) WalkLayers() []transport.Layer {
	out := []transport.Layer{{
		Kind: "transport", Type: "kcp",
		Snapshot: s.ctr.Snapshot(),
	}}
	if tw, ok := s.pipe.(transport.Telemetry); ok {
		out = append(out, tw.WalkLayers()...)
	}
	return out
}

func (s *Session) output(buf []byte, size int) {
	maxChunk := s.pipe.MaxMessageSize()

	start := 0
	for start < size {
		end := start
		for end < size {
			if end+24 > size {
				break
			}
			segLen := 24 + int(binary.LittleEndian.Uint32(buf[end+20:end+24]))
			if end+segLen > size {
				break
			}
			if end+segLen-start > maxChunk {
				break
			}
			end += segLen
		}
		if end == start {
			end = size
		}

		data := make([]byte, end-start)
		copy(data, buf[start:end])

		s.outMu.Lock()
		s.outCount++
		n := s.outCount
		s.outMu.Unlock()
		if n <= 5 || n%1000 == 0 {
			log.Printf("[kcp] output #%d size=%d", n, end-start)
		}

		key := uuid.NewString()
		if err := s.pipe.SendMessage(s.ctx, data, key); err != nil {
			s.outMu.Lock()
			s.pendingOutput = append(s.pendingOutput, pendingChunk{data: data, key: key})
			s.outMu.Unlock()
			s.logger.Debug("kcp: output deferred: ", err)
			continue
		}
		s.ctr.NetworkSent.Add(int64(len(data)))
		start = end
	}
}

func (s *Session) flushPendingOutput() {
	s.outMu.Lock()
	if len(s.pendingOutput) == 0 {
		s.outMu.Unlock()
		return
	}
	batch := s.pendingOutput
	s.pendingOutput = nil
	s.outMu.Unlock()

	for i, p := range batch {
		if err := s.pipe.SendMessage(s.ctx, p.data, p.key); err != nil {
			s.outMu.Lock()
			s.pendingOutput = append(batch[i:], s.pendingOutput...)
			s.outMu.Unlock()
			return
		}
		s.ctr.NetworkSent.Add(int64(len(p.data)))
	}
}

func (s *Session) Start(ctx context.Context) error {
	if s.started {
		return nil
	}
	if err := s.pipe.Start(ctx); err != nil {
		return err
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.started = true

	s.logger.Info("kcp: started; conv=", s.conv,
		" mtu=", s.cfg.MTU,
		" window=", s.cfg.Window)

	s.wg.Add(2)
	go s.recvLoop()
	go s.updateLoop()
	return nil
}

func (s *Session) Reset() error {
	s.kcpMu.Lock()
	defer s.kcpMu.Unlock()

	now := time.Now()
	if !s.lastResetAt.IsZero() && now.Sub(s.lastResetAt) < resetCooldown {
		s.logger.Debug("kcp: reset throttled (last ", now.Sub(s.lastResetAt), ")")
		return nil
	}
	s.lastResetAt = now

	k := kcp.NewKCP(s.conv, s.output)
	if k == nil {
		return errors.New("kcp: reset failed to create KCP instance")
	}
	k.SetMtu(s.cfg.MTU)
	k.WndSize(s.cfg.Window, s.cfg.Window)
	k.NoDelay(s.cfg.NoDelay, s.cfg.Interval, s.cfg.Resend, boolToInt(s.cfg.NC))
	s.kcp = k

	s.logger.Info("kcp: session reset; conv=", s.conv)
	return nil
}

func (s *Session) recvLoop() {
	defer s.wg.Done()
	inCount := 0
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		data, err := s.pipe.ReceiveMessage(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			s.logger.Warn("kcp: recv error: ", err)
			return
		}

		inCount++
		s.ctr.NetworkRecv.Add(int64(len(data)))
		if inCount <= 5 || inCount%1000 == 0 {
			log.Printf("[kcp] input #%d size=%d", inCount, len(data))
		}

		s.kcpMu.Lock()
		ret := s.kcp.Input(data, kcp.IKCP_PACKET_REGULAR, false)
		s.kcpMu.Unlock()
		if ret < 0 {
			s.logger.Warn("kcp: Input failed with code ", ret, " for ", len(data), " bytes")
		}
	}
}

func (s *Session) updateLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.flushPendingOutput()
			s.kcpMu.Lock()
			s.kcp.Update()
			s.kcpMu.Unlock()
		}
	}
}

func (s *Session) Send(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.kcpMu.Lock()
	ret := s.kcp.Send(data)
	s.kcpMu.Unlock()
	if ret < 0 {
		return errors.New("kcp: send failed")
	}
	s.bytesSent.Add(int64(len(data)))
	s.ctr.UsefulSent.Add(int64(len(data)))
	return nil
}

func (s *Session) Receive(ctx context.Context) ([]byte, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		s.mu.Lock()
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return nil, io.EOF
		}

		s.kcpMu.Lock()
		size := s.kcp.PeekSize()
		if size < 0 {
			s.kcpMu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}

		buf := make([]byte, size)
		n := s.kcp.Recv(buf)
		if n == -2 {
			buf = make([]byte, size+1024)
			n = s.kcp.Recv(buf)
		}
		s.kcpMu.Unlock()

		if n < 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}

		out := make([]byte, n)
		copy(out, buf[:n])
		s.bytesDelivered.Add(int64(n))
		s.ctr.UsefulRecv.Add(int64(n))
		return out, nil
	}
}

func (s *Session) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	return s.pipe.Stop(ctx)
}

var _ transport.Transport = (*Session)(nil)
var _ transport.Telemetry = (*Session)(nil)