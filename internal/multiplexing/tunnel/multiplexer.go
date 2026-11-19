package tunnel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	maxOutboundBuf   = 64 * 1024 * 1024
	outboundQueueCap = 1024

	stopFlushTimeout  = 2 * time.Second
	closeFlushTimeout = time.Second

	maxRetiredEpochs = 8
)

var errWriterStopped = errors.New("tunnel: writer stopped")

type Config struct {
	FlushThreshold int
	FlushInterval  time.Duration

	UserID     string
	ExitNodeID string

	Registry *auth.UserRegistry
}

func DefaultConfig() Config {
	return Config{
		FlushThreshold: 12000,
		FlushInterval:  20 * time.Millisecond,
	}
}

type outboundCmd struct {
	frame []byte
	epoch uint32
	done  chan error
	ctx   context.Context
}

type TunnelMultiplexer struct {
	transport transport.Transport
	logger    logging.ContextLogger
	cfg       Config

	mu      sync.Mutex
	streams map[StreamID]*TunnelStream
	closed  bool

	epoch atomic.Uint32

	remoteEpoch    uint32
	remoteEpochSet bool
	retiredEpochs  map[uint32]struct{}
	retiredOrder   []uint32

	authorized   bool
	sessionUser  string
	sessionExit  string
	authRejected bool
	authRequired bool

	incoming chan multiplexing.Stream

	outbound    chan outboundCmd
	queuedBytes atomic.Int64
	writerDone  chan struct{}

	ctr transport.Counters

	ctx    context.Context
	cancel context.CancelFunc

	stateMu sync.Mutex
	started bool
	wg      sync.WaitGroup
}

func New(
	t transport.Transport,
	logger logging.ContextLogger,
	cfg Config,
) *TunnelMultiplexer {
	if cfg.FlushThreshold <= 0 {
		cfg.FlushThreshold = 12000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 20 * time.Millisecond
	}
	if max := t.MaxMessageSize(); max > 0 && cfg.FlushThreshold > max {
		cfg.FlushThreshold = max
	}
	m := &TunnelMultiplexer{
		transport:     t,
		logger:        logger,
		cfg:           cfg,
		streams:       make(map[StreamID]*TunnelStream),
		retiredEpochs: make(map[uint32]struct{}),
		incoming:      make(chan multiplexing.Stream, 64),
		outbound:      make(chan outboundCmd, outboundQueueCap),
		writerDone:    make(chan struct{}),
	}
	m.epoch.Store(newSessionEpoch())
	return m
}

func newSessionEpoch() uint32 {
	id := uuid.New()
	epoch := binary.BigEndian.Uint32(id[:4])
	if epoch == 0 {
		epoch = 1
	}
	return epoch
}

func nextSessionEpoch(epoch uint32) uint32 {
	epoch++
	if epoch == 0 {
		epoch = 1
	}
	return epoch
}

func (m *TunnelMultiplexer) Start(ctx context.Context) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	if m.started {
		return nil
	}
	if err := m.transport.Start(ctx); err != nil {
		return err
	}
	muxCtx, cancel := context.WithCancel(ctx)
	writerDone := make(chan struct{})

	m.drainOutbound(errWriterStopped)
	m.queuedBytes.Store(0)
	drainIncomingStreams(m.incoming)

	m.mu.Lock()
	m.ctx = muxCtx
	m.cancel = cancel
	m.closed = false
	m.epoch.Store(newSessionEpoch())
	m.streams = make(map[StreamID]*TunnelStream)
	m.remoteEpoch = 0
	m.remoteEpochSet = false
	m.retiredEpochs = make(map[uint32]struct{})
	m.retiredOrder = nil
	m.writerDone = writerDone
	m.authorized = false
	m.sessionUser = ""
	m.sessionExit = ""
	m.authRejected = false
	m.authRequired = m.cfg.Registry != nil && m.cfg.Registry.Active()
	m.mu.Unlock()

	m.started = true

	m.wg.Add(2)
	go m.receiveLoop()
	go m.writeLoop(muxCtx, writerDone)

	if m.hasClientIdentity() {
		helloCtx, helloCancel := context.WithTimeout(muxCtx, 2*time.Second)
		m.sendHello(helloCtx)
		helloCancel()
	}
	return nil
}

func (m *TunnelMultiplexer) hasClientIdentity() bool {
	return strings.TrimSpace(m.cfg.UserID) != "" || strings.TrimSpace(m.cfg.ExitNodeID) != ""
}

func (m *TunnelMultiplexer) requiresHelloLocked() bool {
	if m.cfg.Registry == nil {
		return false
	}
	if m.cfg.Registry.Active() {

		m.authRequired = true
	}
	return m.authRequired
}

// SessionIdentity returns the user and exit node confirmed by a FrameHello.
// It is safe for concurrent use.
func (m *TunnelMultiplexer) SessionIdentity() (userID, exitNodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessionUser, m.sessionExit
}

func (m *TunnelMultiplexer) sendHello(ctx context.Context) {
	if ctx == nil {
		return
	}
	frame := &TunnelFrame{
		Type: FrameHello,
		Hello: &Hello{
			UserID:     m.cfg.UserID,
			ExitNodeID: m.cfg.ExitNodeID,
		},
	}
	if err := m.enqueueFrame(ctx, frame); err != nil {
		m.logger.Warn("tunnel: send hello: ", err)
		return
	}
	if err := m.flushOutbound(ctx); err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, errWriterStopped) {
		m.logger.Debug("tunnel: flush hello: ", err)
	}
}

func (m *TunnelMultiplexer) sendErrorFrame(ctx context.Context, code string, reason error) {
	if ctx == nil {
		return
	}
	message := ""
	if reason != nil {
		message = reason.Error()
	}
	if err := m.enqueueFrame(ctx, &TunnelFrame{
		Type:  FrameError,
		Error: &ErrorFrame{Code: code, Message: message},
	}); err != nil {
		m.logger.Debug("tunnel: send error frame: ", err)
		return
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), closeFlushTimeout)
	defer cancel()
	if err := m.flushOutbound(flushCtx); err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, errWriterStopped) {
		m.logger.Debug("tunnel: flush error frame: ", err)
	}
}

func (m *TunnelMultiplexer) sendReset(ctx context.Context, epoch uint32) {
	if ctx == nil {
		return
	}
	if err := m.enqueueFrame(ctx, &TunnelFrame{
		StreamID: StreamID{},
		Type:     FrameReset,
	}); err != nil {
		m.logger.Warn("tunnel: announce session reset (epoch ", epoch, "): ", err)
		return
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), closeFlushTimeout)
	defer cancel()
	_ = m.flushOutbound(flushCtx)
}

func (m *TunnelMultiplexer) rejectSession(code string, reason error) {
	m.mu.Lock()
	if m.closed || m.authRejected {
		m.mu.Unlock()
		return
	}
	m.authRejected = true
	m.authorized = false
	m.sessionUser = ""
	m.sessionExit = ""
	ctx := m.ctx
	epoch := m.epoch.Load()
	m.mu.Unlock()

	if reason != nil {
		m.logger.Warn("tunnel: rejecting session: ", reason)
	}
	m.sendErrorFrame(ctx, code, reason)
	m.closeAllStreams(reason)
	m.sendReset(ctx, epoch)
}

func authErrorCode(err error) string {
	switch {
	case errors.Is(err, auth.ErrUserNotFound):
		return "unknown_user"
	case errors.Is(err, auth.ErrUserDisabled):
		return "user_disabled"
	case errors.Is(err, auth.ErrUserExpired):
		return "user_expired"
	case errors.Is(err, auth.ErrTrafficLimitExceeded):
		return "traffic_limit"
	case errors.Is(err, auth.ErrExitNotAllowed):
		return "exit_forbidden"
	case errors.Is(err, auth.ErrUserIDRequired):
		return "user_required"
	default:
		return "unauthorized"
	}
}

func drainIncomingStreams(ch chan multiplexing.Stream) {
	for {
		select {
		case s := <-ch:
			if ts, ok := s.(*TunnelStream); ok {
				ts.deliverEOF(io.EOF)
			}
		default:
			return
		}
	}
}

func (m *TunnelMultiplexer) Stop(ctx context.Context) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	if !m.started {
		return nil
	}
	m.started = false

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	streams := make([]*TunnelStream, 0, len(m.streams))
	for _, s := range m.streams {
		streams = append(streams, s)
	}
	m.streams = make(map[StreamID]*TunnelStream)
	cancel := m.cancel
	m.mu.Unlock()

	flushCtx, flushCancel := context.WithTimeout(context.Background(), stopFlushTimeout)
	if err := m.flushOutbound(flushCtx); err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, errWriterStopped) {
		m.logger.Debug("tunnel: final flush failed: ", err)
	}
	flushCancel()

	if cancel != nil {
		cancel()
	}

	for _, s := range streams {
		s.deliverEOF(io.EOF)
	}
	m.wg.Wait()

	return m.transport.Stop(ctx)
}

func (m *TunnelMultiplexer) WalkLayers() []transport.Layer {
	out := []transport.Layer{{
		Kind: "mux", Type: "tunnel",
		Snapshot: m.ctr.Snapshot(),
	}}
	if tw, ok := m.transport.(transport.Telemetry); ok {
		out = append(out, tw.WalkLayers()...)
	}
	return out
}

func (m *TunnelMultiplexer) OpenStream(
	ctx context.Context,
	metadata map[string]any,
) (multiplexing.Stream, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, fmt.Errorf("tunnel: multiplexer is closed")
	}
	if m.authRejected {
		m.mu.Unlock()
		return nil, fmt.Errorf("tunnel: session was rejected")
	}

	if m.ctx == nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("tunnel: multiplexer is not started")
	}
	id := StreamID(uuid.New())
	stream := newTunnelStream(m, id, metadata)
	m.streams[id] = stream
	m.mu.Unlock()

	frame := &TunnelFrame{
		StreamID: id,
		Type:     FrameOpen,
		Metadata: metadata,
	}
	if err := m.enqueueFrame(ctx, frame); err != nil {
		m.mu.Lock()
		delete(m.streams, id)
		m.mu.Unlock()
		return nil, err
	}
	if err := m.flushOutbound(ctx); err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, errWriterStopped) {
		m.logger.Debug("tunnel: flush open frame: ", err)
	}

	if stream.Closed() {
		if err := m.sendCloseFrame(ctx, id); err != nil {
			m.logger.Debug("tunnel: close stream after reset failed: ", err)
		}
	}
	return stream, nil
}

func (m *TunnelMultiplexer) AcceptStream(
	ctx context.Context,
) (multiplexing.Stream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case s, ok := <-m.incoming:
		if !ok {
			return nil, io.EOF
		}
		return s, nil
	}
}

func (m *TunnelMultiplexer) writeData(id StreamID, data []byte) error {
	m.mu.Lock()
	_, ok := m.streams[id]
	closed := m.closed
	rejected := m.authRejected
	ctx := m.ctx
	user := m.sessionUser
	authorized := m.authorized
	needHello := m.requiresHelloLocked()
	m.mu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}
	if rejected {
		return fmt.Errorf("tunnel: session was rejected")
	}
	if ctx == nil {
		return fmt.Errorf("tunnel: multiplexer is not started")
	}
	if !ok {
		return fmt.Errorf("tunnel: stream not found: %s", uuid.UUID(id))
	}
	m.ctr.UsefulSent.Add(int64(len(data)))
	if err := m.enqueueFrame(ctx, &TunnelFrame{
		StreamID: id,
		Type:     FrameData,
		Data:     data,
	}); err != nil {
		return err
	}
	if needHello && authorized {
		if err := m.cfg.Registry.AddTraffic(user, 0, int64(len(data))); err != nil {
			m.rejectSession(authErrorCode(err), err)
		}
	}
	return nil
}

func (m *TunnelMultiplexer) closeStream(id StreamID) error {
	m.mu.Lock()
	_, ok := m.streams[id]
	if ok {
		delete(m.streams, id)
	}
	closed := m.closed
	ctx := m.ctx
	m.mu.Unlock()

	if !ok || closed {
		return nil
	}
	return m.sendCloseFrame(ctx, id)
}

func (m *TunnelMultiplexer) sendCloseFrame(ctx context.Context, id StreamID) error {
	if err := m.enqueueFrame(ctx, &TunnelFrame{
		StreamID: id,
		Type:     FrameClose,
	}); err != nil {
		return err
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), closeFlushTimeout)
	defer cancel()
	if err := m.flushOutbound(flushCtx); err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, errWriterStopped) {
		m.logger.Debug("tunnel: flush close frame: ", err)
	}
	return nil
}

func (m *TunnelMultiplexer) enqueueFrame(ctx context.Context, f *TunnelFrame) error {
	if ctx == nil {
		return fmt.Errorf("tunnel: multiplexer is not started")
	}

	epoch := m.epoch.Load()
	if epoch == 0 {
		epoch = newSessionEpoch()
		m.epoch.Store(epoch)
	}
	f.Epoch = epoch

	encoded, err := EncodeFrame(f)
	if err != nil {
		return err
	}

	m.mu.Lock()
	closed := m.closed
	writerDone := m.writerDone
	m.mu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}
	if writerDone == nil {
		return fmt.Errorf("tunnel: multiplexer is not started")
	}

	size := int64(len(encoded))
	queued := m.queuedBytes.Add(size)
	if queued > maxOutboundBuf {
		m.queuedBytes.Add(-size)
		return fmt.Errorf("tunnel: outbound buffer overflow (%d > %d)",
			queued-size, int64(maxOutboundBuf))
	}

	cmd := outboundCmd{frame: encoded, epoch: epoch}
	select {
	case m.outbound <- cmd:
		return nil
	case <-writerDone:
		m.queuedBytes.Add(-size)
		return errWriterStopped
	case <-ctx.Done():
		m.queuedBytes.Add(-size)
		return ctx.Err()
	}
}

func (m *TunnelMultiplexer) flushOutbound(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("tunnel: multiplexer is not started")
	}

	m.mu.Lock()
	writerDone := m.writerDone
	m.mu.Unlock()
	if writerDone == nil {
		return errWriterStopped
	}

	select {
	case <-writerDone:
		return errWriterStopped
	default:
	}

	done := make(chan error, 1)
	cmd := outboundCmd{done: done, ctx: ctx}
	select {
	case m.outbound <- cmd:
	case <-writerDone:
		return errWriterStopped
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-done:
		return err
	case <-writerDone:
		return errWriterStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *TunnelMultiplexer) sendBuffer(ctx context.Context, buf *[]byte) error {
	if len(*buf) == 0 {
		return nil
	}

	maxSize := m.transport.MaxMessageSize()
	if maxSize <= 0 {
		maxSize = len(*buf)
	}

	offset := 0
	for offset < len(*buf) {
		end := offset + maxSize
		if end > len(*buf) {
			end = len(*buf)
		}
		chunk := (*buf)[offset:end]

		var err error
		ok := false
		for attempt := 0; attempt < 3; attempt++ {
			err = m.transport.Send(ctx, chunk)
			if err == nil {
				ok = true
				break
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Millisecond):
			}
		}
		if !ok {
			n := copy(*buf, (*buf)[offset:])
			*buf = (*buf)[:n]
			if err != nil &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				m.logger.Warn("tunnel: flush error after retries (", len(chunk), " bytes): ", err)
			}
			return err
		}
		m.ctr.NetworkSent.Add(int64(len(chunk)))
		m.queuedBytes.Add(-int64(len(chunk)))
		offset = end
	}

	*buf = (*buf)[:0]
	return nil
}

func (m *TunnelMultiplexer) drainOutbound(err error) {
	for {
		select {
		case cmd := <-m.outbound:
			if len(cmd.frame) > 0 {
				m.queuedBytes.Add(-int64(len(cmd.frame)))
			}
			if cmd.done != nil {
				select {
				case cmd.done <- err:
				default:
				}
			}
		default:
			return
		}
	}
}

func (m *TunnelMultiplexer) writeLoop(ctx context.Context, done chan struct{}) {
	defer m.wg.Done()
	defer close(done)

	timer := time.NewTimer(m.cfg.FlushInterval)
	defer timer.Stop()

	var buf []byte
	var bufEpoch uint32

	flush := func(sendCtx context.Context) error {
		if len(buf) == 0 {
			return nil
		}
		if bufEpoch != m.epoch.Load() {
			m.queuedBytes.Add(-int64(len(buf)))
			buf = buf[:0]
			bufEpoch = 0
			return nil
		}
		return m.sendBuffer(sendCtx, &buf)
	}

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(m.cfg.FlushInterval)
	}

	for {
		select {
		case cmd := <-m.outbound:
			if cmd.done != nil {
				sendCtx := cmd.ctx
				if sendCtx == nil {
					sendCtx = ctx
				}
				err := flush(sendCtx)
				select {
				case cmd.done <- err:
				default:
				}
				continue
			}

			if cmd.epoch != m.epoch.Load() {
				m.queuedBytes.Add(-int64(len(cmd.frame)))
				continue
			}
			if len(buf) > 0 && bufEpoch != cmd.epoch {
				m.queuedBytes.Add(-int64(len(buf)))
				buf = buf[:0]
			}
			if len(buf) == 0 {
				bufEpoch = cmd.epoch
			}
			buf = append(buf, cmd.frame...)

			if len(buf) >= m.cfg.FlushThreshold {
				if err := flush(ctx); err != nil && ctx.Err() == nil {
					m.logger.Warn("tunnel: flush error: ", err)
				}
			}
			resetTimer()

		case <-timer.C:
			if err := flush(ctx); err != nil && ctx.Err() == nil {
				m.logger.Warn("tunnel: flush error: ", err)
			}
			timer.Reset(m.cfg.FlushInterval)

		case <-ctx.Done():
			if len(buf) > 0 {
				m.queuedBytes.Add(-int64(len(buf)))
				buf = buf[:0]
			}
			m.drainOutbound(ctx.Err())
			return
		}
	}
}

func (m *TunnelMultiplexer) receiveLoop() {
	defer m.wg.Done()
	decoder := NewFrameDecoder()
	ctx := m.ctx

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		data, err := m.transport.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, transport.ErrTransportReset) {
				m.logger.Warn("tunnel: transport reset; closing all streams")
				decoder.Reset()
				m.handleTransportReset()
				continue
			}
			if errors.Is(err, io.EOF) {
				m.logger.Info("tunnel: transport EOF")
				m.closeAllStreams(io.EOF)
				return
			}
			m.logger.Error("tunnel: receive error: ", err)
			m.closeAllStreams(err)
			return
		}

		m.ctr.NetworkRecv.Add(int64(len(data)))

		frames, err := decoder.Feed(data)
		if err != nil {
			m.logger.Warn("tunnel: decode error: ", err)
			decoder.Reset()
			continue
		}

		for _, f := range frames {
			if !m.noteRemoteEpoch(f.Epoch) {
				continue
			}
			m.handleFrame(f)
		}
	}
}

func (m *TunnelMultiplexer) handleTransportReset() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	epoch := nextSessionEpoch(m.epoch.Load())
	m.epoch.Store(epoch)
	m.authorized = false
	m.sessionUser = ""
	m.sessionExit = ""
	m.authRejected = false
	ctx := m.ctx
	m.mu.Unlock()

	dropped := m.queuedBytes.Load()

	m.closeAllStreams(transport.ErrTransportReset)

	if dropped > 0 {
		m.logger.Debug("tunnel: dropping ", dropped, " bytes of stale outbound data")
	}
	if ctx == nil {
		m.logger.Warn("tunnel: cannot announce session reset: multiplexer not started")
		return
	}
	m.sendReset(ctx, epoch)
	if m.hasClientIdentity() {
		m.sendHello(ctx)
	}
}

func (m *TunnelMultiplexer) noteRemoteEpoch(epoch uint32) bool {
	if epoch == 0 {
		m.logger.Warn("tunnel: dropping frame with zero session epoch")
		return false
	}

	m.mu.Lock()
	if !m.remoteEpochSet {
		m.remoteEpoch = epoch
		m.remoteEpochSet = true
		m.mu.Unlock()
		return true
	}
	if epoch == m.remoteEpoch {
		m.mu.Unlock()
		return true
	}
	if _, retired := m.retiredEpochs[epoch]; retired {
		m.mu.Unlock()
		return false
	}
	previous := m.remoteEpoch
	m.retireEpochLocked(previous)
	m.remoteEpoch = epoch
	m.mu.Unlock()

	m.logger.Warn("tunnel: remote session epoch changed (", previous, " -> ",
		epoch, "); closing all streams")
	m.closeAllStreams(transport.ErrTransportReset)
	return true
}

func (m *TunnelMultiplexer) retireEpochLocked(epoch uint32) {
	if epoch == 0 {
		return
	}
	if _, ok := m.retiredEpochs[epoch]; ok {
		return
	}
	m.retiredEpochs[epoch] = struct{}{}
	m.retiredOrder = append(m.retiredOrder, epoch)
	for len(m.retiredOrder) > maxRetiredEpochs {
		oldest := m.retiredOrder[0]
		m.retiredOrder = m.retiredOrder[1:]
		delete(m.retiredEpochs, oldest)
	}
}

func (m *TunnelMultiplexer) handleFrame(f *TunnelFrame) {
	switch f.Type {
	case FrameHello:
		m.handleHello(f)

	case FrameOpen:
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return
		}
		if m.requiresHelloLocked() && !m.authorized {
			ctx := m.ctx
			rejected := m.authRejected
			m.mu.Unlock()
			if !rejected {
				m.sendErrorFrame(ctx, "hello_required",
					errors.New("tunnel: hello frame is required before opening streams"))
			}
			return
		}
		if _, exists := m.streams[f.StreamID]; exists {
			m.mu.Unlock()
			return
		}
		stream := newTunnelStream(m, f.StreamID, f.Metadata)
		m.streams[f.StreamID] = stream
		ctx := m.ctx
		m.mu.Unlock()

		select {
		case m.incoming <- stream:
		case <-ctx.Done():
			m.mu.Lock()
			delete(m.streams, f.StreamID)
			m.mu.Unlock()
			stream.deliverEOF(io.EOF)
		}

	case FrameData:
		m.mu.Lock()
		stream := m.streams[f.StreamID]
		user := m.sessionUser
		authorized := m.authorized
		needHello := m.requiresHelloLocked()
		m.mu.Unlock()
		if needHello && !authorized {
			return
		}
		if stream != nil {
			if needHello {
				if err := m.cfg.Registry.AddTraffic(user, int64(len(f.Data)), 0); err != nil {
					m.rejectSession(authErrorCode(err), err)
					return
				}
			}
			m.ctr.UsefulRecv.Add(int64(len(f.Data)))
			stream.deliverData(f.Data)
		}

	case FrameClose:
		m.mu.Lock()
		stream := m.streams[f.StreamID]
		delete(m.streams, f.StreamID)
		m.mu.Unlock()
		if stream != nil {
			stream.deliverEOF(io.EOF)
		}

	case FrameReset:

		m.logger.Debug("tunnel: session reset frame (epoch ", f.Epoch, ")")

	case FrameError:
		var errInfo ErrorFrame
		if f.Error != nil {
			errInfo = *f.Error
		}
		err := fmt.Errorf("tunnel: session error %q: %s", errInfo.Code, errInfo.Message)
		m.logger.Warn(err)
		m.closeAllStreams(err)
	}
}

func (m *TunnelMultiplexer) handleHello(f *TunnelFrame) {
	registry := m.cfg.Registry
	if registry == nil {
		// Legacy/isolated sessions have no registry but may still carry an
		// identity for cascade routing.
		var hello Hello
		if f.Hello != nil {
			hello = *f.Hello
		}
		m.storeHelloIdentity(hello)
		return
	}

	m.mu.Lock()
	required := m.requiresHelloLocked()
	m.mu.Unlock()
	if !registry.Active() {
		if required {

			m.rejectSession("unknown_user", auth.ErrUserNotFound)
		} else {
			var hello Hello
			if f.Hello != nil {
				hello = *f.Hello
			}
			m.storeHelloIdentity(hello)
		}
		return
	}
	var hello Hello
	if f.Hello != nil {
		hello = *f.Hello
	}

	userID, err := auth.NormalizeUserID(hello.UserID)
	if err != nil {
		m.rejectSession(authErrorCode(err), err)
		return
	}
	exitNodeID := strings.TrimSpace(hello.ExitNodeID)
	if err := registry.Authorize(userID, exitNodeID); err != nil {
		m.rejectSession(authErrorCode(err), err)
		return
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.authorized = true
	m.sessionUser = userID
	m.sessionExit = exitNodeID
	m.authRejected = false
	m.mu.Unlock()

	m.logger.Info("tunnel: authorized user ", userID)
}

func (m *TunnelMultiplexer) storeHelloIdentity(hello Hello) bool {
	userID, err := auth.NormalizeUserID(hello.UserID)
	if err != nil {
		return false
	}
	exitNodeID := strings.TrimSpace(hello.ExitNodeID)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	m.authorized = true
	m.sessionUser = userID
	m.sessionExit = exitNodeID
	m.authRejected = false
	return true
}

func (m *TunnelMultiplexer) closeAllStreams(err error) {
	m.mu.Lock()
	streams := make([]*TunnelStream, 0, len(m.streams))
	for _, s := range m.streams {
		streams = append(streams, s)
	}
	m.streams = make(map[StreamID]*TunnelStream)
	m.mu.Unlock()

	reset := errors.Is(err, transport.ErrTransportReset)
	for _, s := range streams {
		if reset {
			s.abort(err)
		} else {
			s.deliverEOF(err)
		}
	}
}

var _ transport.Telemetry = (*TunnelMultiplexer)(nil)
