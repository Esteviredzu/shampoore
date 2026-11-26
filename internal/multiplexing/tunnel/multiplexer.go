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

	maxUnknownCloses = 64
)

var errWriterStopped = errors.New("tunnel: writer stopped")

var errHelloPending = errors.New("tunnel: hello handshake is still pending")

type Config struct {
	FlushThreshold int
	FlushInterval  time.Duration

	UserID     string
	ExitNodeID string

	Registry *auth.UserRegistry

	OnSessionError func(error)

	OnRemoteEpochChange func(previous, current uint32)

	PingInterval time.Duration
	PingTimeout  time.Duration
}

const (
	defaultPingInterval = 5 * time.Second

	defaultPingTimeout = 45 * time.Second
	maxOpenRetries     = 2

	defaultOpenAckTimeout = 3 * time.Second

	openAckFlushWindows = 8
)

var openAckTimeout = defaultOpenAckTimeout

var errOpenAckTimeout = errors.New("tunnel: open ack timeout")

func (m *TunnelMultiplexer) openAckWindow() time.Duration {
	window := openAckTimeout
	if window != defaultOpenAckTimeout {
		return window
	}
	if adaptive := openAckFlushWindows * m.cfg.FlushInterval; adaptive > window {
		window = adaptive
	}
	return window
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

	unknownClosed map[StreamID]struct{}

	epoch atomic.Uint32

	failed     atomic.Bool
	peerModern atomic.Bool
	lastRecv   atomic.Int64

	lastPingSentNs atomic.Int64
	srttNs         atomic.Int64
	rttvarNs       atomic.Int64
	resetCount     atomic.Uint64

	remoteEpoch    uint32
	remoteEpochSet bool
	retiredEpochs  map[uint32]struct{}
	retiredOrder   []uint32

	authorized   bool
	sessionUser  string
	sessionExit  string
	authRejected bool
	authRequired bool

	helloMu      sync.Mutex
	helloReady   chan struct{}
	helloEpoch   uint32
	helloErr     error
	helloSettled bool

	incoming chan multiplexing.Stream

	outbound    chan outboundCmd
	queuedBytes atomic.Int64
	writerDone  chan struct{}

	ctr transport.Counters

	ctx    context.Context
	cancel context.CancelFunc

	doneMu     sync.Mutex
	done       chan struct{}
	doneClosed bool

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
		done:          make(chan struct{}),
	}
	m.epoch.Store(newSessionEpoch())
	m.helloReady = make(chan struct{})
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
	m.unknownClosed = make(map[StreamID]struct{})
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
	m.resetHelloGateLocked()
	m.mu.Unlock()
	m.failed.Store(false)
	m.peerModern.Store(false)
	m.lastRecv.Store(time.Now().UnixNano())

	m.doneMu.Lock()
	if m.done == nil || m.doneClosed {
		m.done = make(chan struct{})
		m.doneClosed = false
	}
	m.doneMu.Unlock()

	m.started = true

	m.wg.Add(3)
	go m.receiveLoop()
	go m.writeLoop(muxCtx, writerDone)
	go m.heartbeatLoop(muxCtx)

	if m.hasClientIdentity() {
		helloCtx, helloCancel := context.WithTimeout(muxCtx, 2*time.Second)
		m.sendHello(helloCtx)
		helloCancel()
	}
	return nil
}

func (m *TunnelMultiplexer) hasClientIdentity() bool {

	if m.cfg.Registry != nil {
		return false
	}
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

func (m *TunnelMultiplexer) SessionIdentity() (userID, exitNodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessionUser, m.sessionExit
}

func (m *TunnelMultiplexer) resetHelloGateLocked() {
	m.helloReady = make(chan struct{})
	m.helloEpoch = 0
	m.helloErr = nil
	m.helloSettled = false
}

func (m *TunnelMultiplexer) helloSatisfiedLocked() bool {
	if !m.hasClientIdentity() {
		return true
	}
	return m.helloSettled && m.helloErr == nil && m.helloEpoch == m.epoch.Load()
}

func (m *TunnelMultiplexer) settleHello(epoch uint32, err error) {
	m.mu.Lock()
	if m.epoch.Load() != epoch || m.helloSettled {
		m.mu.Unlock()
		return
	}
	m.helloErr = err
	if err == nil {
		m.helloEpoch = epoch
	}
	m.helloSettled = true
	ready := m.helloReady
	m.mu.Unlock()

	if ready != nil {
		close(ready)
	}
}

func (m *TunnelMultiplexer) awaitHello(ctx context.Context) error {
	if !m.hasClientIdentity() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		m.mu.Lock()
		closed := m.closed
		rejected := m.authRejected
		muxCtx := m.ctx
		ready := m.helloReady
		writerDone := m.writerDone
		satisfied := m.helloSatisfiedLocked()
		settled := m.helloSettled
		helloErr := m.helloErr
		m.mu.Unlock()

		if m.failed.Load() {
			return fmt.Errorf("%w: tunnel: session is dead", transport.ErrTransportReset)
		}
		if closed {
			return fmt.Errorf("tunnel: multiplexer is closed")
		}
		if rejected {
			return fmt.Errorf("tunnel: session was rejected")
		}
		if muxCtx == nil {
			return fmt.Errorf("tunnel: multiplexer is not started")
		}
		if satisfied {
			return nil
		}
		if settled && helloErr != nil {
			return fmt.Errorf("tunnel: hello handshake failed: %w", helloErr)
		}

		select {
		case <-ready:
		case <-writerDone:
			return fmt.Errorf("tunnel: multiplexer is closed")
		case <-muxCtx.Done():
			if m.failed.Load() {
				return fmt.Errorf("%w: tunnel: session is dead", transport.ErrTransportReset)
			}
			return muxCtx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *TunnelMultiplexer) enqueueOpenFrame(ctx context.Context, f *TunnelFrame) error {
	m.helloMu.Lock()
	defer m.helloMu.Unlock()

	m.mu.Lock()
	satisfied := m.helloSatisfiedLocked()
	m.mu.Unlock()
	if !satisfied {
		return errHelloPending
	}
	return m.enqueueFrame(ctx, f)
}

func (m *TunnelMultiplexer) sendHello(ctx context.Context) {
	if ctx == nil {
		return
	}
	m.helloMu.Lock()
	defer m.helloMu.Unlock()
	m.sendHelloLocked(ctx)
}

func (m *TunnelMultiplexer) sendHelloLocked(ctx context.Context) {
	if ctx == nil {
		return
	}

	m.mu.Lock()
	epoch := m.epoch.Load()
	satisfied := m.helloSatisfiedLocked()
	m.mu.Unlock()
	if satisfied {
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
		m.settleHello(epoch, err)
		return
	}
	if err := m.flushOutbound(ctx); err != nil {

		if !isBenignFlushError(err) {
			m.logger.Debug("tunnel: flush hello: ", err)
		}
	}
	m.settleHello(epoch, nil)
}

func (m *TunnelMultiplexer) sendControl(ctx context.Context, f *TunnelFrame) {
	if ctx == nil {
		return
	}
	if err := m.enqueueFrame(ctx, f); err != nil {
		m.logger.Debug("tunnel: send control frame: ", err)
	}
}

type HealthSnapshot struct {
	SRTT        time.Duration
	RTTVAR      time.Duration
	QueuedBytes int64
	Resets      uint64
}

func (m *TunnelMultiplexer) Health() HealthSnapshot {
	return HealthSnapshot{
		SRTT:        time.Duration(m.srttNs.Load()),
		RTTVAR:      time.Duration(m.rttvarNs.Load()),
		QueuedBytes: m.queuedBytes.Load(),
		Resets:      m.resetCount.Load(),
	}
}

func (m *TunnelMultiplexer) Done() <-chan struct{} {
	m.doneMu.Lock()
	defer m.doneMu.Unlock()
	return m.done
}

func (m *TunnelMultiplexer) closeDone() {
	m.doneMu.Lock()
	if !m.doneClosed && m.done != nil {
		m.doneClosed = true
		close(m.done)
	}
	m.doneMu.Unlock()
}

func (m *TunnelMultiplexer) recordPingRTT(rtt time.Duration) {
	if rtt < 500*time.Microsecond {
		rtt = 500 * time.Microsecond
	}
	if rtt > 30*time.Second {
		rtt = 30 * time.Second
	}
	rNs := int64(rtt)
	oldSRTT := m.srttNs.Load()
	if oldSRTT <= 0 {
		m.srttNs.Store(rNs)
		m.rttvarNs.Store(rNs / 2)
		return
	}
	diff := oldSRTT - rNs
	if diff < 0 {
		diff = -diff
	}
	oldVar := m.rttvarNs.Load()
	m.rttvarNs.Store((3*oldVar + diff) / 4)
	m.srttNs.Store((7*oldSRTT + rNs) / 8)
}

func (m *TunnelMultiplexer) sendPing(ctx context.Context) {
	m.lastPingSentNs.Store(time.Now().UnixNano())
	m.sendControl(ctx, &TunnelFrame{Type: FramePing})
}

func (m *TunnelMultiplexer) sendPong(ctx context.Context) {
	m.sendControl(ctx, &TunnelFrame{Type: FramePong})
}

func (m *TunnelMultiplexer) sendOpenAck(ctx context.Context, id StreamID) {
	if ctx == nil {
		return
	}
	if err := m.enqueueFrame(ctx, &TunnelFrame{StreamID: id, Type: FrameOpenAck}); err != nil {
		m.logger.Debug("tunnel: send open ack: ", err)
		return
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), m.openAckWindow())
	defer cancel()
	if err := m.flushOutbound(flushCtx); err != nil {
		m.logger.Debug("tunnel: flush open ack: ", err)
	}
}

func (m *TunnelMultiplexer) heartbeatLoop(ctx context.Context) {
	defer m.wg.Done()

	interval := m.cfg.PingInterval
	if interval <= 0 {
		interval = defaultPingInterval
	}
	timeout := m.cfg.PingTimeout
	if timeout <= 0 {
		timeout = defaultPingTimeout
	}

	m.sendPing(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		m.sendPing(ctx)
		if !m.peerModern.Load() {

			continue
		}
		last := m.lastRecv.Load()
		if last == 0 {
			continue
		}
		if time.Since(time.Unix(0, last)) > timeout {
			m.failSession(fmt.Errorf("tunnel: peer silent for %s", timeout))
			return
		}
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
		m.closeDone()
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
	m.closeDone()

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
	if ctx == nil {
		ctx = context.Background()
	}

	for {

		if err := m.awaitHello(ctx); err != nil {
			return nil, err
		}

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

		if m.failed.Load() {
			m.forgetStream(id)
			return nil, fmt.Errorf("%w: tunnel: session is dead", transport.ErrTransportReset)
		}

		frame := &TunnelFrame{
			StreamID: id,
			Type:     FrameOpen,
			Metadata: metadata,
		}
		err := m.enqueueOpenFrame(ctx, frame)
		if errors.Is(err, errHelloPending) {

			m.forgetStream(id)
			continue
		}
		if err != nil {
			m.forgetStream(id)
			return nil, err
		}

		if err := m.flushOutbound(ctx); err != nil {
			m.forgetStream(id)
			stream.abort(err)
			if isBenignFlushError(err) {
				return nil, err
			}
			return nil, fmt.Errorf("tunnel: flush open frame: %w", err)
		}

		if m.peerModern.Load() {
			if err := m.waitOpenAck(ctx, stream, frame); err != nil {
				return nil, err
			}
		}

		if stream.Closed() {
			if err := m.sendCloseFrame(ctx, id); err != nil {
				m.logger.Debug("tunnel: close stream after reset failed: ", err)
			}
		}
		return stream, nil
	}
}

func (m *TunnelMultiplexer) waitOpenAck(ctx context.Context, stream *TunnelStream, frame *TunnelFrame) error {
	timeout := m.openAckWindow()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for attempt := 0; ; attempt++ {
		select {
		case err := <-stream.ackCh:
			if err != nil {
				m.forgetStream(stream.id)
				return err
			}
			return nil
		case <-ctx.Done():
			m.forgetStream(stream.id)
			stream.abort(ctx.Err())
			return ctx.Err()
		case <-timer.C:
			if attempt >= maxOpenRetries {
				m.forgetStream(stream.id)
				stream.abort(errOpenAckTimeout)
				return errOpenAckTimeout
			}
			if err := m.enqueueFrame(ctx, frame); err != nil {
				m.forgetStream(stream.id)
				stream.abort(err)
				return err
			}
			if err := m.flushOutbound(ctx); err != nil {
				if isBenignFlushError(err) {
					m.forgetStream(stream.id)
					stream.abort(err)
					return err
				}
				m.logger.Debug("tunnel: resend open frame: ", err)
			}
			timer.Reset(timeout)
		}
	}
}

func (m *TunnelMultiplexer) forgetStream(id StreamID) {
	m.mu.Lock()
	delete(m.streams, id)
	m.mu.Unlock()
}

func isBenignFlushError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errWriterStopped)
}

func (m *TunnelMultiplexer) AcceptStream(
	ctx context.Context,
) (multiplexing.Stream, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	done := m.Done()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case s, ok := <-m.incoming:
		if !ok {
			return nil, io.EOF
		}
		return s, nil
	case <-done:

		select {
		case s, ok := <-m.incoming:
			if ok {
				return s, nil
			}
			return nil, io.EOF
		default:
		}
		if m.failed.Load() {
			return nil, fmt.Errorf("%w: tunnel: session is dead", transport.ErrTransportReset)
		}
		return nil, io.EOF
	}
}

func (m *TunnelMultiplexer) writeData(id StreamID, data []byte) error {
	m.mu.Lock()
	ctx := m.ctx
	m.mu.Unlock()
	return m.writeDataCtx(ctx, id, data)
}

func (m *TunnelMultiplexer) writeDataCtx(ctx context.Context, id StreamID, data []byte) error {
	m.mu.Lock()
	_, ok := m.streams[id]
	closed := m.closed
	rejected := m.authRejected
	if ctx == nil {
		ctx = m.ctx
	}
	user := m.sessionUser
	authorized := m.authorized
	needHello := m.requiresHelloLocked()
	m.mu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}
	if m.failed.Load() {
		return fmt.Errorf("%w: tunnel: session is dead", transport.ErrTransportReset)
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
			dropped := len(buf)
			m.queuedBytes.Add(-int64(dropped))
			buf = buf[:0]
			bufEpoch = 0
			if dropped > 0 {
				m.logger.Warn("tunnel: dropping ", dropped,
					" bytes of frames from a retired epoch")
			}
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
				m.failSession(io.EOF)
				return
			}
			m.logger.Error("tunnel: receive error: ", err)
			m.failSession(err)
			return
		}

		m.ctr.NetworkRecv.Add(int64(len(data)))
		m.lastRecv.Store(time.Now().UnixNano())

		frames, decErr := decoder.Feed(data)
		if decErr != nil {
			m.logger.Warn("tunnel: decode error: ", decErr)
			decoder.Reset()
		}

		for _, f := range frames {
			if !m.noteRemoteEpoch(f.Epoch) {
				continue
			}
			m.handleFrame(f)
		}
		if decErr != nil {
			continue
		}
	}
}

func (m *TunnelMultiplexer) handleTransportReset() {
	m.resetCount.Add(1)
	m.lastRecv.Store(time.Now().UnixNano())

	m.helloMu.Lock()
	defer m.helloMu.Unlock()

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
	m.resetHelloGateLocked()
	m.mu.Unlock()

	dropped := m.queuedBytes.Load()

	m.closeAllStreams(transport.ErrTransportReset)

	if dropped > 0 {
		m.logger.Debug("tunnel: dropping ", dropped, " bytes of stale outbound data")
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), closeFlushTimeout)
	defer cancel()
	m.sendReset(flushCtx, epoch)
	if m.hasClientIdentity() {
		m.sendHelloLocked(flushCtx)
	} else {
		m.settleHello(epoch, nil)
	}
}

func (m *TunnelMultiplexer) failSession(reason error) {
	if reason == nil {
		reason = transport.ErrTransportReset
	}
	if !m.failed.CompareAndSwap(false, true) {
		return
	}
	m.resetCount.Add(1)
	m.closeAllStreams(reason)

	m.mu.Lock()
	cancel := m.cancel
	notify := m.cfg.OnSessionError
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.closeDone()
	if notify != nil {
		notify(reason)
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
	if notify := m.cfg.OnRemoteEpochChange; notify != nil {
		notify(previous, epoch)
	}
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
		m.ensureAuthorizedForStream()
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
			ctx := m.ctx
			m.mu.Unlock()

			m.sendOpenAck(ctx, f.StreamID)
			return
		}
		stream := newTunnelStream(m, f.StreamID, f.Metadata)
		m.streams[f.StreamID] = stream
		ctx := m.ctx
		m.mu.Unlock()

		m.sendOpenAck(ctx, f.StreamID)

		select {
		case m.incoming <- stream:
		case <-ctx.Done():
			m.mu.Lock()
			delete(m.streams, f.StreamID)
			m.mu.Unlock()
			stream.deliverEOF(io.EOF)
		}

	case FrameOpenAck:
		m.peerModern.Store(true)
		m.mu.Lock()
		stream := m.streams[f.StreamID]
		m.mu.Unlock()
		if stream != nil {
			stream.notifyAck(nil)
		}

	case FramePing:
		m.peerModern.Store(true)
		m.sendPong(m.ctx)

	case FramePong:
		m.peerModern.Store(true)
		if sentNs := m.lastPingSentNs.Swap(0); sentNs > 0 {
			if rtt := time.Since(time.Unix(0, sentNs)); rtt > 0 {
				m.recordPingRTT(rtt)
			}
		}

	case FrameData:
		m.ensureAuthorizedForStream()
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
			return
		}

		m.closeUnknownStream(f.StreamID)

	case FrameClose:
		m.mu.Lock()
		stream := m.streams[f.StreamID]
		delete(m.streams, f.StreamID)
		m.mu.Unlock()
		if stream != nil {
			stream.deliverEOF(io.EOF)
		}

	case FrameReset:
		m.resetCount.Add(1)
		m.logger.Debug("tunnel: session reset frame (epoch ", f.Epoch, "); closing all streams")
		m.closeAllStreams(transport.ErrTransportReset)

	case FrameError:
		var errInfo ErrorFrame
		if f.Error != nil {
			errInfo = *f.Error
		}
		err := fmt.Errorf("tunnel: session error %q: %s", errInfo.Code, errInfo.Message)
		m.logger.Warn(err)
		m.failSession(err)
	}
}

func (m *TunnelMultiplexer) ensureAuthorizedForStream() {
	m.mu.Lock()
	need := !m.closed && !m.authRejected && m.requiresHelloLocked() && !m.authorized
	m.mu.Unlock()
	if !need {
		return
	}
	m.authorizeConfiguredIdentity()
}

func (m *TunnelMultiplexer) authorizeConfiguredIdentity() bool {
	userID, err := auth.NormalizeUserID(m.cfg.UserID)
	if err != nil {
		return false
	}
	exitNodeID := strings.TrimSpace(m.cfg.ExitNodeID)
	if registry := m.cfg.Registry; registry != nil && registry.Active() {
		if err := registry.Authorize(userID, exitNodeID); err != nil {
			return false
		}
	}
	if !m.storeHelloIdentity(Hello{UserID: userID, ExitNodeID: exitNodeID}) {
		return false
	}
	m.logger.Info("tunnel: authorized configured user ", userID, " before hello")
	return true
}

func (m *TunnelMultiplexer) handleHello(f *TunnelFrame) {
	registry := m.cfg.Registry
	if registry == nil {

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

func (m *TunnelMultiplexer) closeUnknownStream(id StreamID) {
	m.mu.Lock()
	if m.unknownClosed == nil {
		m.unknownClosed = make(map[StreamID]struct{})
	}
	if len(m.unknownClosed) >= maxUnknownCloses {
		m.unknownClosed = make(map[StreamID]struct{})
	}
	_, seen := m.unknownClosed[id]
	m.unknownClosed[id] = struct{}{}
	closed := m.closed
	ctx := m.ctx
	m.mu.Unlock()
	if seen || closed || ctx == nil {
		return
	}
	go func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), closeFlushTimeout)
		defer cancel()
		if err := m.sendCloseFrame(closeCtx, id); err != nil {
			m.logger.Debug("tunnel: close unknown stream: ", err)
		}
	}()
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
