package raw_ws

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/net/utlsdial"
	"github.com/Esteviredzu/shampoore/internal/netdial"
	"github.com/Esteviredzu/shampoore/internal/relay"
	"github.com/Esteviredzu/shampoore/internal/transport"
	"github.com/Esteviredzu/shampoore/internal/wsshape"
)

const (
	enqueueTimeout = 50 * time.Millisecond

	stableSessionLifetime = 15 * time.Second

	defaultWarmupTimeout   = 5 * time.Second
	defaultWarmupBodyLimit = 64 << 10
	defaultDialTimeout     = 10 * time.Second

	sessionDialTimeout = 2 * defaultDialTimeout

	defaultRateBurstBytes = 64 << 10

	chromeAcceptLanguage = "en-US,en;q=0.9"
	chromeAccept         = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
)

var (
	shapeDefaults = wsshape.Default()

	maxWSPayload    = shapeDefaults.Frame.MaxMessageBytes
	maxFrameSize    = shapeDefaults.Frame.MaxFrameBytes
	frameHeaderSize = shapeDefaults.Frame.HeaderBytes
	frameVersion    = byte(shapeDefaults.Frame.FrameVersion)

	writeTimeout  = durationMs(shapeDefaults.Pulse.WriteTimeoutMs)
	pingInterval  = durationMs(shapeDefaults.Pulse.PingIntervalMs)
	pingDeadline  = durationMs(shapeDefaults.Pulse.PingTimeoutMs)
	readIdleLimit = durationMs(shapeDefaults.Pulse.ReadIdleMs)

	maxBatchPayload = shapeDefaults.Batch.MaxBytes
	batchBaseDelay  = durationMs(shapeDefaults.Batch.WindowMs)
	batchJitterMax  = durationMs(shapeDefaults.Batch.JitterMs)
	sendQueueCap    = shapeDefaults.Batch.QueueCapacity
	sendAckTimeout  = durationMs(shapeDefaults.Batch.AckTimeoutMs)

	minPadTotal   = shapeDefaults.Padding.MinTotalBytes
	maxPadExtra   = shapeDefaults.Padding.MaxExtraBytes
	smallPadExtra = shapeDefaults.Padding.SmallExtraBytes

	defaultReconnectDelay = durationMs(shapeDefaults.Lifecycle.ReconnectBaseMs)
	reconnectMaxDelay     = durationMs(shapeDefaults.Lifecycle.ReconnectMaxMs)
	rotateIdleWindow      = durationMs(shapeDefaults.Lifecycle.RotateIdleMs)
	defaultWarmupMinDelay = durationMs(shapeDefaults.Lifecycle.WarmupMinMs)
	defaultWarmupMaxDelay = durationMs(shapeDefaults.Lifecycle.WarmupMaxMs)
	defaultRotationMin    = durationMs(shapeDefaults.Lifecycle.RotationMinMs)
	defaultRotationMax    = durationMs(shapeDefaults.Lifecycle.RotationMaxMs)
)

func durationMs(ms int) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

var warmupAssetPaths = []string{"/", "/style.css", "/app.js"}

var frameMagic = [4]byte{'S', 'W', 'F', 'T'}

const frameFlagBatched byte = 1

var (
	errSessionClosed = errors.New("raw_ws: session closed")
	errSendQueueFull = errors.New("raw_ws: send queue full")
)

type outboundMsg struct {
	data []byte
	done chan error
}

func reportDone(msg outboundMsg, err error) {
	if msg.done == nil {
		return
	}
	select {
	case msg.done <- err:
	default:
	}
}

type recvItem struct {
	data []byte
	err  error
}

type wsTransport struct {
	cfg    Config
	logger logging.ContextLogger

	connMu    sync.Mutex
	conn      *websocket.Conn
	active    *wsSession
	sendQueue chan outboundMsg

	flushMu sync.Mutex

	recvCh chan recvItem

	batchEnabled   bool
	paddingEnabled bool

	reconnectDelay time.Duration

	profile        utlsdial.Profile
	warmupClient   *http.Client
	wsDialer       *websocket.Dialer
	warmupPaths    []string
	warmupMinDelay time.Duration
	warmupMaxDelay time.Duration
	rotationMin    time.Duration
	rotationMax    time.Duration
	rotateIdle     time.Duration

	pingInterval    time.Duration
	pingDeadline    time.Duration
	readIdleTimeout time.Duration
	writeTimeout    time.Duration

	shape wsshape.Config
	lim   *rate.Limiter
	burst int

	lastMuxActivity atomic.Int64
	connectedOnce   atomic.Bool
	lastGapReset    atomic.Int64

	pendingSends atomic.Int32

	stateMu sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	wg      sync.WaitGroup

	warmupDone atomic.Bool

	bytesSent      atomic.Int64
	bytesDelivered atomic.Int64

	ctr transport.Counters
}

type wsSession struct {
	conn   *websocket.Conn
	queue  chan outboundMsg
	ctx    context.Context
	cancel context.CancelFunc

	lastRecv atomic.Int64

	sendMu   sync.Mutex
	done     chan struct{}
	doneOnce sync.Once

	errMu sync.Mutex
	err   error
}

func (s *wsSession) markDone() {
	if s == nil {
		return
	}
	s.doneOnce.Do(func() {
		close(s.done)
	})
}

func (s *wsSession) fail(err error) {
	if s == nil {
		return
	}
	if err == nil {
		err = errSessionClosed
	}
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
	s.markDone()
}

func (s *wsSession) sessionErr() error {
	if s == nil {
		return errSessionClosed
	}
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		return errSessionClosed
	}
	return s.err
}

func (s *wsSession) noteRecv() {
	if s != nil {
		s.lastRecv.Store(time.Now().UnixNano())
	}
}

func (s *wsSession) isDone() bool {
	if s == nil || s.done == nil {
		return true
	}
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *wsSession) enqueue(msg outboundMsg, timeout time.Duration) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.isDone() {
		return errSessionClosed
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case s.queue <- msg:
		return nil
	case <-s.done:
		return errSessionClosed
	case <-timer.C:
		return errSendQueueFull
	}
}

func (s *wsSession) queueLen() int {
	if s == nil || s.queue == nil {
		return 0
	}
	return len(s.queue)
}

func (s *wsSession) close() {
	if s == nil {
		return
	}
	s.markDone()
	s.cancel()
	_ = s.conn.Close()
}

func newTransport(cfg Config, logger logging.ContextLogger) *wsTransport {
	shape := wsshape.Normalize(cfg.Shape)

	ctx, cancel := context.WithCancel(context.Background())
	t := &wsTransport{
		cfg:             cfg,
		logger:          logger,
		shape:           shape,
		recvCh:          make(chan recvItem, 256),
		batchEnabled:    cfg.Batch,
		paddingEnabled:  cfg.Padding,
		reconnectDelay:  durationMs(shape.Lifecycle.ReconnectBaseMs),
		profile:         utlsdial.Profile(strings.TrimSpace(cfg.UTLSProfile)),
		warmupPaths:     append([]string(nil), warmupAssetPaths...),
		warmupMinDelay:  durationMs(shape.Lifecycle.WarmupMinMs),
		warmupMaxDelay:  durationMs(shape.Lifecycle.WarmupMaxMs),
		rotationMin:     durationMs(shape.Lifecycle.RotationMinMs),
		rotationMax:     durationMs(shape.Lifecycle.RotationMaxMs),
		rotateIdle:      durationMs(shape.Lifecycle.RotateIdleMs),
		pingInterval:    durationMs(shape.Pulse.PingIntervalMs),
		pingDeadline:    durationMs(shape.Pulse.PingTimeoutMs),
		readIdleTimeout: durationMs(shape.Pulse.ReadIdleMs),
		writeTimeout:    durationMs(shape.Pulse.WriteTimeoutMs),
		ctx:             ctx,
		cancel:          cancel,
	}
	if kbps := shape.Rate.MaxRateKBps; kbps > 0 {
		burst := shape.Rate.BurstBytes
		if max := t.maxMessageSize(); burst < max {
			burst = max
		}
		if burst <= 0 {
			burst = defaultRateBurstBytes
		}
		t.lim = rate.NewLimiter(rate.Limit(kbps*1024), burst)
		t.burst = burst
	}
	t.lastMuxActivity.Store(time.Now().UnixNano())

	t.warmupClient = &http.Client{
		Transport: utlsdial.NewHTTPTransport(t.profile),
		Timeout:   defaultWarmupTimeout,
	}
	t.wsDialer = utlsdial.NewWebSocketDialer(t.profile)
	return t
}

func (t *wsTransport) transportCtx() context.Context {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	ctx := t.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx
}

func (t *wsTransport) Start(ctx context.Context) error {
	t.stateMu.Lock()
	if t.started {
		t.stateMu.Unlock()
		return nil
	}
	if t.cancel != nil {
		t.cancel()
	}
	t.ctx, t.cancel = context.WithCancel(ctx)
	t.started = true
	t.wg.Add(1)
	t.stateMu.Unlock()

	go t.runLoop()

	return nil
}

func (t *wsTransport) runLoop() {
	defer t.wg.Done()

	ctx := t.transportCtx()
	base := t.reconnectDelay
	if base <= 0 {
		base = defaultReconnectDelay
	}
	delay := base
	reconnect := false

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		established, lifetime, err := t.connectAndServe(ctx, reconnect)
		if established {
			reconnect = true
			if lifetime >= stableSessionLifetime {
				delay = base
			}
		}
		if err != nil && ctx.Err() == nil {
			wait := nextBackoffDelay(delay)
			t.logger.Warn("raw_ws: connection lost, reconnecting in ", wait, ": ", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			delay = nextReconnectDelay(delay, base)
		}
	}
}

func nextReconnectDelay(current, base time.Duration) time.Duration {
	if base <= 0 {
		base = defaultReconnectDelay
	}
	if current < base {
		current = base
	}
	next := current * 2
	if next < current || next > reconnectMaxDelay {
		next = reconnectMaxDelay
	}
	return next
}

func nextBackoffDelay(delay time.Duration) time.Duration {
	if delay <= 0 {
		return defaultReconnectDelay
	}

	jitter := time.Duration(rand.Int64N(int64(delay/4) + 1))
	return delay + jitter
}

func (t *wsTransport) connectAndServe(parent context.Context, reconnect bool) (bool, time.Duration, error) {
	if err := validateSecureServerURL(t.cfg.ServerURL, t.cfg.AllowInsecureWS); err != nil {
		return false, 0, err
	}

	sess, err := t.dialSession(parent)
	if err != nil {
		return false, 0, err
	}
	started := time.Now()

	gapSignaled := t.lastGapReset.Swap(0) != 0
	t.attachSession(sess)
	if reconnect && !gapSignaled {
		t.signalReset()
	}

	t.logger.Info("raw_ws: connected (room=", t.cfg.Room, ")")
	t.runSession(sess)

	rotate := time.NewTimer(t.nextRotationDelay())
	defer rotate.Stop()

	sessionStart := started
	hardLimit := t.rotationHardDelay()

	cur := sess
	for {
		select {
		case <-parent.Done():
			t.closeSession(cur)
			return true, time.Since(started), nil
		case <-cur.done:
			err := cur.sessionErr()
			t.closeSession(cur)
			return true, time.Since(started), err
		case <-rotate.C:

			forced := hardLimit > 0 && time.Since(sessionStart) >= hardLimit
			if !forced && (!t.idle() || cur.queueLen() > 0) {
				rotate.Reset(t.nextRotationDelay())
				continue
			}
			next, derr := t.dialSession(parent)
			if derr != nil {
				t.logger.Warn("raw_ws: rotate: ", derr)

				rotate.Reset(t.rotationRetryDelay())
				continue
			}
			if !forced && (!t.idle() || cur.queueLen() > 0) {
				next.close()
				rotate.Reset(t.nextRotationDelay())
				continue
			}

			old := cur
			cur = next
			t.attachSession(next)

			t.closeSession(old)
			t.runSession(next)
			sessionStart = time.Now()
			t.logger.Info("raw_ws: connection rotated")
			rotate.Reset(t.nextRotationDelay())
		}
	}
}

func (t *wsTransport) rotationHardDelay() time.Duration {
	maxD := t.rotationMax
	if maxD <= 0 {
		maxD = defaultRotationMax
	}
	return maxD + 2*t.pingWait()
}

func (t *wsTransport) rotationRetryDelay() time.Duration {
	if d := t.reconnectDelay; d > 0 {
		return d
	}
	return defaultReconnectDelay
}

func (t *wsTransport) dialSession(parent context.Context) (*wsSession, error) {
	room := roomForUser(t.cfg.Room, t.cfg.UserID)
	wsURL, err := buildStreamURL(t.cfg.ServerURL, room)
	if err != nil {
		return nil, err
	}

	dialCtx, cancelDial := context.WithTimeout(parent, sessionDialTimeout)
	defer cancelDial()

	if !t.warmupDone.Swap(true) {
		t.warmup(dialCtx, wsURL)
	}

	dialer := t.newDialer()
	dialer.Subprotocols = relay.AuthProtocols(room, t.cfg.Password, time.Now())
	conn, resp, err := dialer.DialContext(dialCtx, wsURL, nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("dial: %w (HTTP %d)", err, resp.StatusCode)
		}
		return nil, fmt.Errorf("dial: %w", err)
	}

	conn.SetReadLimit(int64(t.shape.Frame.MaxMessageBytes))
	idle := t.pingWait()
	_ = conn.SetReadDeadline(time.Now().Add(idle))

	sessCtx, cancel := context.WithCancel(parent)
	sess := &wsSession{
		conn:   conn,
		ctx:    sessCtx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	conn.SetPongHandler(func(string) error {
		sess.noteRecv()
		return conn.SetReadDeadline(time.Now().Add(idle))
	})
	if t.batchEnabled {
		sess.queue = make(chan outboundMsg, t.queueCapacity())
	}
	return sess, nil
}

func (t *wsTransport) queueCapacity() int {
	if n := t.shape.Batch.QueueCapacity; n > 0 {
		return n
	}
	return sendQueueCap
}

func (t *wsTransport) batchLimit() int {
	if n := t.shape.Batch.MaxBytes; n > 0 {
		return n
	}
	return maxBatchPayload
}

func (t *wsTransport) pingWait() time.Duration {
	if t.readIdleTimeout > 0 {
		return t.readIdleTimeout
	}
	return readIdleLimit
}

func (t *wsTransport) pingEvery() time.Duration {
	if t.pingInterval > 0 {
		return t.pingInterval
	}
	return pingInterval
}

func (t *wsTransport) pingTimeout() time.Duration {
	if t.pingDeadline > 0 {
		return t.pingDeadline
	}
	return pingDeadline
}

func (t *wsTransport) newDialer() *websocket.Dialer {
	if t.wsDialer != nil {
		d := *t.wsDialer
		return &d
	}
	return &websocket.Dialer{
		HandshakeTimeout: defaultDialTimeout,
		NetDialContext: (&net.Dialer{
			Timeout: defaultDialTimeout,
			Control: netdial.Control,
		}).DialContext,
	}
}

func (t *wsTransport) warmup(ctx context.Context, wsURL string) {
	base, err := httpBaseURL(wsURL)
	if err != nil {
		return
	}
	paths := t.warmupPaths
	if len(paths) == 0 {
		return
	}
	count := 2
	if len(paths) >= 3 && rand.IntN(2) == 1 {
		count = 3
	}
	if count > len(paths) {
		count = len(paths)
	}

	for i := 0; i < count; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(t.nextWarmupPause()):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+paths[i], nil)
		if err != nil {
			return
		}
		req.Header.Set("User-Agent", utlsdial.UserAgent(t.profile))
		req.Header.Set("Accept", chromeAccept)
		req.Header.Set("Accept-Language", chromeAcceptLanguage)
		resp, err := t.warmupClient.Do(req)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, defaultWarmupBodyLimit))
		_ = resp.Body.Close()
	}
}

func (t *wsTransport) runSession(sess *wsSession) {
	go t.readLoop(sess.ctx, sess)
	go t.pingLoop(sess.ctx, sess)
	go t.watchSession(sess)
	if sess.queue != nil {
		go t.sendLoop(sess.ctx, sess)
	}
}

func (t *wsTransport) watchSession(sess *wsSession) {
	interval := t.pingEvery()
	if interval <= 0 {
		interval = pingInterval
	}
	limit := t.pingWait()
	if limit <= 0 {
		limit = readIdleLimit
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-sess.ctx.Done():
			return
		case <-sess.done:
			return
		case <-ticker.C:
			last := sess.lastRecv.Load()
			if last == 0 {
				continue
			}
			if time.Since(time.Unix(0, last)) < limit {
				continue
			}
			if t.logger != nil {
				t.logger.Warn("raw_ws: session silent for ", limit, ", dropping the connection")
			}
			sess.fail(fmt.Errorf("%w: raw_ws: session silent for %s", transport.ErrTransportReset, limit))
			sess.close()
			return
		}
	}
}

func (t *wsTransport) attachSession(sess *wsSession) {
	t.connMu.Lock()
	t.conn = sess.conn
	t.active = sess
	t.sendQueue = sess.queue
	t.connMu.Unlock()
	t.connectedOnce.Store(true)
	t.lastGapReset.Store(0)
	t.noteActivity()
}

func (t *wsTransport) isActive(sess *wsSession) bool {
	t.connMu.Lock()
	active := t.active == sess
	t.connMu.Unlock()
	return active
}

func (t *wsTransport) activeSession() *wsSession {
	t.connMu.Lock()
	sess := t.active
	t.connMu.Unlock()
	return sess
}

func (t *wsTransport) sessionGapReset() bool {
	if !t.connectedOnce.Load() || t.activeSession() != nil {
		return false
	}
	window := int64(t.pingWait())
	if window <= 0 {
		window = int64(readIdleLimit)
	}
	now := time.Now().UnixNano()
	last := t.lastGapReset.Load()
	if last != 0 && now-last < window {
		return false
	}
	return t.lastGapReset.CompareAndSwap(last, now)
}

func (t *wsTransport) noteActivity() {
	t.lastMuxActivity.Store(time.Now().UnixNano())
}

func (t *wsTransport) idle() bool {
	last := t.lastMuxActivity.Load()
	if last == 0 {
		return true
	}
	window := t.rotateIdle
	if window <= 0 {
		window = rotateIdleWindow
	}
	return time.Since(time.Unix(0, last)) >= window
}

func (t *wsTransport) closeSession(sess *wsSession) {
	if sess == nil {
		return
	}
	sess.close()
	t.failPending(sess, errSessionClosed)
	t.detachConn(sess.conn)
}

func (t *wsTransport) failPending(sess *wsSession, reason error) {
	if sess == nil || sess.queue == nil {
		return
	}
	for {
		select {
		case msg := <-sess.queue:
			reportDone(msg, reason)
		default:
			return
		}
	}
}

func (t *wsTransport) nextWarmupPause() time.Duration {
	return randomBetween(t.warmupMinDelay, t.warmupMaxDelay, defaultWarmupMinDelay, defaultWarmupMaxDelay)
}

func (t *wsTransport) nextRotationDelay() time.Duration {
	return randomBetween(t.rotationMin, t.rotationMax, defaultRotationMin, defaultRotationMax)
}

func randomBetween(minD, maxD, defMin, defMax time.Duration) time.Duration {
	if minD <= 0 {
		minD = defMin
	}
	if maxD <= 0 {
		maxD = defMax
	}
	if maxD <= minD {
		return minD
	}
	return minD + time.Duration(rand.Int64N(int64(maxD-minD)))
}

func httpBaseURL(wsURL string) (string, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "wss":
		u.Scheme = "https"
	case "ws":
		u.Scheme = "http"
	case "https", "http":
	default:
		return "", fmt.Errorf("raw_ws: unsupported scheme %q", u.Scheme)
	}
	p := u.Path
	if idx := strings.Index(p, "/ws"); idx >= 0 {
		p = p[:idx]
	}
	u.Path = p
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimSuffix(u.String(), "/"), nil
}

func (t *wsTransport) detachConn(conn *websocket.Conn) {
	_ = conn.Close()
	t.connMu.Lock()
	if t.conn == conn {
		t.conn = nil
		t.active = nil
		t.sendQueue = nil
	}
	t.connMu.Unlock()
}

func (t *wsTransport) failConnSession(conn *websocket.Conn, err error) {
	if conn == nil {
		return
	}
	t.connMu.Lock()
	sess := t.active
	if sess != nil && sess.conn != conn {
		sess = nil
	}
	t.connMu.Unlock()
	if sess != nil {
		sess.fail(fmt.Errorf("%w: %v", transport.ErrTransportReset, err))
	}
	t.detachConn(conn)
}

func (t *wsTransport) signalReset() {
	dropped := 0
drain:
	for {
		select {
		case item := <-t.recvCh:
			if item.err == nil {
				dropped++
			}
		default:
			break drain
		}
	}
	if dropped > 0 && t.logger != nil {
		t.logger.Warn("raw_ws: dropped ", dropped, " stale messages of the previous session")
	}

	resetErr := fmt.Errorf("%w: raw_ws: session reconnected", transport.ErrTransportReset)
	select {
	case t.recvCh <- recvItem{err: resetErr}:
	case <-t.transportCtx().Done():
	}
}

func (t *wsTransport) NotifyNetworkChanged() {
	t.connMu.Lock()
	conn := t.conn
	t.connMu.Unlock()
	if conn == nil {
		return
	}
	if t.logger != nil {
		t.logger.Info("raw_ws: network changed, reconnecting")
	}
	if err := conn.Close(); err != nil && t.logger != nil {
		t.logger.Debug("raw_ws: close for reconnect: ", err)
	}
}

func (t *wsTransport) Stop(ctx context.Context) error {
	t.stateMu.Lock()
	if !t.started {
		t.stateMu.Unlock()
		return nil
	}
	t.started = false
	cancel := t.cancel
	t.stateMu.Unlock()

	if cancel != nil {
		cancel()
	}
	t.wg.Wait()
	if t.warmupClient != nil {
		t.warmupClient.CloseIdleConnections()
	}
	t.logger.Info("raw_ws: stopped")
	return nil
}

func (t *wsTransport) Send(ctx context.Context, data []byte) error {
	t.noteActivity()
	if !t.batchEnabled {
		if t.paddingEnabled {
			frame := encodeFrame([][]byte{data}, false, true, t.shape.Padding)
			return t.sendRaw(ctx, frame)
		}
		return t.sendRaw(ctx, data)
	}

	cp := make([]byte, len(data))
	copy(cp, data)

	t.connMu.Lock()
	sess := t.active
	t.connMu.Unlock()
	if sess == nil || sess.queue == nil {
		return fmt.Errorf("%w: raw_ws: no active connection", transport.ErrTransportReset)
	}

	msg := outboundMsg{data: cp, done: make(chan error, 1)}

	t.pendingSends.Add(1)
	defer t.pendingSends.Add(-1)

	if err := sess.enqueue(msg, enqueueTimeout); err != nil {
		if errors.Is(err, errSendQueueFull) {
			return err
		}
		return fmt.Errorf("%w: raw_ws: %v", transport.ErrTransportReset, err)
	}

	ackTimer := time.NewTimer(t.ackTimeout())
	defer ackTimer.Stop()
	select {
	case err := <-msg.done:
		if err != nil {
			return fmt.Errorf("%w: raw_ws: %v", transport.ErrTransportReset, err)
		}
		return nil
	case <-sess.done:
		return fmt.Errorf("%w: raw_ws: connection lost while sending", transport.ErrTransportReset)
	case <-ackTimer.C:
		return fmt.Errorf("%w: raw_ws: write ack timeout", transport.ErrTransportReset)
	case <-ctx.Done():
		return ctx.Err()
	case <-t.transportCtx().Done():
		return errors.New("raw_ws: transport stopped")
	}
}

func (t *wsTransport) ackTimeout() time.Duration {
	if d := durationMs(t.shape.Batch.AckTimeoutMs); d > 0 {
		return d
	}
	return sendAckTimeout
}

func (t *wsTransport) sendLoop(ctx context.Context, sess *wsSession) {
	conn := sess.conn
	queue := sess.queue
	var batch []outboundMsg
	var batchBytes int

	defer func() {
		for _, msg := range batch {
			reportDone(msg, errSessionClosed)
		}
		t.failPending(sess, errSessionClosed)
	}()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		payloads := make([][]byte, len(batch))
		for i := range batch {
			payloads[i] = batch[i].data
		}
		frame := encodeFrame(payloads, true, t.paddingEnabled, t.shape.Padding)
		err := t.sendRawTo(ctx, conn, frame)

		for _, msg := range batch {
			reportDone(msg, err)
		}
		if err != nil {
			if t.logger != nil {
				t.logger.Warn("raw_ws: flush batch failed (", len(batch), " msgs / ",
					batchBytes, " bytes): ", err)
			}
			batch = nil
			batchBytes = 0
			return err
		}
		batch = nil
		batchBytes = 0
		return nil
	}

	timer := time.NewTimer(t.nextFlushDelay())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			if err := flush(); err != nil && t.logger != nil {
				t.logger.Debug("raw_ws: final flush during shutdown: ", err)
			}
			return
		case msg := <-queue:
			batch = append(batch, msg)
			batchBytes += len(msg.data)

			if batchBytes >= t.batchLimit() || t.pendingSends.Load() > 0 {
				if err := flush(); err != nil {
					sess.fail(fmt.Errorf("%w: raw_ws: flush: %v", transport.ErrTransportReset, err))
					return
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(t.nextFlushDelay())
			}
		case <-timer.C:
			if err := flush(); err != nil {
				sess.fail(fmt.Errorf("%w: raw_ws: flush: %v", transport.ErrTransportReset, err))
				return
			}
			timer.Reset(t.nextFlushDelay())
		}
	}
}

func (t *wsTransport) nextFlushDelay() time.Duration {
	window := durationMs(t.shape.Batch.WindowMs)
	if window <= 0 {
		window = batchBaseDelay
	}
	jitter := durationMs(t.shape.Batch.JitterMs)
	if jitter <= 0 {
		return window
	}
	return window + time.Duration(rand.IntN(int(jitter)))
}

func (t *wsTransport) sendRaw(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	t.connMu.Lock()
	conn := t.conn
	t.connMu.Unlock()
	if conn == nil {
		return fmt.Errorf("%w: raw_ws: no active connection", transport.ErrTransportReset)
	}
	return t.sendRawTo(ctx, conn, data)
}

func (t *wsTransport) sendRawTo(ctx context.Context, conn *websocket.Conn, data []byte) error {
	if conn == nil {
		return fmt.Errorf("%w: raw_ws: no active connection", transport.ErrTransportReset)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := t.waitBudget(ctx, len(data)); err != nil {
		return err
	}

	t.flushMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(t.writeTimeout))
	err := conn.WriteMessage(websocket.BinaryMessage, data)
	t.flushMu.Unlock()

	if err != nil {

		t.failConnSession(conn, fmt.Errorf("raw_ws: write: %w", err))
		return fmt.Errorf("raw_ws: write: %w", err)
	}

	t.bytesSent.Add(int64(len(data)))
	t.ctr.NetworkSent.Add(int64(len(data)))
	return nil
}

func (t *wsTransport) waitBudget(ctx context.Context, n int) error {
	if t.lim == nil || n <= 0 {
		return nil
	}
	burst := t.burst
	if burst <= 0 {
		burst = n
	}
	for n > 0 {
		chunk := n
		if chunk > burst {
			chunk = burst
		}
		if err := t.lim.WaitN(ctx, chunk); err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			return fmt.Errorf("%w: raw_ws: rate limit: %v", transport.ErrTransportReset, err)
		}
		n -= chunk
	}
	return nil
}

func (t *wsTransport) Receive(ctx context.Context) ([]byte, error) {

	if t.sessionGapReset() {
		return nil, fmt.Errorf("%w: raw_ws: session lost", transport.ErrTransportReset)
	}
	tctx := t.transportCtx()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-tctx.Done():
		return nil, tctx.Err()
	case item := <-t.recvCh:
		if item.err != nil {
			return nil, item.err
		}
		t.noteActivity()
		return item.data, nil
	}
}

func (t *wsTransport) maxMessageSize() int {
	if n := t.shape.Frame.MaxFrameBytes; n > 0 {
		return n
	}
	return maxFrameSize
}

func (t *wsTransport) MaxMessageSize() int   { return t.maxMessageSize() }
func (t *wsTransport) BytesDelivered() int64 { return t.bytesDelivered.Load() }
func (t *wsTransport) BytesAcked() int64     { return t.bytesSent.Load() }

func (t *wsTransport) WalkLayers() []transport.Layer {
	return []transport.Layer{{
		Kind: "pipe", Type: "raw_ws", Name: "ws", Snapshot: t.ctr.Snapshot(),
	}}
}

func (t *wsTransport) readLoop(ctx context.Context, sess *wsSession) {
	conn := sess.conn
	idle := t.pingWait()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil || sess.isDone() {
				return
			}
			sess.fail(fmt.Errorf("%w: raw_ws: read: %v", transport.ErrTransportReset, err))
			return
		}
		sess.noteRecv()

		_ = conn.SetReadDeadline(time.Now().Add(idle))

		if !t.isActive(sess) {

			sess.fail(fmt.Errorf("%w: raw_ws: session replaced", transport.ErrTransportReset))
			return
		}
		t.bytesDelivered.Add(int64(len(data)))
		t.ctr.NetworkRecv.Add(int64(len(data)))

		msgs, ok := decodeFrame(data)
		if !ok {
			msgs = [][]byte{data}
		}

		for _, m := range msgs {
			select {
			case <-ctx.Done():
				return
			default:
			}
			select {
			case t.recvCh <- recvItem{data: m}:
			case <-ctx.Done():
				return
			case <-sess.done:
				return
			}
		}
	}
}

func (t *wsTransport) pingLoop(ctx context.Context, sess *wsSession) {
	conn := sess.conn
	ticker := time.NewTicker(t.pingEvery())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sess.done:
			return
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(t.pingTimeout())); err != nil {
				sess.fail(fmt.Errorf("%w: raw_ws: ping: %v", transport.ErrTransportReset, err))
				return
			}
		}
	}
}

func validateSecureServerURL(raw string, allowInsecure bool) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("raw_ws: server_url is empty")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("raw_ws: invalid server_url: %q", raw)
	}
	switch strings.ToLower(u.Scheme) {
	case "wss", "https":
		return nil
	case "ws", "http":
		if !allowInsecure {
			return errors.New("raw_ws: insecure ws:// is disabled; use wss:// or set allow_insecure_ws: true")
		}
		return nil
	default:
		return fmt.Errorf("raw_ws: invalid scheme: %s", u.Scheme)
	}
}

func buildWSURL(base, room string) (string, error) {
	base = strings.TrimSpace(base)
	room = strings.TrimSpace(room)
	if base == "" {
		return "", errors.New("raw_ws: server_url is empty")
	}
	if room == "" {
		return "", errors.New("raw_ws: room is empty")
	}

	switch {
	case strings.HasPrefix(base, "https://"):
		base = "wss://" + base[len("https://"):]
	case strings.HasPrefix(base, "http://"):
		base = "ws://" + base[len("http://"):]
	case strings.HasPrefix(base, "wss://"), strings.HasPrefix(base, "ws://"):
	default:
		return "", fmt.Errorf("raw_ws: invalid scheme: %s", base)
	}

	base = strings.TrimSuffix(base, "/")
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("raw_ws: parse server_url: %w", err)
	}

	escaped := url.PathEscape(room)
	if strings.HasSuffix(u.Path, "/ws") || u.Path == "/ws" {
		return base + "/" + escaped, nil
	}
	return base + "/ws/" + escaped, nil
}

func buildStreamURL(base, room string) (string, error) {
	raw, err := buildWSURL(base, room)
	if err != nil {
		return "", err
	}
	idx := strings.Index(raw, "/ws/")
	if idx < 0 {
		return raw, nil
	}
	return raw[:idx] + "/ws/stream?room=" + url.QueryEscape(strings.TrimSpace(room)), nil
}

func encodeFrame(msgs [][]byte, batched, padding bool, pad wsshape.Padding) []byte {
	payloadLen := 0
	if batched {
		for _, m := range msgs {
			payloadLen += 4 + len(m)
		}
	} else if len(msgs) > 0 {
		payloadLen = len(msgs[0])
	}

	var flags byte
	if batched {
		flags |= frameFlagBatched
	}

	var padLen int
	if padding {
		padLen = wsshape.PadLen(pad, payloadLen)
	}

	totalLen := frameHeaderSize + payloadLen + padLen
	out := make([]byte, 0, totalLen)
	out = append(out, frameMagic[:]...)
	out = append(out, frameVersion, flags)

	var tmp [4]byte
	binary.BigEndian.PutUint16(tmp[0:2], uint16(padLen))
	out = append(out, tmp[0:2]...)
	binary.BigEndian.PutUint32(tmp[0:4], uint32(payloadLen))
	out = append(out, tmp[:]...)

	if batched {
		for _, m := range msgs {
			binary.BigEndian.PutUint32(tmp[0:4], uint32(len(m)))
			out = append(out, tmp[:]...)
			out = append(out, m...)
		}
	} else if len(msgs) > 0 {
		out = append(out, msgs[0]...)
	}

	if padLen > 0 {
		start := len(out)
		out = out[:start+padLen]
		fillRandom(out[start:])
	}
	return out
}

func decodeFrame(data []byte) ([][]byte, bool) {
	if len(data) < frameHeaderSize {
		return nil, false
	}
	if data[0] != frameMagic[0] || data[1] != frameMagic[1] ||
		data[2] != frameMagic[2] || data[3] != frameMagic[3] {
		return nil, false
	}
	if data[4] != frameVersion {
		return nil, false
	}
	flags := data[5]
	padLen := int(binary.BigEndian.Uint16(data[6:8]))
	payloadLen := int(binary.BigEndian.Uint32(data[8:12]))
	total := frameHeaderSize + payloadLen + padLen
	if total < frameHeaderSize || len(data) < total {
		return nil, false
	}
	payload := data[frameHeaderSize : frameHeaderSize+payloadLen]

	if flags&frameFlagBatched != 0 {
		var msgs [][]byte
		off := 0
		for off+4 <= len(payload) {
			l := int(binary.BigEndian.Uint32(payload[off : off+4]))
			off += 4
			if l < 0 || off+l > len(payload) {
				return nil, false
			}
			msgs = append(msgs, payload[off:off+l])
			off += l
		}
		if off != len(payload) {
			return nil, false
		}
		return msgs, true
	}

	return [][]byte{payload}, true
}

func randomPadLen(payloadLen int) int {
	return wsshape.PadLen(shapeDefaults.Padding, payloadLen)
}

func fillRandom(b []byte) {
	for len(b) >= 8 {
		binary.LittleEndian.PutUint64(b, rand.Uint64())
		b = b[8:]
	}
	if len(b) > 0 {
		v := rand.Uint64()
		for i := range b {
			b[i] = byte(v)
			v >>= 8
		}
	}
}

var _ transport.Transport = (*wsTransport)(nil)
var _ transport.Telemetry = (*wsTransport)(nil)
