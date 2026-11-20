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

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/net/utlsdial"
	"github.com/Esteviredzu/shampoore/internal/netdial"
	"github.com/Esteviredzu/shampoore/internal/relay"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	maxWSPayload   = 4 * 1024 * 1024
	maxFrameSize   = 16384
	writeTimeout   = 10 * time.Second
	pingInterval   = 30 * time.Second
	pingDeadline   = 5 * time.Second
	readIdleTimout = 90 * time.Second

	frameHeaderSize = 12
	frameVersion    = byte(1)

	maxBatchPayload = 32768
	minPadTotal     = 64
	maxPadExtra     = 320
	smallPadExtra   = 32

	batchBaseDelay = 5 * time.Millisecond
	batchJitterMax = 10 * time.Millisecond

	enqueueTimeout = 50 * time.Millisecond
	sendQueueCap   = 4096

	defaultReconnectDelay = 2 * time.Second

	defaultWarmupMinDelay  = 60 * time.Millisecond
	defaultWarmupMaxDelay  = 250 * time.Millisecond
	defaultWarmupTimeout   = 5 * time.Second
	defaultWarmupBodyLimit = 64 << 10
	defaultRotationMin     = 15 * time.Minute
	defaultRotationMax     = 35 * time.Minute
	defaultRotateGraceMin  = 2 * time.Second
	defaultRotateGraceMax  = 3 * time.Second
	defaultDialTimeout     = 10 * time.Second

	chromeAcceptLanguage = "en-US,en;q=0.9"
	chromeAccept         = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
)

var warmupAssetPaths = []string{"/", "/style.css", "/app.js"}

var frameMagic = [4]byte{'S', 'W', 'F', 'T'}

const frameFlagBatched byte = 1

type recvItem struct {
	data []byte
	err  error
}

type wsTransport struct {
	cfg    Config
	logger logging.ContextLogger

	connMu    sync.Mutex
	conn      *websocket.Conn
	sendQueue chan []byte

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
	graceMin       time.Duration
	graceMax       time.Duration

	stateMu sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	wg      sync.WaitGroup

	stopOnce sync.Once

	bytesSent      atomic.Int64
	bytesDelivered atomic.Int64

	ctr transport.Counters
}

type wsSession struct {
	conn   *websocket.Conn
	queue  chan []byte
	ctx    context.Context
	cancel context.CancelFunc
	errCh  chan error
}

func (s *wsSession) close() {
	if s == nil {
		return
	}
	s.cancel()
	_ = s.conn.Close()
}

func newTransport(cfg Config, logger logging.ContextLogger) *wsTransport {

	ctx, cancel := context.WithCancel(context.Background())
	t := &wsTransport{
		cfg:            cfg,
		logger:         logger,
		recvCh:         make(chan recvItem, 256),
		batchEnabled:   cfg.Batch,
		paddingEnabled: cfg.Padding,
		reconnectDelay: defaultReconnectDelay,
		profile:        utlsdial.DefaultProfile,
		warmupPaths:    append([]string(nil), warmupAssetPaths...),
		warmupMinDelay: defaultWarmupMinDelay,
		warmupMaxDelay: defaultWarmupMaxDelay,
		rotationMin:    defaultRotationMin,
		rotationMax:    defaultRotationMax,
		graceMin:       defaultRotateGraceMin,
		graceMax:       defaultRotateGraceMax,
		ctx:            ctx,
		cancel:         cancel,
	}
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
	reconnect := false

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		established, err := t.connectAndServe(ctx, reconnect)
		if established {
			reconnect = true
		}
		if err != nil && ctx.Err() == nil {
			delay := t.reconnectDelay
			if delay <= 0 {
				delay = defaultReconnectDelay
			}
			t.logger.Warn("raw_ws: connection lost, reconnecting in ", delay, ": ", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}
}

func (t *wsTransport) connectAndServe(parent context.Context, reconnect bool) (bool, error) {
	if err := validateSecureServerURL(t.cfg.ServerURL, t.cfg.AllowInsecureWS); err != nil {
		return false, err
	}

	sess, err := t.dialSession(parent)
	if err != nil {
		return false, err
	}

	t.attachSession(sess)
	if reconnect {
		t.signalReset()
	}

	t.logger.Info("raw_ws: connected (room=", t.cfg.Room, ")")
	t.runSession(sess)

	rotate := time.NewTimer(t.nextRotationDelay())
	defer rotate.Stop()

	cur := sess
	for {
		select {
		case <-parent.Done():
			t.closeSession(cur, 0)
			return true, nil
		case err := <-cur.errCh:
			t.closeSession(cur, 0)
			return true, err
		case <-rotate.C:
			next, derr := t.dialSession(parent)
			if derr != nil {
				t.logger.Warn("raw_ws: rotate: ", derr)
				rotate.Reset(t.nextRotationDelay())
				continue
			}

			t.attachSession(next)
			t.runSession(next)
			old := cur
			cur = next
			t.logger.Info("raw_ws: connection rotated")
			go t.closeSession(old, t.nextGrace())
			rotate.Reset(t.nextRotationDelay())
		}
	}
}

func (t *wsTransport) dialSession(parent context.Context) (*wsSession, error) {
	room := roomForUser(t.cfg.Room, t.cfg.UserID)
	wsURL, err := buildStreamURL(t.cfg.ServerURL, room)
	if err != nil {
		return nil, err
	}

	t.warmup(parent, wsURL)

	dialer := t.newDialer()
	dialer.Subprotocols = relay.AuthProtocols(room, t.cfg.Password, time.Now())
	conn, resp, err := dialer.DialContext(parent, wsURL, nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("dial: %w (HTTP %d)", err, resp.StatusCode)
		}
		return nil, fmt.Errorf("dial: %w", err)
	}

	conn.SetReadLimit(maxWSPayload)
	_ = conn.SetReadDeadline(time.Now().Add(readIdleTimout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readIdleTimout))
	})

	sessCtx, cancel := context.WithCancel(parent)
	sess := &wsSession{
		conn:   conn,
		ctx:    sessCtx,
		cancel: cancel,
		errCh:  make(chan error, 4),
	}
	if t.batchEnabled {
		sess.queue = make(chan []byte, sendQueueCap)
	}
	return sess, nil
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
	go t.readLoop(sess.ctx, sess.conn, sess.errCh)
	go t.pingLoop(sess.ctx, sess.conn, sess.errCh)
	if sess.queue != nil {
		go t.sendLoop(sess.ctx, sess.conn, sess.queue, sess.errCh)
	}
}

func (t *wsTransport) attachSession(sess *wsSession) {
	t.connMu.Lock()
	t.conn = sess.conn
	t.sendQueue = sess.queue
	t.connMu.Unlock()
}

func (t *wsTransport) closeSession(sess *wsSession, grace time.Duration) {
	if sess == nil {
		return
	}
	if grace > 0 {
		select {
		case <-sess.ctx.Done():
		case <-time.After(grace):
		}
	}
	sess.close()
	t.detachConn(sess.conn)
}

func (t *wsTransport) nextWarmupPause() time.Duration {
	return randomBetween(t.warmupMinDelay, t.warmupMaxDelay, defaultWarmupMinDelay, defaultWarmupMaxDelay)
}

func (t *wsTransport) nextRotationDelay() time.Duration {
	return randomBetween(t.rotationMin, t.rotationMax, defaultRotationMin, defaultRotationMax)
}

func (t *wsTransport) nextGrace() time.Duration {
	return randomBetween(t.graceMin, t.graceMax, defaultRotateGraceMin, defaultRotateGraceMax)
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
		t.sendQueue = nil
	}
	t.connMu.Unlock()
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
	t.stopOnce.Do(func() {
		t.stateMu.Lock()
		cancel := t.cancel
		t.stateMu.Unlock()
		if cancel != nil {
			cancel()
		}
	})
	t.wg.Wait()
	if t.warmupClient != nil {
		t.warmupClient.CloseIdleConnections()
	}
	t.logger.Info("raw_ws: stopped")
	return nil
}

func (t *wsTransport) Send(ctx context.Context, data []byte) error {
	if !t.batchEnabled {
		if t.paddingEnabled {
			frame := encodeFrame([][]byte{data}, false, true)
			return t.sendRaw(ctx, frame)
		}
		return t.sendRaw(ctx, data)
	}

	cp := make([]byte, len(data))
	copy(cp, data)

	t.connMu.Lock()
	queue := t.sendQueue
	t.connMu.Unlock()
	if queue == nil {

		return fmt.Errorf("%w: raw_ws: no active connection", transport.ErrTransportReset)
	}

	timer := time.NewTimer(enqueueTimeout)
	defer timer.Stop()

	select {
	case queue <- cp:
	case <-timer.C:
		return errors.New("raw_ws: send queue full")
	case <-ctx.Done():
		return ctx.Err()
	case <-t.transportCtx().Done():
		return errors.New("raw_ws: transport stopped")
	}

	t.connMu.Lock()
	live := t.sendQueue == queue
	t.connMu.Unlock()
	if !live {
		return fmt.Errorf("%w: raw_ws: connection lost while enqueuing", transport.ErrTransportReset)
	}
	return nil
}

func reportErr(errCh chan<- error, err error) {
	if err == nil {
		return
	}
	select {
	case errCh <- err:
	default:
	}
}

func (t *wsTransport) sendLoop(ctx context.Context, conn *websocket.Conn, queue <-chan []byte, errCh chan<- error) {
	var batch [][]byte
	var batchBytes int

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		frame := encodeFrame(batch, true, t.paddingEnabled)
		if err := t.sendRawTo(conn, frame); err != nil {

			if t.logger != nil {
				t.logger.Warn("raw_ws: flush batch failed (", len(batch), " msgs / ",
					batchBytes, " bytes): ", err)
			}
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
			batchBytes += len(msg)
			if batchBytes >= maxBatchPayload {
				if err := flush(); err != nil {
					reportErr(errCh, err)
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
				reportErr(errCh, err)
				return
			}
			timer.Reset(t.nextFlushDelay())
		}
	}
}

func (t *wsTransport) nextFlushDelay() time.Duration {
	return batchBaseDelay + time.Duration(rand.IntN(int(batchJitterMax)))
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
	return t.sendRawTo(conn, data)
}

func (t *wsTransport) sendRawTo(conn *websocket.Conn, data []byte) error {
	if conn == nil {
		return fmt.Errorf("%w: raw_ws: no active connection", transport.ErrTransportReset)
	}

	t.flushMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	err := conn.WriteMessage(websocket.BinaryMessage, data)
	t.flushMu.Unlock()

	if err != nil {

		t.detachConn(conn)
		return fmt.Errorf("raw_ws: write: %w", err)
	}

	t.bytesSent.Add(int64(len(data)))
	t.ctr.NetworkSent.Add(int64(len(data)))
	return nil
}

func (t *wsTransport) Receive(ctx context.Context) ([]byte, error) {
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
		return item.data, nil
	}
}

func (t *wsTransport) MaxMessageSize() int   { return maxFrameSize }
func (t *wsTransport) BytesDelivered() int64 { return t.bytesDelivered.Load() }
func (t *wsTransport) BytesAcked() int64     { return t.bytesSent.Load() }

func (t *wsTransport) WalkLayers() []transport.Layer {
	return []transport.Layer{{
		Kind: "pipe", Type: "raw_ws", Name: "ws", Snapshot: t.ctr.Snapshot(),
	}}
}

func (t *wsTransport) readLoop(ctx context.Context, conn *websocket.Conn, errCh chan<- error) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			reportErr(errCh, err)
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
			}
		}
	}
}

func (t *wsTransport) pingLoop(ctx context.Context, conn *websocket.Conn, errCh chan<- error) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(pingDeadline)); err != nil {
				reportErr(errCh, err)
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

func encodeFrame(msgs [][]byte, batched, padding bool) []byte {
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
		padLen = randomPadLen(payloadLen)
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
	target := minPadTotal + rand.IntN(maxPadExtra)
	if payloadLen < target {
		return target - payloadLen
	}
	return rand.IntN(smallPadExtra)
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
