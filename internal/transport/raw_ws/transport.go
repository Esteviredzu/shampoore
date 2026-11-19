package raw_ws

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
	"github.com/Esteviredzu/shampoore/internal/netdial"
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
)

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

func newTransport(cfg Config, logger logging.ContextLogger) *wsTransport {

	ctx, cancel := context.WithCancel(context.Background())
	return &wsTransport{
		cfg:            cfg,
		logger:         logger,
		recvCh:         make(chan recvItem, 256),
		batchEnabled:   cfg.Batch,
		paddingEnabled: cfg.Padding,
		reconnectDelay: defaultReconnectDelay,
		ctx:            ctx,
		cancel:         cancel,
	}
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
	wsURL, err := buildWSURL(t.cfg.ServerURL, roomForUser(t.cfg.Room, t.cfg.UserID))
	if err != nil {
		return false, err
	}

	header := http.Header{}
	if t.cfg.Password != "" {
		header.Set("X-Room-Password", t.cfg.Password)
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		NetDialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
			Control: netdial.Control,
		}).DialContext,
	}
	conn, resp, err := dialer.DialContext(parent, wsURL, header)
	if err != nil {
		if resp != nil {
			return false, fmt.Errorf("dial: %w (HTTP %d)", err, resp.StatusCode)
		}
		return false, fmt.Errorf("dial: %w", err)
	}

	conn.SetReadLimit(maxWSPayload)
	_ = conn.SetReadDeadline(time.Now().Add(readIdleTimout))
	conn.SetPongHandler(func(_ string) error {
		return conn.SetReadDeadline(time.Now().Add(readIdleTimout))
	})

	var queue chan []byte
	if t.batchEnabled {
		queue = make(chan []byte, sendQueueCap)
	}

	t.connMu.Lock()
	t.conn = conn
	t.sendQueue = queue
	t.connMu.Unlock()

	if reconnect {

		t.signalReset()
	}

	t.logger.Info("raw_ws: connected (room=", t.cfg.Room, ")")

	connCtx, connCancel := context.WithCancel(parent)

	var connWg sync.WaitGroup
	errCh := make(chan error, 3)

	connWg.Add(1)
	go func() {
		defer connWg.Done()
		t.readLoop(connCtx, conn, errCh)
	}()
	connWg.Add(1)
	go func() {
		defer connWg.Done()
		t.pingLoop(connCtx, conn, errCh)
	}()
	if queue != nil {
		connWg.Add(1)
		go func() {
			defer connWg.Done()
			t.sendLoop(connCtx, queue, errCh)
		}()
	}

	var runErr error
	select {
	case <-parent.Done():
		runErr = nil
	case err := <-errCh:
		runErr = err
	}

	connCancel()
	t.detachConn(conn)
	connWg.Wait()

	return true, runErr
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

func (t *wsTransport) sendLoop(ctx context.Context, queue <-chan []byte, errCh chan<- error) {
	var batch [][]byte
	var batchBytes int

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		frame := encodeFrame(batch, true, t.paddingEnabled)
		if err := t.sendRaw(ctx, frame); err != nil {

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
