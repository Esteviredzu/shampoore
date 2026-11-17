package raw_ws

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Esteviredzu/shampoore/internal/logging"
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
)

var frameMagic = [4]byte{'S', 'W', 'F', 'T'}

const frameFlagBatched byte = 1

type wsTransport struct {
	cfg    Config
	logger logging.ContextLogger

	connMu sync.Mutex
	conn   *websocket.Conn

	recvCh    chan []byte
	sendQueue chan []byte

	batchEnabled   bool
	paddingEnabled bool

	ctx      context.Context
	cancel   context.CancelFunc
	started  bool
	wg       sync.WaitGroup
	stopOnce sync.Once

	bytesSent      atomic.Int64
	bytesDelivered atomic.Int64

	ctr transport.Counters
}

func newTransport(cfg Config, logger logging.ContextLogger) *wsTransport {
	t := &wsTransport{
		cfg:            cfg,
		logger:         logger,
		recvCh:         make(chan []byte, 256),
		batchEnabled:   cfg.Batch,
		paddingEnabled: cfg.Padding,
	}
	if t.batchEnabled {
		t.sendQueue = make(chan []byte, sendQueueCap)
	}
	return t
}

func (t *wsTransport) Start(ctx context.Context) error {
	if t.started {
		return nil
	}
	t.ctx, t.cancel = context.WithCancel(ctx)
	t.started = true

	t.wg.Add(1)
	go t.runLoop()

	if t.batchEnabled {
		t.wg.Add(1)
		go t.sendLoop()
	}

	return nil
}

func (t *wsTransport) runLoop() {
	defer t.wg.Done()

	for {
		select {
		case <-t.ctx.Done():
			return
		default:
		}

		err := t.connectAndServe()
		if err != nil {
			if t.ctx.Err() == nil {
				t.logger.Warn("raw_ws: connection lost, reconnecting in 2s: ", err)
			}
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (t *wsTransport) connectAndServe() error {
	wsURL, err := buildWSURL(t.cfg.ServerURL, t.cfg.Room)
	if err != nil {
		return err
	}

	header := http.Header{}
	if t.cfg.Password != "" {
		header.Set("X-Room-Password", t.cfg.Password)
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.DialContext(t.ctx, wsURL, header)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial: %w (HTTP %d)", err, resp.StatusCode)
		}
		return fmt.Errorf("dial: %w", err)
	}

	conn.SetReadLimit(maxWSPayload)
	_ = conn.SetReadDeadline(time.Now().Add(readIdleTimout))
	conn.SetPongHandler(func(_ string) error {
		return conn.SetReadDeadline(time.Now().Add(readIdleTimout))
	})

	t.connMu.Lock()
	t.conn = conn
	t.connMu.Unlock()

	t.logger.Info("raw_ws: connected (room=", t.cfg.Room, ")")

	errCh := make(chan error, 2)
	go t.readLoop(conn, errCh)
	go t.pingLoop(conn, errCh)

	var runErr error
	select {
	case <-t.ctx.Done():
		runErr = nil
	case err := <-errCh:
		runErr = err
	}

	_ = conn.Close()
	t.connMu.Lock()
	t.conn = nil
	t.connMu.Unlock()

	return runErr
}

func (t *wsTransport) Stop(ctx context.Context) error {
	t.stopOnce.Do(func() {
		if t.cancel != nil {
			t.cancel()
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

	timer := time.NewTimer(enqueueTimeout)
	defer timer.Stop()

	select {
	case t.sendQueue <- cp:
		return nil
	case <-timer.C:
		return errors.New("raw_ws: send queue full")
	case <-ctx.Done():
		return ctx.Err()
	case <-t.ctx.Done():
		return errors.New("raw_ws: transport stopped")
	}
}

func (t *wsTransport) sendLoop() {
	defer t.wg.Done()

	var batch [][]byte
	var batchBytes int

	flush := func() {
		if len(batch) == 0 {
			return
		}
		frame := encodeFrame(batch, true, t.paddingEnabled)
		if err := t.sendRaw(t.ctx, frame); err != nil {
			if t.ctx.Err() == nil {
				t.logger.Warn("raw_ws: flush batch: ", err)
			}
		}
		batch = nil
		batchBytes = 0
	}

	timer := time.NewTimer(t.nextFlushDelay())
	defer timer.Stop()

	for {
		select {
		case <-t.ctx.Done():
			flush()
			return
		case msg := <-t.sendQueue:
			batch = append(batch, msg)
			batchBytes += len(msg)
			if batchBytes >= maxBatchPayload {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(t.nextFlushDelay())
			}
		case <-timer.C:
			flush()
			timer.Reset(t.nextFlushDelay())
		}
	}
}

func (t *wsTransport) nextFlushDelay() time.Duration {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(batchJitterMax)))
	if err != nil {
		return batchBaseDelay
	}
	return batchBaseDelay + time.Duration(n.Int64())
}

func (t *wsTransport) sendRaw(ctx context.Context, data []byte) error {
	for {
		t.connMu.Lock()
		conn := t.conn
		t.connMu.Unlock()

		if conn != nil {
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			err := conn.WriteMessage(websocket.BinaryMessage, data)
			if err == nil {
				t.bytesSent.Add(int64(len(data)))
				t.ctr.NetworkSent.Add(int64(len(data)))
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.ctx.Done():
			return errors.New("raw_ws: transport stopped")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (t *wsTransport) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	case data := <-t.recvCh:
		return data, nil
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

func (t *wsTransport) readLoop(conn *websocket.Conn, errCh chan<- error) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			errCh <- err
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
			case t.recvCh <- m:
			case <-t.ctx.Done():
				return
			}
		}
	}
}

func (t *wsTransport) pingLoop(conn *websocket.Conn, errCh chan<- error) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(pingDeadline)); err != nil {
				errCh <- err
				return
			}
		}
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
	var payload []byte
	if batched {
		for _, m := range msgs {
			var l [4]byte
			binary.BigEndian.PutUint32(l[:], uint32(len(m)))
			payload = append(payload, l[:]...)
			payload = append(payload, m...)
		}
	} else {
		payload = msgs[0]
	}

	var flags byte
	if batched {
		flags |= frameFlagBatched
	}

	var padLen int
	if padding {
		padLen = randomPadLen(len(payload))
	}

	out := make([]byte, frameHeaderSize+len(payload)+padLen)
	copy(out[0:4], frameMagic[:])
	out[4] = frameVersion
	out[5] = flags
	binary.BigEndian.PutUint16(out[6:8], uint16(padLen))
	binary.BigEndian.PutUint32(out[8:12], uint32(len(payload)))
	copy(out[frameHeaderSize:], payload)
	if padLen > 0 {
		_, _ = rand.Read(out[frameHeaderSize+len(payload):])
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
	base, err := rand.Int(rand.Reader, big.NewInt(int64(maxPadExtra)))
	if err != nil {
		return 0
	}
	target := minPadTotal + int(base.Int64())
	if payloadLen < target {
		return target - payloadLen
	}
	extra, err := rand.Int(rand.Reader, big.NewInt(int64(smallPadExtra)))
	if err != nil {
		return 0
	}
	return int(extra.Int64())
}

var _ transport.Transport = (*wsTransport)(nil)
var _ transport.Telemetry = (*wsTransport)(nil)
