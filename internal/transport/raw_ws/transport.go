package raw_ws

import (
	"context"
	"errors"
	"fmt"
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
)

type wsTransport struct {
	cfg    Config
	logger logging.ContextLogger

	connMu sync.Mutex
	conn   *websocket.Conn

	recvCh chan []byte

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
	return &wsTransport{
		cfg:    cfg,
		logger: logger,
		recvCh: make(chan []byte, 256),
	}
}

func (t *wsTransport) Start(ctx context.Context) error {
	if t.started {
		return nil
	}
	t.ctx, t.cancel = context.WithCancel(ctx)
	t.started = true

	t.wg.Add(1)
	go t.runLoop()

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

		select {
		case t.recvCh <- data:
		case <-t.ctx.Done():
			return
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

var _ transport.Transport = (*wsTransport)(nil)
var _ transport.Telemetry = (*wsTransport)(nil)
