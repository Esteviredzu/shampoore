package httpstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mrand "math/rand/v2"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/net/utlsdial"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	sendAckTimeout   = 20 * time.Second
	httpRequestLimit = 25 * time.Second
	recvQueueCap     = 1024
	sendQueueCap     = 4096
)

var errPeerClosed = errors.New("httpstream: peer closed session")

var requestTimeout = httpRequestLimit

var downlinkAckMinInterval = 100 * time.Millisecond

type outboundMsg struct {
	data []byte
	done chan error
}

type recvItem struct {
	data []byte
	err  error
}

type Client struct {
	profile MimicProfile
	cfg     ClientConfig
	logger  logging.ContextLogger
	info    SessionInfo
	sid     [16]byte

	httpClient *http.Client

	stateMu sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	wg      sync.WaitGroup

	sendCh chan outboundMsg
	recvCh chan recvItem

	epoch           atomic.Uint32
	serverGen       atomic.Uint32
	genBound        atomic.Bool
	genResetPending atomic.Bool
	downlinkNext    atomic.Uint64
	downlinkCursor  atomic.Uint64

	pendingSends atomic.Int32
	resetPending atomic.Bool
	wake         chan struct{}

	ackWake   chan struct{}
	lastAckAt atomic.Int64

	dlMu     sync.Mutex
	dlCancel context.CancelFunc

	seqMu         sync.Mutex
	nextUplinkSeq uint64

	bytesDelivered atomic.Int64
	bytesAcked     atomic.Int64
	ctr            transport.Counters
}

func NewClient(profile MimicProfile, cfg ClientConfig) (*Client, error) {
	profile = profile.Normalize()
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	cfg, err := normalizeClientConfig(profile, cfg)
	if err != nil {
		return nil, err
	}
	sid, err := parseSessionID(cfg.SessionID)
	if err != nil {
		return nil, err
	}
	info := SessionInfo{
		SessionID:  cfg.SessionID,
		Namespace:  cfg.Namespace,
		UserID:     cfg.UserID,
		ExitNodeID: cfg.ExitNodeID,
		Role:       cfg.Role,
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		tr := utlsdial.NewHTTPTransport(utlsdial.Profile(profile.UTLSProfile))
		httpClient = &http.Client{Transport: tr}
	}

	if httpClient.CheckRedirect == nil {
		httpClient = &http.Client{
			Transport: httpClient.Transport,
			Jar:       httpClient.Jar,
			Timeout:   httpClient.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}

	c := &Client{
		profile:    profile,
		cfg:        cfg,
		logger:     cfg.Logger,
		info:       info,
		sid:        sid,
		httpClient: httpClient,
		sendCh:     make(chan outboundMsg, sendQueueCap),
		recvCh:     make(chan recvItem, recvQueueCap),
		wake:       make(chan struct{}, 1),
		ackWake:    make(chan struct{}, 1),
	}
	c.epoch.Store(1)
	c.downlinkNext.Store(1)
	c.nextUplinkSeq = 1
	return c, nil
}

func (c *Client) SessionInfo() SessionInfo { return c.info }

func (c *Client) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.stateMu.Lock()
	if c.started {
		c.stateMu.Unlock()
		return nil
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.started = true
	c.resetPending.Store(false)
	runCtx := c.ctx
	c.stateMu.Unlock()

	c.wg.Add(1)
	go c.runDownlink(runCtx, c.ctx)
	c.wg.Add(1)
	go c.sendLoop(runCtx, c.ctx)
	c.wg.Add(1)
	go c.ackLoop(runCtx, c.ctx)
	for _, decoy := range c.profile.DecoyRequests {
		spec := decoy
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.runDecoy(runCtx, spec)
		}()
	}
	return nil
}

func (c *Client) Stop(ctx context.Context) error {
	c.stateMu.Lock()
	if !c.started {
		c.stateMu.Unlock()
		return nil
	}
	c.started = false
	cancel := c.cancel
	c.cancel = nil
	c.stateMu.Unlock()

	if cancel != nil {
		cancel()
	}
	c.cancelDownlink()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	if ctx == nil {
		<-done
	} else {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.httpClient.CloseIdleConnections()
	return nil
}

func (c *Client) Close() error { return c.Stop(context.Background()) }

func (c *Client) transportContext() context.Context {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

func (c *Client) Send(ctx context.Context, data []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(data) == 0 {
		return nil
	}
	if len(data) > c.MaxMessageSize() {
		return fmt.Errorf("httpstream: message too large (%d > %d)", len(data), c.MaxMessageSize())
	}
	c.stateMu.Lock()
	started := c.started
	c.stateMu.Unlock()
	if !started {
		return fmt.Errorf("%w: httpstream: client is not started", transport.ErrTransportReset)
	}
	cp := append([]byte(nil), data...)
	msg := outboundMsg{data: cp, done: make(chan error, 1)}

	c.pendingSends.Add(1)
	defer c.pendingSends.Add(-1)
	if c.profile.ImmediateOnUplink {
		c.noteWake()
	}

	tctx := c.transportContext()
	select {
	case c.sendCh <- msg:
	case <-ctx.Done():
		return ctx.Err()
	case <-tctx.Done():
		return fmt.Errorf("%w: httpstream: transport stopped", transport.ErrTransportReset)
	}

	timer := time.NewTimer(sendAckTimeout)
	defer timer.Stop()
	select {
	case err := <-msg.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-tctx.Done():
		return fmt.Errorf("%w: httpstream: transport stopped", transport.ErrTransportReset)
	case <-timer.C:
		return fmt.Errorf("%w: httpstream: uplink ack timeout", transport.ErrTransportReset)
	}
}

func (c *Client) Receive(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	tctx := c.transportContext()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-tctx.Done():
		return nil, tctx.Err()
	case item := <-c.recvCh:
		if item.err != nil {
			if errors.Is(item.err, transport.ErrTransportReset) {

				c.clearReset()
			}
			return nil, item.err
		}
		c.bytesDelivered.Add(int64(len(item.data)))
		return item.data, nil
	}
}

func (c *Client) MaxMessageSize() int   { return c.cfg.MaxMessageSize }
func (c *Client) BytesDelivered() int64 { return c.bytesDelivered.Load() }
func (c *Client) BytesAcked() int64     { return c.bytesAcked.Load() }

func (c *Client) NotifyNetworkChanged() {
	c.cancelDownlink()
	c.noteWake()
	if c.logger != nil {
		c.logger.Info("httpstream: network changed, reconnecting downlink")
	}
}

func (c *Client) WalkLayers() []transport.Layer {
	return []transport.Layer{{
		Kind: "pipe", Type: "httpstream", Name: "httpstream", Snapshot: c.ctr.Snapshot(),
	}}
}

func (c *Client) nextSequence() (uint32, uint64) {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	seq := c.nextUplinkSeq
	c.nextUplinkSeq++
	return c.epoch.Load(), seq
}

func (c *Client) resetUplinkSequence() {
	c.seqMu.Lock()
	c.epoch.Add(1)
	c.nextUplinkSeq = 1
	c.seqMu.Unlock()
}

func (c *Client) signalReset(reason error) {
	if reason == nil {
		reason = errors.New("connection lost")
	}
	if !c.resetPending.CompareAndSwap(false, true) {
		return
	}
	dropped := 0
drain:
	for {
		select {
		case item := <-c.recvCh:
			if item.err != nil {
				continue
			}
			dropped++
		default:
			break drain
		}
	}
	if dropped > 0 && c.logger != nil {
		c.logger.Warn("httpstream: dropped ", dropped, " stale messages of the previous epoch")
	}
	resetErr := fmt.Errorf("%w: httpstream: %v", transport.ErrTransportReset, reason)
	select {
	case c.recvCh <- recvItem{err: resetErr}:
	case <-c.transportContext().Done():
	}
}

func (c *Client) clearReset() { c.resetPending.Store(false) }

func (c *Client) noteWake() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Client) cancelDownlink() {
	c.dlMu.Lock()
	cancel := c.dlCancel
	c.dlMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Client) setDownlinkCancel(cancel context.CancelFunc) {
	c.dlMu.Lock()
	c.dlCancel = cancel
	c.dlMu.Unlock()
}

func (c *Client) clearDownlinkCancel() {
	c.dlMu.Lock()
	c.dlCancel = nil
	c.dlMu.Unlock()
}

func resolveOutbound(msg outboundMsg, err error) {
	if msg.done == nil {
		return
	}
	select {
	case msg.done <- err:
	default:
	}
}

func (c *Client) sendLoop(ctx context.Context, _ context.Context) {
	defer c.wg.Done()

	var batch []outboundMsg
	var batchBytes int
	window := c.profile.UplinkBatchWindow
	timer := time.NewTimer(window)
	if !timer.Stop() {
		<-timer.C
	}
	running := false

	stopTimer := func() {
		if !running {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		running = false
	}

	maxInflight := c.profile.MaxUplinkInflight
	if maxInflight <= 0 {
		maxInflight = 1
	}
	slots := make(chan struct{}, maxInflight)
	var sendWG sync.WaitGroup

	acquire := func() bool {
		select {
		case slots <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	fail := func(msgs []outboundMsg) {
		err := fmt.Errorf("%w: httpstream: transport stopped", transport.ErrTransportReset)
		for _, msg := range msgs {
			resolveOutbound(msg, err)
		}
	}

	flush := func() {
		if len(batch) == 0 {
			stopTimer()
			return
		}
		stopTimer()

		if !acquire() {
			fail(batch)
			batch = nil
			batchBytes = 0
			return
		}
		messages := make([][]byte, len(batch))
		useful := 0
		for i := range batch {
			messages[i] = batch[i].data
			useful += len(batch[i].data)
		}
		epoch, seq := c.nextSequence()
		payloadSize := 0
		for _, msg := range messages {
			payloadSize += 4 + len(msg)
		}
		pad := c.profile.Padding.PadLen(frameHeaderSize + payloadSize)
		frame := encodeFrame(frameData, c.sid, epoch, seq,
			c.downlinkCursor.Load(), messages, pad)
		pending := batch
		batch = nil
		batchBytes = 0

		sendWG.Add(1)
		go func() {
			defer sendWG.Done()
			defer func() { <-slots }()
			err := c.sendUplinkFrame(ctx, seq, frame, useful, len(messages))
			for _, msg := range pending {
				resolveOutbound(msg, err)
			}
			if err != nil {
				c.signalReset(err)
			}
		}()
	}

	defer func() {
		stopTimer()
		sendWG.Wait()
		fail(batch)
		batch = nil
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			running = false
			flush()
		case msg := <-c.sendCh:
			batch = append(batch, msg)
			batchBytes += len(msg.data)
			if len(batch) == 1 && !running {
				timer.Reset(window)
				running = true
			}
			if batchBytes >= c.profile.MaxUplinkBatchBytes {
				flush()
			}
		}
	}
}

func (c *Client) sendUplinkFrame(ctx context.Context, seq uint64, frame []byte, useful, messages int) error {
	spec, path := c.profile.nextSpec(c.profile.UplinkSpecs)
	method := strings.TrimSpace(spec.Method)
	if method == "" {
		method = http.MethodPost
	}
	target, err := c.buildURL(spec, path, c.downlinkCursor.Load(), seq, false)
	if err != nil {
		return fmt.Errorf("%w: httpstream: %v", transport.ErrTransportReset, err)
	}
	contentType, body, err := spec.WrapUplinkBody(frame)
	if err != nil {
		return fmt.Errorf("%w: httpstream: %v", transport.ErrTransportReset, err)
	}
	token, err := signSessionToken(c.cfg.Secret, c.info, time.Now(), c.cfg.TokenTTL)
	if err != nil {
		return fmt.Errorf("%w: httpstream: %v", transport.ErrTransportReset, err)
	}

	attempts := c.cfg.MaxUplinkRetries + 1
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		reqCtx, cancel := context.WithTimeout(ctx, httpRequestLimit)
		req, err := http.NewRequestWithContext(reqCtx, method, target, bytes.NewReader(body))
		if err != nil {
			cancel()
			return fmt.Errorf("%w: httpstream: %v", transport.ErrTransportReset, err)
		}
		for key, value := range spec.Headers {
			req.Header.Set(key, ExpandTemplate(value, PlaceholderValues{Seq: seq}))
		}
		if req.Header.Get("Accept") == "" {
			req.Header.Set("Accept", "*/*")
		}
		if req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", utlsdial.UserAgent(utlsdial.Profile(c.profile.UTLSProfile)))
		}
		if contentType != "" && (spec.ContentType != "" || req.Header.Get("Content-Type") == "") {
			req.Header.Set("Content-Type", contentType)
		}
		if err := c.profile.applySessionToken(req, token); err != nil {
			cancel()
			return fmt.Errorf("%w: httpstream: %v", transport.ErrTransportReset, err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !c.waitRetry(ctx, attempt) {
				return ctx.Err()
			}
			continue
		}
		ack := strings.TrimSpace(resp.Header.Get(c.profile.AckHeader))
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		cancel()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if seq > 0 {
				got, ackErr := strconv.ParseUint(ack, 10, 64)
				if ackErr != nil || got != seq {
					return fmt.Errorf("%w: httpstream: uplink ack mismatch (got %q, want %d)",
						transport.ErrTransportReset, ack, seq)
				}
			}
			c.ctr.NetworkSent.Add(int64(len(body)))
			if useful > 0 {
				c.ctr.UsefulSent.Add(int64(useful))
				c.ctr.MessagesSent.Add(int64(messages))
				c.ctr.FramesSent.Add(1)
				c.bytesAcked.Add(int64(useful))
			}
			return nil
		}
		lastErr = fmt.Errorf("httpstream: uplink status %d", resp.StatusCode)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return fmt.Errorf("%w: %v", transport.ErrTransportReset, lastErr)
		}
		if !c.waitRetry(ctx, attempt) {
			return ctx.Err()
		}
	}
	return fmt.Errorf("%w: httpstream: uplink failed: %v", transport.ErrTransportReset, lastErr)
}

func (c *Client) waitRetry(ctx context.Context, attempt int) bool {
	delay := time.Duration(50*(attempt+1)) * time.Millisecond
	if delay > time.Second {
		delay = time.Second
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(delay):
		return true
	}
}

func (c *Client) buildURL(spec RequestSpec, path string, cursor, seq uint64, withCursor bool) (string, error) {
	values := PlaceholderValues{Seq: seq, Cursor: cursor}
	expanded := ExpandTemplate(path, values)
	if !strings.HasPrefix(expanded, "/") {
		expanded = "/" + expanded
	}
	base := strings.TrimRight(c.cfg.BaseURL, "/")
	u, err := url.Parse(base + expanded)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for key, list := range spec.Query {
		for _, value := range list {
			q.Add(key, ExpandTemplate(value, values))
		}
	}
	if withCursor {
		q.Set(cursorQueryParam, strconv.FormatUint(cursor, 10))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *Client) runDownlink(ctx context.Context, _ context.Context) {
	defer c.wg.Done()

	base := c.cfg.ReconnectDelay
	delay := base
	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		err := c.downlinkOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			if time.Since(started) >= defaultStableSessionLifetime {
				delay = base
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}

		c.signalReset(err)
		if c.logger != nil {
			c.logger.Warn("httpstream: downlink lost, reconnecting in ", delay, ": ", err)
		}
		wait := delay + time.Duration(mrand.Int64N(int64(delay/4)+1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		delay *= 2
		if delay > c.cfg.ReconnectMax {
			delay = c.cfg.ReconnectMax
		}
	}
}

func (c *Client) downlinkOnce(ctx context.Context) error {
	if c.profile.DownlinkMode == DownlinkChunkedPoll {
		return c.downlinkPoll(ctx)
	}
	return c.downlinkStream(ctx)
}

func (c *Client) downlinkStream(ctx context.Context) error {
	spec, path := c.profile.nextSpec(c.profile.DownlinkSpecs)
	method := strings.TrimSpace(spec.Method)
	if method == "" {
		method = http.MethodGet
	}
	cursor := c.downlinkCursor.Load()
	target, err := c.buildURL(spec, path, cursor, 0, true)
	if err != nil {
		return err
	}
	token, err := signSessionToken(c.cfg.Secret, c.info, time.Now(), c.cfg.TokenTTL)
	if err != nil {
		return err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	c.setDownlinkCancel(cancel)
	defer func() {
		c.clearDownlinkCancel()
		cancel()
	}()

	req, err := http.NewRequestWithContext(streamCtx, method, target, nil)
	if err != nil {
		return err
	}
	for key, value := range spec.Headers {
		req.Header.Set(key, ExpandTemplate(value, PlaceholderValues{Cursor: cursor}))
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/octet-stream")
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", utlsdial.UserAgent(utlsdial.Profile(c.profile.UTLSProfile)))
	}
	if err := c.profile.applySessionToken(req, token); err != nil {
		return err
	}

	headersCtx, cancelHeaders := context.WithCancel(streamCtx)
	defer cancelHeaders()
	var headersFired atomic.Bool
	headerTimer := time.AfterFunc(requestTimeout, func() {
		headersFired.Store(true)
		cancelHeaders()
	})
	defer headerTimer.Stop()
	req = req.WithContext(headersCtx)

	resp, err := c.httpClient.Do(req)
	headerTimer.Stop()
	if err != nil {
		if headersFired.Load() && streamCtx.Err() == nil {
			return fmt.Errorf("%w: httpstream: downlink headers timeout", transport.ErrTransportReset)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("httpstream: downlink status %d", resp.StatusCode)
	}

	var rotating atomic.Bool
	duration := time.AfterFunc(c.profile.MaxStreamDuration, func() {
		rotating.Store(true)
		cancel()
	})
	defer duration.Stop()

	var idleFired atomic.Bool
	idle := time.AfterFunc(c.cfg.ReadIdleTimeout, func() {
		idleFired.Store(true)
		cancel()
	})
	defer idle.Stop()

	budget := c.profile.MaxStreamBytes
	for {
		frame, wire, err := readFrame(resp.Body, c.profile.maxWireFrame())
		if err != nil {
			switch {
			case rotating.Load():
				return nil
			case idleFired.Load():
				return fmt.Errorf("%w: httpstream: downlink idle timeout", transport.ErrTransportReset)
			case ctx.Err() != nil:
				return ctx.Err()
			case errors.Is(err, io.EOF):
				return nil
			default:
				return err
			}
		}
		idle.Reset(c.cfg.ReadIdleTimeout)
		budget -= wire
		c.ctr.NetworkRecv.Add(int64(wire))

		if err := c.handleDownlinkFrame(frame); err != nil {
			return err
		}
		if budget <= 0 {
			return nil
		}
	}
}

func (c *Client) downlinkPoll(ctx context.Context) error {
	first := true
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !first {
			c.pause(ctx)
		}
		first = false

		idle, err := c.pollOnce(ctx)
		if err != nil {
			return err
		}
		if !idle && c.profile.ImmediateOnUplink && c.pendingSends.Load() > 0 {
			continue
		}
	}
}

func (c *Client) pollOnce(ctx context.Context) (bool, error) {
	spec, path := c.profile.nextSpec(c.profile.DownlinkSpecs)
	method := strings.TrimSpace(spec.Method)
	if method == "" {
		method = http.MethodGet
	}
	cursor := c.downlinkCursor.Load()
	target, err := c.buildURL(spec, path, cursor, 0, true)
	if err != nil {
		return false, err
	}
	token, err := signSessionToken(c.cfg.Secret, c.info, time.Now(), c.cfg.TokenTTL)
	if err != nil {
		return false, err
	}

	pollTimeout := c.profile.PollHold + c.cfg.ReadIdleTimeout
	if pollTimeout < requestTimeout {
		pollTimeout = requestTimeout
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, pollTimeout)
	defer timeoutCancel()
	pollCtx, cancel := context.WithCancel(timeoutCtx)
	c.setDownlinkCancel(cancel)
	defer func() {
		c.clearDownlinkCancel()
		cancel()
	}()
	req, err := http.NewRequestWithContext(pollCtx, method, target, nil)
	if err != nil {
		return false, err
	}
	for key, value := range spec.Headers {
		req.Header.Set(key, ExpandTemplate(value, PlaceholderValues{Cursor: cursor}))
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/octet-stream")
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", utlsdial.UserAgent(utlsdial.Profile(c.profile.UTLSProfile)))
	}
	if err := c.profile.applySessionToken(req, token); err != nil {
		return false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return true, nil
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return false, fmt.Errorf("httpstream: poll status %d", resp.StatusCode)
	}

	delivered := false
	for {
		frame, wire, err := readFrame(resp.Body, c.profile.maxWireFrame())
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return delivered, err
			}
			return delivered, err
		}
		c.ctr.NetworkRecv.Add(int64(wire))
		before := c.downlinkCursor.Load()
		if err := c.handleDownlinkFrame(frame); err != nil {
			return delivered, err
		}
		if c.downlinkCursor.Load() > before {
			delivered = true
		}
	}
	return !delivered, nil
}

func (c *Client) handleDownlinkFrame(frame decodedFrame) error {
	if frame.SessionID != c.sid {
		return fmt.Errorf("%w: httpstream: downlink session mismatch", transport.ErrTransportReset)
	}
	if err := c.observeServerGeneration(frame.Epoch); err != nil {
		return err
	}
	switch frame.Kind {
	case frameKeepAlive:
		return nil
	case frameClose:
		return errPeerClosed
	case frameEpochReset:

		c.genResetPending.Store(true)
		c.resetUplinkSequence()
		c.downlinkNext.Store(1)
		c.downlinkCursor.Store(0)
		c.clearReset()
		return fmt.Errorf("%w: httpstream: server requested epoch reset", transport.ErrTransportReset)
	case frameData:
		return c.deliverDownlink(frame)
	default:
		return fmt.Errorf("httpstream: unexpected downlink frame kind %d", frame.Kind)
	}
}

func (c *Client) observeServerGeneration(epoch uint32) error {
	if epoch == 0 {
		return nil
	}
	if !c.genBound.Load() || c.genResetPending.Swap(false) {
		c.serverGen.Store(epoch)
		c.genBound.Store(true)
		return nil
	}
	previous := c.serverGen.Load()
	if previous == epoch {
		return nil
	}
	c.serverGen.Store(epoch)
	c.resetUplinkSequence()
	c.downlinkNext.Store(1)
	c.downlinkCursor.Store(0)
	return fmt.Errorf("%w: httpstream: server session restarted", transport.ErrTransportReset)
}

func (c *Client) deliverDownlink(frame decodedFrame) error {
	if frame.Seq == 0 {
		return fmt.Errorf("%w: httpstream: downlink frame without sequence", transport.ErrTransportReset)
	}
	expected := c.downlinkNext.Load()
	if frame.Seq < expected {
		return nil
	}
	if frame.Seq > expected {
		return fmt.Errorf("%w: httpstream: downlink gap (got %d, want %d)",
			transport.ErrTransportReset, frame.Seq, expected)
	}
	for _, msg := range frame.Messages {

		select {
		case c.recvCh <- recvItem{data: msg}:
		case <-c.transportContext().Done():
			return context.Canceled
		}
	}
	c.downlinkCursor.Store(frame.Seq)
	c.downlinkNext.Store(frame.Seq + 1)
	c.ctr.UsefulRecv.Add(int64(framePayloadLen(frame.Messages)))
	c.ctr.MessagesRecv.Add(int64(len(frame.Messages)))
	c.ctr.FramesRecv.Add(1)
	c.clearReset()
	c.noteDownlinkProgress()
	return nil
}

func framePayloadLen(messages [][]byte) int {
	total := 0
	for _, msg := range messages {
		total += len(msg)
	}
	return total
}

func (c *Client) pause(ctx context.Context) {
	d := c.profile.IdlePauseMin
	if c.profile.IdlePauseMax > d {
		d += time.Duration(mrand.Int64N(int64(c.profile.IdlePauseMax - d)))
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	if c.profile.ImmediateOnUplink {
		select {
		case <-ctx.Done():
		case <-timer.C:
		case <-c.wake:
		}
		return
	}
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (c *Client) ackLoop(ctx context.Context, _ context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.AckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.sendAck(ctx)
		case <-c.ackWake:
			c.sendAck(ctx)
		}
	}
}

func (c *Client) sendAck(ctx context.Context) {
	c.lastAckAt.Store(time.Now().UnixNano())
	if err := c.sendControl(ctx, frameKeepAlive); err != nil && ctx.Err() == nil && c.logger != nil {
		c.logger.Debug("httpstream: keepalive failed: ", err)
	}
}

func (c *Client) noteDownlinkProgress() {
	if interval := downlinkAckMinInterval; interval > 0 {
		if last := c.lastAckAt.Load(); last != 0 && time.Since(time.Unix(0, last)) < interval {
			return
		}
	}
	select {
	case c.ackWake <- struct{}{}:
	default:
	}
}

func (c *Client) sendControl(ctx context.Context, kind frameKind) error {
	frame := encodeFrame(kind, c.sid, c.epoch.Load(), 0, c.downlinkCursor.Load(), nil, 0)
	return c.sendUplinkFrame(ctx, 0, frame, 0, 0)
}

func (c *Client) runDecoy(ctx context.Context, spec DecoySpec) {
	minInterval := spec.IntervalMin
	if minInterval <= 0 {
		minInterval = 15 * time.Second
	}
	maxInterval := spec.IntervalMax
	if maxInterval < minInterval {
		maxInterval = minInterval
	}
	first := true
	for {
		if ctx.Err() != nil {
			return
		}
		if !first {
			wait := minInterval
			if maxInterval > minInterval {
				wait += time.Duration(mrand.Int64N(int64(maxInterval - minInterval)))
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}
		first = false
		c.decoyOnce(ctx, spec)
	}
}

func (c *Client) decoyOnce(ctx context.Context, spec DecoySpec) {
	if len(spec.Paths) == 0 {
		return
	}
	path := spec.Paths[mrand.IntN(len(spec.Paths))]
	specCopy := RequestSpec{Method: http.MethodGet, Paths: []string{path}, Headers: spec.Headers}
	target, err := c.buildURL(specCopy, path, 0, 0, false)
	if err != nil {
		return
	}
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, target, nil)
	if err != nil {
		return
	}
	for key, value := range spec.Headers {
		req.Header.Set(key, value)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", utlsdial.UserAgent(utlsdial.Profile(c.profile.UTLSProfile)))
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
}

var _ transport.Transport = (*Client)(nil)
var _ transport.Telemetry = (*Client)(nil)
