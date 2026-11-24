package httpstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const serverRecvQueueCap = 4096

type SessionHandler func(ctx context.Context, sess *Session) error

type Handler struct {
	profile   MimicProfile
	cfg       ServerConfig
	logger    logging.ContextLogger
	onSession SessionHandler
	fallback  http.Handler

	mu       sync.Mutex
	sessions map[string]*Session

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	initErr error
}

func NewHandler(profile MimicProfile, cfg ServerConfig, onSession SessionHandler) *Handler {
	p := profile.Normalize()
	if cfg.Logger == nil {
		cfg.Logger = logging.New("httpstream", false)
	}
	h := &Handler{
		profile:   p,
		onSession: onSession,
		fallback:  cfg.FallbackHandler,
		sessions:  make(map[string]*Session),
	}
	if err := p.Validate(); err != nil {
		h.initErr = err
	}
	ncfg, err := normalizeServerConfig(p, cfg)
	if err != nil {
		if h.initErr == nil {
			h.initErr = err
		}
		h.cfg = cfg
	} else {
		h.cfg = ncfg
	}
	if h.cfg.Logger == nil {
		h.cfg.Logger = cfg.Logger
	}
	h.logger = h.cfg.Logger
	h.ctx, h.cancel = context.WithCancel(context.Background())
	if h.initErr == nil {
		h.wg.Add(1)
		go h.janitorLoop()
	}
	return h
}

func (h *Handler) Err() error { return h.initErr }

func (h *Handler) Close() error {
	if h.cancel != nil {
		h.cancel()
	}
	h.mu.Lock()
	sessions := make([]*Session, 0, len(h.sessions))
	for key, session := range h.sessions {
		sessions = append(sessions, session)
		delete(h.sessions, key)
	}
	h.mu.Unlock()
	for _, session := range sessions {
		session.closeTransport()
		session.cancel()
	}
	h.wg.Wait()
	return nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.initErr != nil {
		http.Error(w, "httpstream: not configured", http.StatusInternalServerError)
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodPost, http.MethodPut:
	default:
		h.serveFallback(w, r)
		return
	}

	raw := h.profile.extractSessionToken(r)
	if raw == "" {
		h.serveFallback(w, r)
		return
	}
	info, err := verifySessionToken(h.cfg.Secret, raw, time.Now(), h.cfg.TokenTTL, h.cfg.TokenLeeway, h.cfg.Namespace)
	if err != nil {
		if h.logger != nil {
			h.logger.Debug("httpstream: rejected session token: ", err)
		}
		h.serveFallback(w, r)
		return
	}
	if !roleAllowed(h.cfg.AllowedRoles, info.Role) {
		if h.logger != nil {
			h.logger.Debug("httpstream: rejected session role ", info.Role)
		}
		h.serveFallback(w, r)
		return
	}
	if !h.matchPinned(&info) {
		h.serveFallback(w, r)
		return
	}
	if h.cfg.Authorize != nil {
		if err := h.cfg.Authorize(r.Context(), info); err != nil {
			if h.logger != nil {
				h.logger.Warn("httpstream: session authorization failed for ", info.UserID, ": ", err)
			}
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
	}

	session, err := h.getOrCreate(info)
	if err != nil {
		http.Error(w, "session unavailable", http.StatusServiceUnavailable)
		return
	}
	session.touch()

	switch r.Method {
	case http.MethodGet:
		h.handleDownlink(w, r, session)
	case http.MethodPost, http.MethodPut:
		h.handleUplink(w, r, session)
	}
}

func (h *Handler) matchPinned(info *SessionInfo) bool {
	if info == nil {
		return false
	}
	if h.cfg.PinnedUserID != "" {
		if info.UserID != "" && info.UserID != h.cfg.PinnedUserID {
			return false
		}
		info.UserID = h.cfg.PinnedUserID
	}
	if h.cfg.RequireUser && info.UserID == "" {
		return false
	}
	if h.cfg.PinnedExitNodeID != "" {
		if info.ExitNodeID != "" && info.ExitNodeID != h.cfg.PinnedExitNodeID {
			return false
		}
		info.ExitNodeID = h.cfg.PinnedExitNodeID
	}
	return true
}

func (h *Handler) serveFallback(w http.ResponseWriter, r *http.Request) {
	if h.fallback != nil {
		h.fallback.ServeHTTP(w, r)
		return
	}
	http.NotFound(w, r)
}

func (h *Handler) getOrCreate(info SessionInfo) (*Session, error) {
	sid, err := parseSessionID(info.SessionID)
	if err != nil {
		return nil, err
	}
	key := info.Namespace + "\x00" + info.UserID + "\x00" + info.Role + "\x00" + strings.ToLower(info.SessionID)

	h.mu.Lock()
	if h.ctx == nil || h.ctx.Err() != nil {
		h.mu.Unlock()
		return nil, errors.New("httpstream: handler is closed")
	}
	if session := h.sessions[key]; session != nil {
		h.mu.Unlock()
		return session, nil
	}
	if len(h.sessions) >= h.cfg.MaxSessions {
		var oldest *Session
		for _, session := range h.sessions {
			if oldest == nil || session.lastSeen.Load() < oldest.lastSeen.Load() {
				oldest = session
			}
		}
		if oldest != nil {
			delete(h.sessions, oldest.key)
			oldest.closeTransport()
			oldest.cancel()
		}
	}
	session := newSession(h, key, info, sid)
	h.sessions[key] = session
	h.wg.Add(1)
	go h.serveSession(session)
	h.mu.Unlock()
	return session, nil
}

func (h *Handler) serveSession(session *Session) {
	defer h.wg.Done()
	defer func() {
		session.closeTransport()
		session.cancel()
		h.mu.Lock()
		if h.sessions[session.key] == session {
			delete(h.sessions, session.key)
		}
		h.mu.Unlock()
	}()

	if h.logger != nil {
		h.logger.Info("httpstream: session started (namespace=", session.info.Namespace,
			" user=", session.info.UserID, " sid=", shortSessionID(session.info.SessionID), ")")
	}
	handler := h.onSession
	if handler == nil {
		for {
			if _, err := session.Receive(session.ctx); err != nil {
				return
			}
		}
	}
	if err := handler(session.ctx, session); err != nil && session.ctx.Err() == nil && h.logger != nil {
		h.logger.Warn("httpstream: session ended: ", err)
	}
}

func (h *Handler) janitorLoop() {
	defer h.wg.Done()
	interval := h.cfg.SessionTTL / 2
	if interval < 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.expireIdleSessions()
		}
	}
}

func (h *Handler) expireIdleSessions() {
	now := time.Now()
	var stale []*Session
	h.mu.Lock()
	for key, session := range h.sessions {
		last := time.Unix(0, session.lastSeen.Load())
		if now.Sub(last) > h.cfg.SessionTTL {
			delete(h.sessions, key)
			stale = append(stale, session)
		}
	}
	h.mu.Unlock()
	for _, session := range stale {
		if h.logger != nil {
			h.logger.Info("httpstream: expiring idle session ", shortSessionID(session.info.SessionID))
		}
		session.closeTransport()
		session.cancel()
	}
}

func shortSessionID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

type downlinkFrame struct {
	seq    uint64
	wire   []byte
	useful int
}

type Session struct {
	handler *Handler
	key     string
	info    SessionInfo
	sid     [16]byte

	ctx    context.Context
	cancel context.CancelFunc

	epoch      atomic.Uint32
	generation atomic.Uint32
	reorder    *ReorderingBuffer
	recvCh     chan []byte

	dlMu      sync.Mutex
	dlWait    chan struct{}
	dlFrames  []*downlinkFrame
	dlBytes   int
	dlMinSeq  uint64
	dlNextSeq uint64
	dlAcked   uint64
	dlClosed  bool

	lastSeen atomic.Int64

	uplinkCursor atomic.Uint64

	ctr            transport.Counters
	bytesDelivered atomic.Int64
	bytesAcked     atomic.Int64
}

func newSession(h *Handler, key string, info SessionInfo, sid [16]byte) *Session {
	s := &Session{
		handler:   h,
		key:       key,
		info:      info,
		sid:       sid,
		recvCh:    make(chan []byte, serverRecvQueueCap),
		dlWait:    make(chan struct{}, 1),
		dlMinSeq:  1,
		dlNextSeq: 1,
	}
	s.ctx, s.cancel = context.WithCancel(h.ctx)
	s.epoch.Store(1)
	s.generation.Store(randomUint32())
	s.reorder = newReorderingBuffer(h.cfg.ReorderTimeout, h.cfg.ReorderCapacity, h.cfg.ReorderMaxBytes)
	if h.cfg.SessionSeed != nil {
		if epoch, delivered := h.cfg.SessionSeed(info); epoch != 0 || delivered != 0 {
			s.epoch.Store(epoch)
			s.reorder.Resume(epoch, delivered)
		}
	}
	s.lastSeen.Store(time.Now().UnixNano())
	h.wg.Add(1)
	go s.watchReorder()
	return s
}

func (s *Session) watchReorder() {
	defer s.handler.wg.Done()
	interval := s.handler.cfg.ReorderTimeout / 4
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	if interval > time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.reorder.CheckDeadline(time.Now()); err != nil {
				if s.handler.logger != nil {
					s.handler.logger.Warn("httpstream: uplink reorder stalled: ", err)
				}
				s.closeTransport()
				s.cancel()
				return
			}
		}
	}
}

func (s *Session) Info() SessionInfo { return s.info }

func (s *Session) SessionID() string { return s.info.SessionID }

func (s *Session) Start(context.Context) error { return nil }

func (s *Session) Stop(context.Context) error {
	s.closeTransport()
	s.cancel()
	return nil
}

func (s *Session) Close() error { return s.Stop(context.Background()) }

func (s *Session) Send(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if len(data) > s.MaxMessageSize() {
		return fmt.Errorf("httpstream: message too large (%d > %d)", len(data), s.MaxMessageSize())
	}
	_, err := s.enqueueDownlink(ctx, frameData, [][]byte{data})
	return err
}

func (s *Session) Receive(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, fmt.Errorf("%w: httpstream: session closed", transport.ErrTransportReset)
	case data := <-s.recvCh:
		s.bytesDelivered.Add(int64(len(data)))
		return data, nil
	}
}

func (s *Session) MaxMessageSize() int   { return s.handler.cfg.MaxMessageSize }
func (s *Session) BytesDelivered() int64 { return s.bytesDelivered.Load() }
func (s *Session) BytesAcked() int64     { return s.bytesAcked.Load() }

func (s *Session) UplinkState() (uint32, uint64) {
	if s == nil {
		return 0, 0
	}
	return s.epoch.Load(), s.reorder.Delivered()
}

func (s *Session) WalkLayers() []transport.Layer {
	name := s.info.UserID
	if name == "" {
		name = s.info.Namespace + "/" + shortSessionID(s.info.SessionID)
	}
	return []transport.Layer{{
		Kind: "pipe", Type: "httpstream", Name: name, Snapshot: s.ctr.Snapshot(),
	}}
}

func (s *Session) touch() { s.lastSeen.Store(time.Now().UnixNano()) }

func (s *Session) closeTransport() {
	s.dlMu.Lock()
	s.dlClosed = true
	s.dlMu.Unlock()
	s.notifyDownlink()
}

func (s *Session) notifyDownlink() {
	select {
	case s.dlWait <- struct{}{}:
	default:
	}
}

func (s *Session) enqueueDownlink(ctx context.Context, kind frameKind, messages [][]byte) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	useful := framePayloadLen(messages)
	payloadSize := 0
	for _, msg := range messages {
		payloadSize += 4 + len(msg)
	}
	pad := s.handler.profile.Padding.PadLen(frameHeaderSize + payloadSize)

	for {
		s.dlMu.Lock()
		if s.dlClosed {
			s.dlMu.Unlock()
			return 0, fmt.Errorf("%w: httpstream: session closed", transport.ErrTransportReset)
		}
		seq := s.dlNextSeq
		wire := encodeFrame(kind, s.sid, s.generation.Load(), seq, s.uplinkCursor.Load(), messages, pad)
		if len(s.dlFrames) == 0 || s.dlBytes+len(wire) <= s.handler.cfg.DownlinkReplayBytes {
			s.dlFrames = append(s.dlFrames, &downlinkFrame{seq: seq, wire: wire, useful: useful})
			s.dlBytes += len(wire)
			s.dlNextSeq++
			s.dlMu.Unlock()
			s.notifyDownlink()
			s.ctr.NetworkSent.Add(int64(len(wire)))
			s.ctr.UsefulSent.Add(int64(useful))
			s.ctr.MessagesSent.Add(int64(len(messages)))
			s.ctr.FramesSent.Add(1)
			return seq, nil
		}
		s.dlMu.Unlock()

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.ctx.Done():
			return 0, fmt.Errorf("%w: httpstream: session closed", transport.ErrTransportReset)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s *Session) ackDownlink(cursor uint64) {
	s.dlMu.Lock()
	if cursor >= s.dlNextSeq || cursor <= s.dlAcked {
		s.dlMu.Unlock()
		return
	}
	removed := 0
	freed := 0
	useful := 0
	for removed < len(s.dlFrames) && s.dlFrames[removed].seq <= cursor {
		freed += len(s.dlFrames[removed].wire)
		useful += s.dlFrames[removed].useful
		removed++
	}
	if removed > 0 {
		remaining := make([]*downlinkFrame, len(s.dlFrames)-removed)
		copy(remaining, s.dlFrames[removed:])
		s.dlFrames = remaining
		s.dlBytes -= freed
	}
	s.dlAcked = cursor
	if len(s.dlFrames) == 0 {
		s.dlMinSeq = s.dlNextSeq
	} else {
		s.dlMinSeq = s.dlFrames[0].seq
	}
	s.dlMu.Unlock()
	if useful > 0 {
		s.bytesAcked.Add(int64(useful))
	}
	s.notifyDownlink()
}

func (s *Session) snapshotFrom(cursor uint64, maxBytes int) ([]*downlinkFrame, bool) {
	s.dlMu.Lock()
	defer s.dlMu.Unlock()
	if cursor >= s.dlNextSeq {

		return nil, false
	}
	start := cursor + 1
	if start < s.dlMinSeq {
		return nil, true
	}
	if maxBytes <= 0 {
		maxBytes = s.handler.cfg.DownlinkReplayBytes
	}
	var out []*downlinkFrame
	total := 0
	for _, frame := range s.dlFrames {
		if frame.seq < start {
			continue
		}
		if total > 0 && total+len(frame.wire) > maxBytes {
			break
		}
		out = append(out, frame)
		total += len(frame.wire)
		if total >= maxBytes {
			break
		}
	}
	return out, false
}

func (s *Session) handleFrame(frame decodedFrame) error {
	if frame.SessionID != s.sid {
		return errors.New("httpstream: frame session mismatch")
	}
	s.touch()
	switch frame.Kind {
	case frameKeepAlive:
		s.ackDownlink(frame.Ack)
		return nil
	case frameClose:
		s.closeTransport()
		s.cancel()
		return nil
	case frameEpochReset:
		if frame.Epoch != 0 && frame.Epoch >= s.epoch.Load() {
			s.epoch.Store(frame.Epoch)
			s.reorder.Reset(frame.Epoch)
		}
		s.ackDownlink(frame.Ack)
		return nil
	case frameData:
		if frame.Epoch == 0 || frame.Seq == 0 {
			return errors.New("httpstream: data frame without epoch or sequence")
		}
		current := s.epoch.Load()
		if frame.Epoch < current {
			return nil
		}
		if frame.Epoch > current {
			s.epoch.Store(frame.Epoch)
		}
		ready, err := s.reorder.Push(frame.Epoch, frame.Seq, frame.Payload)
		if err != nil {
			s.closeTransport()
			s.cancel()
			return err
		}
		if len(ready) == 0 {
			return nil
		}
		s.uplinkCursor.Store(s.reorder.Delivered())
		for _, payload := range ready {
			msgs, err := decodeMessages(payload)
			if err != nil {
				return err
			}
			s.ctr.FramesRecv.Add(1)
			for _, msg := range msgs {
				cp := append([]byte(nil), msg...)
				select {
				case s.recvCh <- cp:
				case <-s.ctx.Done():
					return fmt.Errorf("%w: httpstream: session closed", transport.ErrTransportReset)
				}
				s.ctr.UsefulRecv.Add(int64(len(cp)))
				s.ctr.MessagesRecv.Add(1)
			}
		}
		return nil
	default:
		return fmt.Errorf("httpstream: unexpected uplink frame kind %d", frame.Kind)
	}
}

func (h *Handler) handleUplink(w http.ResponseWriter, r *http.Request, session *Session) {
	setNoBufferHeaders(w)
	limit := int64(h.cfg.MaxUplinkBodyBytes)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	session.ctr.NetworkRecv.Add(int64(len(body)))

	raw, err := h.profile.UnwrapUplinkBody(r.Header.Get("Content-Type"), body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	frame, err := decodeFrame(raw, h.profile.maxWireFrame())
	if err != nil {
		http.Error(w, "bad frame", http.StatusBadRequest)
		return
	}
	if frame.SessionID != session.sid {
		http.Error(w, "session mismatch", http.StatusBadRequest)
		return
	}
	if err := session.handleFrame(frame); err != nil {
		if errors.Is(err, transport.ErrTransportReset) {
			http.Error(w, "reset", http.StatusConflict)
			return
		}
		http.Error(w, "bad frame", http.StatusBadRequest)
		return
	}

	ack := uint64(0)
	if frame.Kind == frameData {
		ack = frame.Seq
	}
	w.Header().Set(h.profile.AckHeader, strconv.FormatUint(ack, 10))
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

func (h *Handler) handleDownlink(w http.ResponseWriter, r *http.Request, session *Session) {
	cursor := requestCursor(r)
	session.ackDownlink(cursor)
	if h.profile.DownlinkMode == DownlinkChunkedPoll {
		h.downlinkPoll(w, r, session, cursor)
		return
	}
	h.downlinkStream(w, r, session, cursor)
}

func (h *Handler) downlinkStream(w http.ResponseWriter, r *http.Request, session *Session, cursor uint64) {
	setStreamHeaders(w)
	w.WriteHeader(http.StatusOK)
	flushResponse(w)

	budget := h.profile.MaxStreamBytes
	initial := encodeFrame(frameKeepAlive, session.sid, session.generation.Load(), 0,
		session.uplinkCursor.Load(), nil, 0)
	if err := writeFrame(w, initial); err != nil {
		return
	}
	budget -= len(initial)
	if budget <= 0 {
		return
	}
	duration := time.NewTimer(h.profile.MaxStreamDuration)
	defer duration.Stop()
	keepalive := time.NewTicker(h.cfg.KeepAliveInterval)
	defer keepalive.Stop()
	watch := time.NewTicker(500 * time.Millisecond)
	defer watch.Stop()

	for {
		if err := session.reorder.CheckDeadline(time.Now()); err != nil {
			session.closeTransport()
			session.cancel()
			return
		}
		frames, needReset := session.snapshotFrom(cursor, budget)
		if needReset {
			h.writeEpochReset(w, session)
			session.closeTransport()
			session.cancel()
			return
		}
		if len(frames) > 0 {
			for _, frame := range frames {
				if err := writeFrame(w, frame.wire); err != nil {
					return
				}
				cursor = frame.seq
				budget -= len(frame.wire)
				if budget <= 0 {
					return
				}
			}
			continue
		}

		select {
		case <-r.Context().Done():
			return
		case <-session.ctx.Done():
			return
		case <-duration.C:
			return
		case <-watch.C:
			session.touch()
		case <-keepalive.C:
			frame := encodeFrame(frameKeepAlive, session.sid, session.generation.Load(), 0,
				session.uplinkCursor.Load(), nil, 0)
			if err := writeFrame(w, frame); err != nil {
				return
			}
			budget -= len(frame)
			if budget <= 0 {
				return
			}
		case <-session.dlWait:
		}
	}
}

func (h *Handler) downlinkPoll(w http.ResponseWriter, r *http.Request, session *Session, cursor uint64) {
	deadline := time.NewTimer(h.cfg.PollHold)
	defer deadline.Stop()
	watch := time.NewTicker(500 * time.Millisecond)
	defer watch.Stop()

	for {
		if err := session.reorder.CheckDeadline(time.Now()); err != nil {
			session.closeTransport()
			session.cancel()
			return
		}
		frames, needReset := session.snapshotFrom(cursor, h.profile.TargetChunkMax)
		if needReset {
			setStreamHeaders(w)
			w.WriteHeader(http.StatusOK)
			h.writeEpochReset(w, session)
			session.closeTransport()
			session.cancel()
			return
		}
		if len(frames) > 0 {
			setStreamHeaders(w)
			w.WriteHeader(http.StatusOK)
			for _, frame := range frames {
				if err := writeFrame(w, frame.wire); err != nil {
					return
				}
				cursor = frame.seq
			}
			return
		}

		select {
		case <-r.Context().Done():
			return
		case <-session.ctx.Done():
			return
		case <-deadline.C:

			setStreamHeaders(w)
			w.WriteHeader(http.StatusOK)
			frame := encodeFrame(frameKeepAlive, session.sid, session.generation.Load(), 0,
				session.uplinkCursor.Load(), nil, 0)
			if err := writeFrame(w, frame); err != nil {
				return
			}
			return
		case <-watch.C:
			session.touch()
		case <-session.dlWait:
		}
	}
}

func (h *Handler) writeEpochReset(w http.ResponseWriter, session *Session) {
	frame := encodeFrame(frameEpochReset, session.sid, session.generation.Load(), 0,
		session.uplinkCursor.Load(), nil, 0)
	if err := writeFrame(w, frame); err != nil && h.logger != nil {
		h.logger.Debug("httpstream: write epoch reset: ", err)
	}
}

func writeFrame(w http.ResponseWriter, frame []byte) error {
	if _, err := w.Write(frame); err != nil {
		return err
	}
	flushResponse(w)
	return nil
}

func (h *Handler) WalkLayers() []transport.Layer {
	h.mu.Lock()
	sessions := make([]*Session, 0, len(h.sessions))
	for _, session := range h.sessions {
		sessions = append(sessions, session)
	}
	h.mu.Unlock()

	var appSent, appRecv, wireSent, wireRecv int64
	pipes := make([]transport.Layer, 0, len(sessions))
	for _, session := range sessions {
		snap := session.ctr.Snapshot()
		appSent += snap.UsefulSent
		appRecv += snap.UsefulRecv
		wireSent += snap.NetworkSent
		wireRecv += snap.NetworkRecv
		name := session.info.UserID
		if name == "" {
			name = session.info.Namespace + "/" + shortSessionID(session.info.SessionID)
		}
		pipes = append(pipes, transport.Layer{
			Kind: "pipe", Type: "httpstream", Name: name, Snapshot: snap,
		})
	}
	out := make([]transport.Layer, 0, len(pipes)+1)
	out = append(out, transport.Layer{
		Kind: "mux", Type: "httpstream",
		Snapshot: transport.Snapshot{
			UsefulSent:  appSent,
			UsefulRecv:  appRecv,
			NetworkSent: wireSent,
			NetworkRecv: wireRecv,
			Timestamp:   time.Now(),
		},
	})
	out = append(out, pipes...)
	return out
}

func (h *Handler) SessionCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sessions)
}

var _ http.Handler = (*Handler)(nil)
var _ transport.Telemetry = (*Handler)(nil)
var _ transport.Transport = (*Session)(nil)
var _ transport.Telemetry = (*Session)(nil)
