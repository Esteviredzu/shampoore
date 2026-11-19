package reliability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	initialRetryDelay  = 1 * time.Second
	maxRetryDelay      = 10 * time.Second
	retryScanInterval  = 100 * time.Millisecond
	maxReorderDistance = 256
	defaultWindow      = 64
	sendWindowTimeout  = 10 * time.Second
	sessionCooldown    = 5 * time.Second

	defaultAckBatchMs = 20

	maxAssembledMessageSize = 4 << 20

	maxBufferedIncomingBytes = 16 << 20

	maxUnexpectedRemoteSessions = 64
)

type Config struct {
	Window             int
	ChunkSize          int
	AckBatchMs         int
	AckDisabled        bool
	EncryptionPassword string

	Role string
}

func DefaultConfig() Config {
	return Config{Window: defaultWindow, AckBatchMs: defaultAckBatchMs}
}

type pendingPacket struct {
	sequence   int
	data       []byte
	keys       []string
	lastSentAt time.Time
	retryDelay time.Duration
	attempts   int
}

type receivedPacket struct {
	sequence  int
	sessionID string
	data      []byte
}

type resetMarker struct {
	reason string
}

type ReliableSession struct {
	pipe   transport.Pipe
	codec  *ReliableCodec
	logger logging.ContextLogger
	cfg    Config

	ownSessionID string

	remoteMu                 sync.Mutex
	remoteSessionID          string
	lastSessionChange        time.Time
	unexpectedRemoteSessions map[string]bool

	sendMu           sync.Mutex
	pendingPackets   map[int]*pendingPacket
	nextSendSequence int
	spaceCh          chan struct{}

	encrypted bool

	wakeRetry chan struct{}

	recvMu              sync.Mutex
	nextReceiveSequence int
	nextACKSequence     int
	incomingChunks      map[int]map[int][]byte
	incomingTotals      map[int]int
	incomingSizes       map[int]int
	incomingBytes       int
	maxChunksPerMessage int
	completedPackets    map[int][]byte

	pendingACKSession string
	pendingACKSeq     int
	ackDirty          bool
	ackCond           *sync.Cond

	receiveQueue chan any

	bytesDelivered atomic.Int64
	bytesAcked     atomic.Int64

	ctr transport.Counters

	ctx      context.Context
	cancel   context.CancelFunc
	started  bool
	starting bool
	closed   bool
	wg       sync.WaitGroup
	ackWg    sync.WaitGroup
}

func NewReliableSession(pipe transport.Pipe, log logging.ContextLogger, cfg Config) (*ReliableSession, error) {
	if cfg.Window <= 0 {
		cfg.Window = defaultWindow
	}

	if cfg.AckBatchMs <= 0 {
		cfg.AckBatchMs = defaultAckBatchMs
	}

	encrypted := false
	if cfg.EncryptionPassword != "" {
		wrapped, err := security.WrapPipeWithRole(pipe, cfg.EncryptionPassword, cfg.Role, log)
		if err != nil {
			return nil, err
		}
		pipe = wrapped
		encrypted = true
		log.Info("reliable: encryption enabled")
	}

	var chunkSize int
	if cfg.ChunkSize > 0 {
		chunkSize = cfg.ChunkSize
		log.Info("reliable: chunk_size from config = ", chunkSize)
	} else {
		var err error
		chunkSize, err = DeriveChunkSize(pipe.MaxMessageSize())
		if err != nil {
			return nil, err
		}
		log.Info("reliable: auto chunk_size = ", chunkSize,
			" (pipe max = ", pipe.MaxMessageSize(), ")")
	}

	codec, err := NewReliableCodec(chunkSize)
	if err != nil {
		return nil, err
	}

	maxChunksPerMessage := maxAssembledMessageSize / chunkSize
	if maxAssembledMessageSize%chunkSize != 0 {
		maxChunksPerMessage++
	}
	if maxChunksPerMessage < 1 {
		maxChunksPerMessage = 1
	}
	if maxChunksPerMessage > maxChunks {
		maxChunksPerMessage = maxChunks
	}

	s := &ReliableSession{
		pipe:                     pipe,
		codec:                    codec,
		logger:                   log,
		cfg:                      cfg,
		encrypted:                encrypted,
		unexpectedRemoteSessions: make(map[string]bool),
		pendingPackets:           make(map[int]*pendingPacket),
		spaceCh:                  make(chan struct{}, 1),
		incomingChunks:           make(map[int]map[int][]byte),
		incomingTotals:           make(map[int]int),
		incomingSizes:            make(map[int]int),
		maxChunksPerMessage:      maxChunksPerMessage,
		completedPackets:         make(map[int][]byte),
		receiveQueue:             make(chan any, 512),
		wakeRetry:                make(chan struct{}, 1),
	}
	s.ackCond = sync.NewCond(&s.recvMu)

	if cfg.AckDisabled {
		log.Warn("reliable: ACK disabled")
	} else {
		log.Info("reliable: ack batching every ", cfg.AckBatchMs, "ms")
	}
	return s, nil
}

func (s *ReliableSession) OwnSessionID() string { return s.ownSessionID }

func (s *ReliableSession) MaxMessageSize() int {
	return s.pipe.MaxMessageSize()
}

func (s *ReliableSession) BytesDelivered() int64 {
	return s.bytesDelivered.Load()
}

func (s *ReliableSession) BytesAcked() int64 {
	return s.bytesAcked.Load()
}

func (s *ReliableSession) WalkLayers() []transport.Layer {
	out := []transport.Layer{{
		Kind: "transport", Type: "reliable",
		Snapshot: s.ctr.Snapshot(),
	}}
	if tw, ok := s.pipe.(transport.Telemetry); ok {
		out = append(out, tw.WalkLayers()...)
	}
	return out
}

func (s *ReliableSession) Start(ctx context.Context) error {
	s.sendMu.Lock()
	if s.started || s.starting {
		s.sendMu.Unlock()
		return nil
	}
	if s.closed {
		s.sendMu.Unlock()
		return errors.New("reliable: session closed")
	}
	s.starting = true
	s.sendMu.Unlock()

	if err := s.pipe.Start(ctx); err != nil {
		s.sendMu.Lock()
		s.starting = false
		s.sendMu.Unlock()
		return err
	}

	s.sendMu.Lock()
	if s.closed {
		s.starting = false
		s.sendMu.Unlock()
		if err := s.pipe.Stop(context.Background()); err != nil {
			s.logger.Warn("reliable: pipe stop after aborted start: ", err)
		}
		return errors.New("reliable: session closed during start")
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.ownSessionID = uuid.NewString()
	s.nextSendSequence = 0
	s.recvMu.Lock()
	s.nextReceiveSequence = 0
	s.nextACKSequence = 0
	s.recvMu.Unlock()
	s.started = true
	s.starting = false

	ackFlush := s.cfg.AckBatchMs > 0 && !s.cfg.AckDisabled

	s.wg.Add(2)
	if ackFlush {
		s.wg.Add(1)
	}
	s.sendMu.Unlock()

	s.logger.Info("reliable: started; session=", s.ownSessionID,
		" window=", s.cfg.Window,
		" ack_disabled=", s.cfg.AckDisabled,
		" ack_batch_ms=", s.cfg.AckBatchMs)

	go s.receiveLoop()
	go s.retryLoop()
	if ackFlush {
		go s.ackFlushLoop()
	}
	return nil
}

func (s *ReliableSession) Stop(ctx context.Context) error {
	s.sendMu.Lock()
	if s.closed {
		s.sendMu.Unlock()
		return nil
	}
	s.closed = true
	cancel := s.cancel
	s.sendMu.Unlock()

	s.recvMu.Lock()
	s.ackCond.Broadcast()
	s.recvMu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
	s.ackWg.Wait()

	return s.pipe.Stop(ctx)
}

func (s *ReliableSession) Send(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	deadline := time.Now().Add(sendWindowTimeout)

	for {
		s.sendMu.Lock()
		if s.closed {
			s.sendMu.Unlock()
			return errors.New("reliable: session closed")
		}
		if len(s.pendingPackets) < s.cfg.Window {
			break
		}
		s.sendMu.Unlock()

		remain := time.Until(deadline)
		if remain <= 0 {
			return errors.New("reliable: send window timeout")
		}
		timer := time.NewTimer(remain)
		select {
		case <-s.spaceCh:
			timer.Stop()
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-s.ctx.Done():
			timer.Stop()
			return errors.New("reliable: session closed")
		case <-timer.C:
			return errors.New("reliable: send window timeout")
		}
	}

	seq := s.nextSendSequence
	s.nextSendSequence++

	chunks := (len(data) + s.codec.ChunkSize() - 1) / s.codec.ChunkSize()
	if chunks < 1 {
		chunks = 1
	}
	keys := make([]string, chunks)
	for i := range keys {
		keys[i] = fmt.Sprintf("d:%s:%d:%d", s.ownSessionID, seq, i)
	}

	s.pendingPackets[seq] = &pendingPacket{
		sequence:   seq,
		data:       data,
		keys:       keys,
		retryDelay: initialRetryDelay,
	}
	s.sendMu.Unlock()

	s.ctr.UsefulSent.Add(int64(len(data)))

	select {
	case s.wakeRetry <- struct{}{}:
	default:
	}
	return nil
}

func (s *ReliableSession) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case item, ok := <-s.receiveQueue:
		if !ok {
			return nil, io.EOF
		}
		switch v := item.(type) {
		case *resetMarker:
			return nil, fmt.Errorf("%w: %s", transport.ErrTransportReset, v.reason)
		case receivedPacket:
			s.recvMu.Lock()
			expected := s.nextACKSequence
			if v.sequence != expected {
				s.recvMu.Unlock()
				return nil, fmt.Errorf(
					"receive sequence mismatch: expected %d, got %d",
					expected, v.sequence)
			}
			s.nextACKSequence++
			s.recvMu.Unlock()

			if !s.cfg.AckDisabled {
				if s.cfg.AckBatchMs > 0 {
					s.markACKDirty(v.sessionID, v.sequence)
				} else {
					s.ackWg.Add(1)
					go func(sid string, seq int) {
						defer s.ackWg.Done()
						s.sendACKSafe(sid, seq)
					}(v.sessionID, v.sequence)
				}
			}
			s.bytesDelivered.Add(int64(len(v.data)))
			s.ctr.UsefulRecv.Add(int64(len(v.data)))
			return v.data, nil
		default:
			return nil, fmt.Errorf("unexpected queue item type %T", item)
		}
	}
}

func (s *ReliableSession) markACKDirty(sessionID string, seq int) {
	s.recvMu.Lock()
	s.pendingACKSession = sessionID
	s.pendingACKSeq = seq
	s.ackDirty = true
	s.ackCond.Signal()
	s.recvMu.Unlock()
}

func (s *ReliableSession) ackFlushLoop() {
	defer s.wg.Done()
	interval := time.Duration(s.cfg.AckBatchMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}

		s.recvMu.Lock()
		if !s.ackDirty {
			s.recvMu.Unlock()
			continue
		}
		sid := s.pendingACKSession
		seq := s.pendingACKSeq
		s.ackDirty = false
		s.recvMu.Unlock()

		s.ackWg.Add(1)
		go func(sid string, seq int) {
			defer s.ackWg.Done()
			s.sendACKSafe(sid, seq)
		}(sid, seq)
	}
}

func (s *ReliableSession) retryLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(retryScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wakeRetry:
		case <-ticker.C:
		}

		s.sendMu.Lock()
		packets := make([]*pendingPacket, 0, len(s.pendingPackets))
		for _, p := range s.pendingPackets {
			packets = append(packets, p)
		}
		s.sendMu.Unlock()

		now := time.Now()
		for _, p := range packets {
			s.sendMu.Lock()
			cur, ok := s.pendingPackets[p.sequence]
			if !ok || cur != p {
				s.sendMu.Unlock()
				continue
			}
			due := p.lastSentAt.IsZero() || now.Sub(p.lastSentAt) >= p.retryDelay
			s.sendMu.Unlock()
			if !due {
				continue
			}
			if err := s.transmitPacket(p); err != nil {
				s.logger.Warn("reliable: transmit error for seq ",
					p.sequence, ": ", err)
				s.sendMu.Lock()
				p.lastSentAt = time.Now()
				p.retryDelay = nextDelay(p.retryDelay)
				s.sendMu.Unlock()
			}
		}
	}
}

func nextDelay(d time.Duration) time.Duration {
	nd := d * 2
	if nd > maxRetryDelay {
		nd = maxRetryDelay
	}
	if nd < initialRetryDelay {
		nd = initialRetryDelay
	}
	return nd
}

func (s *ReliableSession) transmitPacket(p *pendingPacket) error {
	encoded, err := s.codec.Encode(s.ownSessionID, p.sequence, p.data)
	if err != nil {
		return err
	}
	if p.attempts > 0 {
		s.logger.Debug("reliable: retransmit seq ", p.sequence,
			", attempt ", p.attempts+1)
	}
	for i, chunk := range encoded {
		s.sendMu.Lock()
		cur, ok := s.pendingPackets[p.sequence]
		if !ok || cur != p {
			s.sendMu.Unlock()
			return nil
		}
		s.sendMu.Unlock()

		key := p.keys[i]
		if err := s.pipe.SendMessage(s.ctx, chunk, key); err != nil {
			return err
		}
		s.ctr.NetworkSent.Add(int64(len(chunk)))
	}
	s.sendMu.Lock()
	p.attempts++
	p.lastSentAt = time.Now()
	if p.attempts > 1 {
		p.retryDelay = nextDelay(p.retryDelay)
	}
	s.sendMu.Unlock()
	return nil
}

func (s *ReliableSession) receiveLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		raw, err := s.pipe.ReceiveMessage(s.ctx)
		if err != nil {
			if errors.Is(err, transport.ErrPipeReset) {
				s.logger.Warn("reliable: pipe reset")
				s.handlePipeReset()
				continue
			}
			if errors.Is(err, io.EOF) {
				s.logger.Warn("reliable: pipe closed")
				s.signalTransportReset("pipe closed")
				return
			}
			if s.ctx.Err() != nil {
				return
			}
			s.logger.Error("reliable: receive loop error: ", err)
			s.signalTransportReset("receive loop error: " + err.Error())
			return
		}

		s.ctr.NetworkRecv.Add(int64(len(raw)))
		s.handleRaw(raw)
	}
}

func (s *ReliableSession) handleRaw(raw []byte) {
	if s.encrypted && !pipeAuthenticated(s.pipe) {
		s.logger.Error("reliable: dropping unauthenticated packet from insecure pipe")
		return
	}
	pkt := s.codec.Decode(raw)
	if pkt == nil {
		return
	}
	s.dispatch(pkt)
}

type authenticatedPipe interface {
	Authenticated() bool
}

func pipeAuthenticated(p transport.Pipe) bool {
	a, ok := p.(authenticatedPipe)
	return ok && a.Authenticated()
}

func (s *ReliableSession) dispatch(pkt *DecodedPacket) {
	if pkt.Kind == "data" {
		if pkt.SessionID == s.ownSessionID {
			return
		}
		key := fmt.Sprintf("d:%s:%d:%d",
			pkt.SessionID, pkt.Sequence, pkt.ChunkIndex)
		if s.acceptRemoteSession(pkt.SessionID) {
			s.processChunk(pkt)
		}
		s.safeDelete(key)
		return
	}

	if pkt.Kind == "ack" {
		if pkt.SessionID != s.ownSessionID {
			return
		}
		key := fmt.Sprintf("a:%s:%s:%d",
			pkt.SenderSessionID, pkt.SessionID, pkt.Sequence)
		if s.acceptRemoteSession(pkt.SenderSessionID) {
			s.processACK(pkt.Sequence)
		}
		s.safeDelete(key)
	}
}

func (s *ReliableSession) acceptRemoteSession(sessionID string) bool {
	s.remoteMu.Lock()
	if s.remoteSessionID == "" {
		s.remoteSessionID = sessionID
		s.remoteMu.Unlock()
		return true
	}
	if sessionID == s.remoteSessionID {
		s.remoteMu.Unlock()
		return true
	}
	now := time.Now()
	if !s.lastSessionChange.IsZero() && now.Sub(s.lastSessionChange) < sessionCooldown {
		s.remoteMu.Unlock()
		return false
	}
	oldID := s.remoteSessionID
	s.remoteSessionID = sessionID
	s.lastSessionChange = now
	if !s.unexpectedRemoteSessions[sessionID] {

		if len(s.unexpectedRemoteSessions) >= maxUnexpectedRemoteSessions {
			s.unexpectedRemoteSessions = make(map[string]bool)
		}
		s.unexpectedRemoteSessions[sessionID] = true
	}
	s.remoteMu.Unlock()

	s.logger.Warn("reliable: remote session changed from ", oldID, " to ", sessionID)
	s.resetForNewRemoteSession()
	return true
}

func (s *ReliableSession) resetForNewRemoteSession() {
	s.recvMu.Lock()
	s.incomingChunks = make(map[int]map[int][]byte)
	s.incomingTotals = make(map[int]int)
	s.incomingSizes = make(map[int]int)
	s.incomingBytes = 0
	s.completedPackets = make(map[int][]byte)
	s.ackDirty = false
	s.nextReceiveSequence = 0
	s.nextACKSequence = 0
	s.recvMu.Unlock()

	s.sendMu.Lock()
	s.pendingPackets = make(map[int]*pendingPacket)
	s.nextSendSequence = 0
	s.sendMu.Unlock()

	select {
	case s.spaceCh <- struct{}{}:
	default:
	}
}

func (s *ReliableSession) processACK(sequence int) {
	s.sendMu.Lock()
	var freed [][]string
	for seq, pkt := range s.pendingPackets {
		if seq <= sequence {
			freed = append(freed, pkt.keys)
			s.bytesAcked.Add(int64(len(pkt.data)))
			delete(s.pendingPackets, seq)
		}
	}
	s.sendMu.Unlock()

	select {
	case s.spaceCh <- struct{}{}:
	default:
	}

	for _, keys := range freed {
		for _, key := range keys {
			s.safeDelete(key)
		}
	}
}

func (s *ReliableSession) sendACKSafe(targetSessionID string, sequence int) {
	if s.cfg.AckDisabled {
		return
	}
	ack, err := s.codec.EncodeACK(targetSessionID, s.ownSessionID, sequence)
	if err != nil {
		return
	}
	key := fmt.Sprintf("a:%s:%s:%d", s.ownSessionID, targetSessionID, sequence)
	if err := s.pipe.SendMessage(s.ctx, ack, key); err != nil {
		s.logger.Warn("reliable: ACK send failed seq ", sequence, ": ", err)
	}
}

func (s *ReliableSession) safeDelete(key string) {
	if err := s.pipe.DeleteMessage(s.ctx, key); err != nil {
		s.logger.Warn("reliable: delete failed for ", key, ": ", err)
	}
}

func (s *ReliableSession) processChunk(pkt *DecodedPacket) {
	seq := pkt.Sequence
	var ready []receivedPacket

	s.recvMu.Lock()

	if seq < s.nextReceiveSequence {
		if seq < s.nextACKSequence && !s.cfg.AckDisabled {
			lastSeen := s.nextACKSequence - 1
			s.markACKDirtyLocked(pkt.SessionID, lastSeen)
		}
		s.recvMu.Unlock()
		return
	}
	if seq > s.nextReceiveSequence+maxReorderDistance {
		s.recvMu.Unlock()
		return
	}
	if _, done := s.completedPackets[seq]; done {
		s.recvMu.Unlock()
		return
	}
	if pkt.TotalChunks <= 0 || pkt.TotalChunks > s.maxChunksPerMessage {
		s.recvMu.Unlock()
		return
	}
	if pkt.ChunkIndex < 0 || pkt.ChunkIndex >= pkt.TotalChunks {
		s.recvMu.Unlock()
		return
	}
	chunks, ok := s.incomingChunks[seq]
	if !ok {
		chunks = make(map[int][]byte)
		s.incomingChunks[seq] = chunks
		s.incomingTotals[seq] = pkt.TotalChunks
	} else if s.incomingTotals[seq] != pkt.TotalChunks {
		s.recvMu.Unlock()
		return
	}
	if _, exists := chunks[pkt.ChunkIndex]; exists {
		s.recvMu.Unlock()
		return
	}
	if len(pkt.Data) > maxAssembledMessageSize ||
		s.incomingSizes[seq]+len(pkt.Data) > maxAssembledMessageSize ||
		s.incomingBytes+len(pkt.Data) > maxBufferedIncomingBytes {
		s.dropIncomingLocked(seq)
		s.recvMu.Unlock()
		return
	}
	chunks[pkt.ChunkIndex] = pkt.Data
	s.incomingSizes[seq] += len(pkt.Data)
	s.incomingBytes += len(pkt.Data)

	if len(chunks) != pkt.TotalChunks {
		s.recvMu.Unlock()
		return
	}

	totalSize := 0
	for i := 0; i < pkt.TotalChunks; i++ {
		chunk, ok := chunks[i]
		if !ok {
			s.dropIncomingLocked(seq)
			s.recvMu.Unlock()
			return
		}
		totalSize += len(chunk)
	}
	if totalSize > maxAssembledMessageSize {
		s.dropIncomingLocked(seq)
		s.recvMu.Unlock()
		return
	}

	full := make([]byte, 0, totalSize)
	for i := 0; i < pkt.TotalChunks; i++ {
		full = append(full, chunks[i]...)
	}
	s.dropIncomingLocked(seq)
	s.completedPackets[seq] = full
	ready = s.flushCompletedLocked(pkt.SessionID)
	s.recvMu.Unlock()

	s.deliverCompleted(ready)
}

func (s *ReliableSession) dropIncomingLocked(seq int) {
	for _, chunk := range s.incomingChunks[seq] {
		s.incomingBytes -= len(chunk)
	}
	if s.incomingBytes < 0 {
		s.incomingBytes = 0
	}
	delete(s.incomingChunks, seq)
	delete(s.incomingTotals, seq)
	delete(s.incomingSizes, seq)
}

func (s *ReliableSession) markACKDirtyLocked(sessionID string, seq int) {
	if s.ackDirty && seq < s.pendingACKSeq {

		return
	}
	s.pendingACKSession = sessionID
	s.pendingACKSeq = seq
	s.ackDirty = true
	s.ackCond.Signal()
}

func (s *ReliableSession) flushCompletedLocked(sessionID string) []receivedPacket {
	var ready []receivedPacket
	for {
		full, ok := s.completedPackets[s.nextReceiveSequence]
		if !ok {
			return ready
		}
		delete(s.completedPackets, s.nextReceiveSequence)
		ready = append(ready, receivedPacket{
			sequence:  s.nextReceiveSequence,
			sessionID: sessionID,
			data:      full,
		})
		s.nextReceiveSequence++
	}
}

func (s *ReliableSession) deliverCompleted(pkts []receivedPacket) {
	if len(pkts) == 0 {
		return
	}
	var done <-chan struct{}
	if s.ctx != nil {
		done = s.ctx.Done()
	}
	for _, pkt := range pkts {
		select {
		case s.receiveQueue <- pkt:
		case <-done:
			return
		}
	}
}

func (s *ReliableSession) handlePipeReset() {
	s.sendMu.Lock()
	for _, p := range s.pendingPackets {
		p.lastSentAt = time.Time{}
	}
	s.sendMu.Unlock()
	select {
	case s.wakeRetry <- struct{}{}:
	default:
	}
}

func (s *ReliableSession) signalTransportReset(reason string) {
	s.logger.Warn("reliable: transport reset: ", reason)

	s.recvMu.Lock()
	s.incomingChunks = make(map[int]map[int][]byte)
	s.incomingTotals = make(map[int]int)
	s.incomingSizes = make(map[int]int)
	s.incomingBytes = 0
	s.completedPackets = make(map[int][]byte)
	s.ackDirty = false
	s.nextReceiveSequence = 0
	s.nextACKSequence = 0
	s.recvMu.Unlock()

	s.sendMu.Lock()
	s.pendingPackets = make(map[int]*pendingPacket)
	s.nextSendSequence = 0
	s.sendMu.Unlock()

	select {
	case s.spaceCh <- struct{}{}:
	default:
	}

	s.remoteMu.Lock()
	s.remoteSessionID = ""
	s.remoteMu.Unlock()

	for {
		select {
		case <-s.receiveQueue:
		default:
			goto push
		}
	}
push:
	select {
	case s.receiveQueue <- &resetMarker{reason: reason}:
	default:
	}
}

func (s *ReliableSession) NotifyNetworkChanged() {
	if n, ok := s.pipe.(interface{ NotifyNetworkChanged() }); ok {
		n.NotifyNetworkChanged()
	}
}

var _ transport.Transport = (*ReliableSession)(nil)
var _ transport.Telemetry = (*ReliableSession)(nil)
