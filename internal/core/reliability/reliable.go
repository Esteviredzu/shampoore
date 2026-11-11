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

	"github.com/Esteviredzu/shampoore-core/internal/core/security"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

const (
	initialRetryDelay  = 1 * time.Second
	maxRetryDelay      = 10 * time.Second
	retryScanInterval  = 100 * time.Millisecond
	maxReorderDistance = 256
	defaultWindow      = 64

	sendWindowTimeout = 10 * time.Second
)

type Config struct {
	Window             int
	ChunkSize          int
	AckBatchMs         int
	AckDisabled        bool
	EncryptionPassword string
}

func DefaultConfig() Config {
	return Config{Window: defaultWindow}
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
	unexpectedRemoteSessions map[string]bool

	sendMu           sync.Mutex
	pendingPackets   map[int]*pendingPacket
	nextSendSequence int
	spaceCh          chan struct{}

	wakeRetry chan struct{}

	recvMu              sync.Mutex
	nextReceiveSequence int
	nextACKSequence     int
	incomingChunks      map[int]map[int][]byte
	incomingTotals      map[int]int
	completedPackets    map[int][]byte

	pendingACKSession string
	pendingACKSeq     int
	ackDirty          bool
	ackCond           *sync.Cond

	receiveQueue chan any

	bytesDelivered atomic.Int64
	bytesAcked     atomic.Int64

	ctr transport.Counters

	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	closed  bool
	wg      sync.WaitGroup
	ackWg   sync.WaitGroup
}

func NewReliableSession(pipe transport.Pipe, log logging.ContextLogger, cfg Config) (*ReliableSession, error) {
	if cfg.Window <= 0 {
		cfg.Window = defaultWindow
	}

	if cfg.EncryptionPassword != "" {
		wrapped, err := security.WrapPipe(pipe, cfg.EncryptionPassword, log)
		if err != nil {
			return nil, err
		}
		pipe = wrapped
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

	s := &ReliableSession{
		pipe:                     pipe,
		codec:                    codec,
		logger:                   log,
		cfg:                      cfg,
		unexpectedRemoteSessions: make(map[string]bool),
		pendingPackets:           make(map[int]*pendingPacket),
		spaceCh:                  make(chan struct{}, 1),
		incomingChunks:           make(map[int]map[int][]byte),
		incomingTotals:           make(map[int]int),
		completedPackets:         make(map[int][]byte),
		receiveQueue:             make(chan any, 512),
		wakeRetry:                make(chan struct{}, 1),
	}
	s.ackCond = sync.NewCond(&s.recvMu)

	if cfg.AckDisabled {
		log.Warn("reliable: ACK disabled")
	}
	if cfg.AckBatchMs > 0 {
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
	if s.started {
		return nil
	}
	if err := s.pipe.Start(ctx); err != nil {
		return err
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.ownSessionID = uuid.NewString()
	s.nextSendSequence = 0
	s.nextReceiveSequence = 0
	s.nextACKSequence = 0
	s.started = true

	s.wg.Add(2)
	go s.receiveLoop()
	go s.retryLoop()
	if s.cfg.AckBatchMs > 0 && !s.cfg.AckDisabled {
		s.wg.Add(1)
		go s.ackFlushLoop()
	}

	s.logger.Info("reliable: started; session=", s.ownSessionID,
		" window=", s.cfg.Window,
		" ack_disabled=", s.cfg.AckDisabled,
		" ack_batch_ms=", s.cfg.AckBatchMs)
	return nil
}

func (s *ReliableSession) Stop(ctx context.Context) error {
	s.sendMu.Lock()
	if s.closed {
		s.sendMu.Unlock()
		return nil
	}
	s.closed = true
	s.sendMu.Unlock()

	s.recvMu.Lock()
	s.ackCond.Broadcast()
	s.recvMu.Unlock()

	if s.cancel != nil {
		s.cancel()
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
			if v.sequence != s.nextACKSequence {
				return nil, fmt.Errorf(
					"receive sequence mismatch: expected %d, got %d",
					s.nextACKSequence, v.sequence)
			}
			s.nextACKSequence++

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

		pkt := s.codec.Decode(raw)
		if pkt == nil {
			continue
		}
		if err := s.dispatch(pkt); err != nil {
			s.logger.Warn("reliable: dispatch error: ", err)
			s.signalTransportReset(err.Error())
			return
		}
	}
}

func (s *ReliableSession) dispatch(pkt *DecodedPacket) error {
	if pkt.Kind == "data" {
		if pkt.SessionID == s.ownSessionID {
			return nil
		}
		key := fmt.Sprintf("d:%s:%d:%d",
			pkt.SessionID, pkt.Sequence, pkt.ChunkIndex)
		err := s.acceptRemoteSession(pkt.SessionID)
		if err == nil {
			s.processChunk(pkt)
		}
		s.safeDelete(key)
		return err
	}

	if pkt.Kind == "ack" {
		if pkt.SessionID != s.ownSessionID {
			return nil
		}
		key := fmt.Sprintf("a:%s:%s:%d",
			pkt.SenderSessionID, pkt.SessionID, pkt.Sequence)
		err := s.acceptRemoteSession(pkt.SenderSessionID)
		if err == nil {
			s.processACK(pkt.Sequence)
		}
		s.safeDelete(key)
		return err
	}
	return nil
}

func (s *ReliableSession) acceptRemoteSession(sessionID string) error {
	s.remoteMu.Lock()
	defer s.remoteMu.Unlock()
	if s.remoteSessionID == "" {
		s.remoteSessionID = sessionID
		return nil
	}
	if sessionID == s.remoteSessionID {
		return nil
	}
	if !s.unexpectedRemoteSessions[sessionID] {
		s.unexpectedRemoteSessions[sessionID] = true
		s.logger.Warn("reliable: remote session changed")
	}
	return fmt.Errorf("remote session changed to %s", sessionID)
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
	s.recvMu.Lock()

	if seq < s.nextReceiveSequence {
		if seq < s.nextACKSequence && !s.cfg.AckDisabled {
			lastSeen := s.nextACKSequence - 1
			if s.cfg.AckBatchMs > 0 {
				s.markACKDirtyLocked(pkt.SessionID, lastSeen)
			} else {
				s.ackWg.Add(1)
				go func(sid string, sq int) {
					defer s.ackWg.Done()
					s.sendACKSafe(sid, sq)
				}(pkt.SessionID, lastSeen)
			}
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
	chunks[pkt.ChunkIndex] = pkt.Data
	if len(chunks) != pkt.TotalChunks {
		s.recvMu.Unlock()
		return
	}
	full := make([]byte, 0)
	for i := 0; i < pkt.TotalChunks; i++ {
		full = append(full, chunks[i]...)
	}
	delete(s.incomingChunks, seq)
	delete(s.incomingTotals, seq)
	s.completedPackets[seq] = full
	s.flushCompletedLocked(pkt.SessionID)
	s.recvMu.Unlock()
}

func (s *ReliableSession) markACKDirtyLocked(sessionID string, seq int) {
	s.pendingACKSession = sessionID
	s.pendingACKSeq = seq
	s.ackDirty = true
	s.ackCond.Signal()
}

func (s *ReliableSession) flushCompletedLocked(sessionID string) {
	for {
		full, ok := s.completedPackets[s.nextReceiveSequence]
		if !ok {
			return
		}
		delete(s.completedPackets, s.nextReceiveSequence)
		pkt := receivedPacket{
			sequence:  s.nextReceiveSequence,
			sessionID: sessionID,
			data:      full,
		}
		select {
		case s.receiveQueue <- pkt:
		default:
			s.receiveQueue <- pkt
		}
		s.nextReceiveSequence++
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
	s.completedPackets = make(map[int][]byte)
	s.ackDirty = false
	s.recvMu.Unlock()

	s.sendMu.Lock()
	s.pendingPackets = make(map[int]*pendingPacket)
	s.sendMu.Unlock()

	select {
	case s.spaceCh <- struct{}{}:
	default:
	}

	s.remoteMu.Lock()
	s.remoteSessionID = ""
	s.remoteMu.Unlock()

	s.nextSendSequence = 0
	s.nextReceiveSequence = 0
	s.nextACKSequence = 0

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

var _ transport.Transport = (*ReliableSession)(nil)
var _ transport.Telemetry = (*ReliableSession)(nil)
