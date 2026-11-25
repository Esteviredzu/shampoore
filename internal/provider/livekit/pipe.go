package livekit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	maxPacketSize = 12_000
	recvQueueSize = 4096

	recvOverflowWait = 50 * time.Millisecond

	resetDedupWindow = 250 * time.Millisecond
)

var (
	heartbeatPayload = []byte("LKHB\x00\x01\x02\x03")

	pipeHeartbeatInterval = 5 * time.Second
	pipeHeartbeatTimeout  = 20 * time.Second
)

func isHeartbeatPayload(data []byte) bool {
	return len(data) == len(heartbeatPayload) && bytes.Equal(data, heartbeatPayload)
}

type Pipe struct {
	localMu  sync.RWMutex
	local    *lksdk.LocalParticipant
	topic    string
	remoteID atomic.Pointer[string]

	recvCh  chan []byte
	resetCh chan error

	lastRecv      atomic.Int64
	peerHeartbeat atomic.Bool

	lastSentNs atomic.Int64

	peerPresent atomic.Bool

	adoptFirst atomic.Bool
	deadFired  atomic.Bool

	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration

	deadMu sync.Mutex
	onDead func()

	onPublishError func(error)

	resetMu   sync.Mutex
	lastReset time.Time

	ctxMu     sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	started   atomic.Bool
	closeOnce sync.Once
	closed    chan struct{}

	bytesSent atomic.Int64
	bytesRecv atomic.Int64
	dropped   atomic.Int64

	ctr transport.Counters
}

func newPipe(
	local *lksdk.LocalParticipant,
	topic string,
	remoteID string,
) *Pipe {
	p := &Pipe{
		local:             local,
		topic:             topic,
		recvCh:            make(chan []byte, recvQueueSize),
		resetCh:           make(chan error, 1),
		closed:            make(chan struct{}),
		heartbeatInterval: pipeHeartbeatInterval,
		heartbeatTimeout:  pipeHeartbeatTimeout,
	}
	p.lastRecv.Store(time.Now().UnixNano())
	if remoteID != "" {
		p.SetRemoteID(remoteID)
	}
	return p
}

func (p *Pipe) SetDeadHandler(fn func()) {
	p.deadMu.Lock()
	p.onDead = fn
	p.deadMu.Unlock()
}

func (p *Pipe) SetPublishErrorHandler(fn func(error)) {
	p.deadMu.Lock()
	p.onPublishError = fn
	p.deadMu.Unlock()
}

func (p *Pipe) reportPublishError(err error) {
	if err == nil {
		return
	}
	p.deadMu.Lock()
	fn := p.onPublishError
	p.deadMu.Unlock()
	if fn != nil {
		fn(err)
	}
}

func (p *Pipe) ResetLiveness() {
	p.lastRecv.Store(time.Now().UnixNano())
	p.deadFired.Store(false)
}

func (p *Pipe) noteIncoming() {
	p.lastRecv.Store(time.Now().UnixNano())
}

func (p *Pipe) LastPublishOK() time.Time {
	ns := p.lastSentNs.Load()
	if ns <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (p *Pipe) PeerJoined(id string) bool {
	previous := p.GetRemoteID()
	p.SetRemoteID(id)
	p.peerPresent.Store(true)
	p.ResetLiveness()
	return previous == ""
}

func (p *Pipe) PeerLeft() {
	p.SetRemoteID("")
	p.peerHeartbeat.Store(false)
	p.peerPresent.Store(false)
	p.ResetLiveness()
}

func (p *Pipe) PeerPresent() bool { return p.peerPresent.Load() }

func (p *Pipe) SetAdoptFirstPacket(enable bool) { p.adoptFirst.Store(enable) }

func (p *Pipe) SignalReset(err error) {
	if err == nil {
		err = io.EOF
	}

	p.resetMu.Lock()
	if len(p.resetCh) > 0 {
		p.resetMu.Unlock()
		return
	}
	if !p.lastReset.IsZero() && time.Since(p.lastReset) < resetDedupWindow {
		p.resetMu.Unlock()
		return
	}
	p.lastReset = time.Now()
	p.resetMu.Unlock()

	p.DrainIncoming()
	select {
	case p.resetCh <- err:
	default:
	}
}

func (p *Pipe) DrainIncoming() {
	for {
		select {
		case <-p.recvCh:
		default:
			return
		}
	}
}

func (p *Pipe) UpdateLocal(l *lksdk.LocalParticipant) {
	p.localMu.Lock()
	p.local = l
	p.localMu.Unlock()
}

func (p *Pipe) SetRemoteID(id string) { p.remoteID.Store(&id) }

func (p *Pipe) GetRemoteID() string {
	v := p.remoteID.Load()
	if v == nil {
		return ""
	}
	return *v
}

func (p *Pipe) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return nil
	}
	p.ctxMu.Lock()
	p.ctx, p.cancel = context.WithCancel(ctx)
	runCtx := p.ctx
	p.ctxMu.Unlock()
	p.ResetLiveness()
	go p.heartbeatLoop(runCtx)
	return nil
}

func (p *Pipe) heartbeatLoop(ctx context.Context) {
	interval := p.heartbeatInterval
	if interval <= 0 {
		interval = pipeHeartbeatInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.closed:
			return
		case <-ticker.C:
		}

		p.sendHeartbeat()

		timeout := p.heartbeatTimeout
		if timeout <= 0 {
			timeout = pipeHeartbeatTimeout
		}

		if !p.peerPresent.Load() {

			continue
		}
		if time.Since(time.Unix(0, p.lastRecv.Load())) >= timeout {
			p.fireDead()
		}
	}
}

func (p *Pipe) sendHeartbeat() {
	rid := p.GetRemoteID()
	p.localMu.RLock()
	local := p.local
	p.localMu.RUnlock()
	if local == nil {
		return
	}
	opts := []lksdk.DataPublishOption{
		lksdk.WithDataPublishReliable(true),
		lksdk.WithDataPublishTopic(p.topic),
	}
	if rid != "" {
		opts = append(opts, lksdk.WithDataPublishDestination([]string{rid}))
	}

	_ = local.PublishDataPacket(lksdk.UserData(heartbeatPayload), opts...)
}

func (p *Pipe) fireDead() {
	if p.deadFired.Swap(true) {
		return
	}
	p.deadMu.Lock()
	fn := p.onDead
	p.deadMu.Unlock()
	if fn != nil {
		fn()
	}
}

func (p *Pipe) PushIncoming(data []byte, senderIdentity string) {
	rid := p.GetRemoteID()
	if rid != "" && senderIdentity != "" && senderIdentity != rid {
		if p.peerPresent.Load() || !p.adoptFirst.Load() {

			return
		}

		rid = senderIdentity
		p.SetRemoteID(senderIdentity)
	}

	if rid == "" && senderIdentity != "" {
		p.SetRemoteID(senderIdentity)
	}
	select {
	case <-p.closed:
		return
	default:
	}

	p.peerPresent.Store(true)
	p.noteIncoming()
	if isHeartbeatPayload(data) {
		p.peerHeartbeat.Store(true)
		return
	}

	select {
	case p.recvCh <- data:
		n := int64(len(data))
		p.ctr.UsefulRecv.Add(n)
		p.ctr.NetworkRecv.Add(n)
		p.ctr.MessagesRecv.Add(1)
		p.ctr.FramesRecv.Add(1)
	default:
		p.pushWithBackpressure(data)
	}
}

func (p *Pipe) pushWithBackpressure(data []byte) {
	timer := time.NewTimer(recvOverflowWait)
	defer timer.Stop()

	select {
	case p.recvCh <- data:
		n := int64(len(data))
		p.ctr.UsefulRecv.Add(n)
		p.ctr.NetworkRecv.Add(n)
		p.ctr.MessagesRecv.Add(1)
		p.ctr.FramesRecv.Add(1)
	case <-p.closed:
	case <-timer.C:
		p.dropped.Add(1)
		p.SignalReset(fmt.Errorf("%w: livekit: receive queue overflow", transport.ErrTransportReset))
	}
}

func (p *Pipe) Dropped() int64 { return p.dropped.Load() }

func (p *Pipe) SendMessage(ctx context.Context, data []byte, _ string) error {
	if len(data) > maxPacketSize {
		return errors.New("livekit: message exceeds max packet size")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		return io.EOF
	default:
	}

	p.localMu.RLock()
	local := p.local
	p.localMu.RUnlock()
	if local == nil {
		return errors.New("livekit: not connected")
	}

	opts := []lksdk.DataPublishOption{
		lksdk.WithDataPublishReliable(true),
		lksdk.WithDataPublishTopic(p.topic),
	}
	if rid := p.GetRemoteID(); rid != "" {
		opts = append(opts, lksdk.WithDataPublishDestination([]string{rid}))
	}

	if err := local.PublishDataPacket(lksdk.UserData(data), opts...); err != nil {
		p.reportPublishError(err)
		return err
	}
	p.lastSentNs.Store(time.Now().UnixNano())
	n := int64(len(data))
	p.bytesSent.Add(n)
	p.ctr.UsefulSent.Add(n)
	p.ctr.NetworkSent.Add(n)
	p.ctr.MessagesSent.Add(1)
	p.ctr.FramesSent.Add(1)
	return nil
}

func (p *Pipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closed:
		return nil, io.EOF
	case err := <-p.resetCh:
		return nil, err
	case data := <-p.recvCh:
		p.bytesRecv.Add(int64(len(data)))
		return data, nil
	}
}

func (p *Pipe) DeleteMessage(context.Context, string) error { return nil }
func (p *Pipe) SessionID() string                           { return p.topic }
func (p *Pipe) MaxMessageSize() int                         { return maxPacketSize }
func (p *Pipe) BytesSent() int64                            { return p.bytesSent.Load() }
func (p *Pipe) BytesRecv() int64                            { return p.bytesRecv.Load() }

func (p *Pipe) WalkLayers() []transport.Layer {
	return []transport.Layer{{Kind: "pipe", Type: "livekit", Snapshot: p.ctr.Snapshot()}}
}

func (p *Pipe) Stop(_ context.Context) error {
	p.closeOnce.Do(func() {
		p.ctxMu.Lock()
		cancel := p.cancel
		p.ctxMu.Unlock()
		if cancel != nil {
			cancel()
		}
		close(p.closed)
	})
	return nil
}

var _ transport.Pipe = (*Pipe)(nil)
var _ transport.Telemetry = (*Pipe)(nil)
