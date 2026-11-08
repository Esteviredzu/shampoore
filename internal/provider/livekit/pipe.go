package livekit

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

const (
	maxPacketSize = 12_000
	recvQueueSize = 4096
)

type Pipe struct {
	localMu  sync.RWMutex
	local    *lksdk.LocalParticipant
	topic    string
	remoteID atomic.Pointer[string]

	recvCh chan []byte

	ctx       context.Context
	cancel    context.CancelFunc
	started   atomic.Bool
	closeOnce sync.Once
	closed    chan struct{}

	bytesSent atomic.Int64
	bytesRecv atomic.Int64

	ctr transport.Counters
}

func newPipe(
	local *lksdk.LocalParticipant,
	topic string,
	remoteID string,
) *Pipe {
	p := &Pipe{
		local:  local,
		topic:  topic,
		recvCh: make(chan []byte, recvQueueSize),
		closed: make(chan struct{}),
	}
	if remoteID != "" {
		p.SetRemoteID(remoteID)
	}
	return p
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
	p.ctx, p.cancel = context.WithCancel(ctx)
	return nil
}

func (p *Pipe) PushIncoming(data []byte, senderIdentity string) {
	rid := p.GetRemoteID()
	if rid != "" && senderIdentity != rid {
		return
	}
	select {
	case <-p.closed:
		return
	default:
	}
	select {
	case p.recvCh <- data:
		n := int64(len(data))
		p.ctr.UsefulRecv.Add(n)
		p.ctr.NetworkRecv.Add(n)
		p.ctr.MessagesRecv.Add(1)
		p.ctr.FramesRecv.Add(1)
	case <-p.closed:
	}
}

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
		return err
	}
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
		if p.cancel != nil {
			p.cancel()
		}
		close(p.closed)
	})
	return nil
}

var _ transport.Pipe = (*Pipe)(nil)
var _ transport.Telemetry = (*Pipe)(nil)