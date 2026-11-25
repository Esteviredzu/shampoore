package livekit

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/livekit/server-sdk-go/v2"

	"github.com/Esteviredzu/shampoore/internal/core/channel/vp8"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/names"
	"github.com/Esteviredzu/shampoore/internal/netdial"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	connectTimeout = 5 * time.Second

	reconnectBaseDelay = 500 * time.Millisecond
	reconnectMaxDelay  = 2 * time.Second
)

type Provider struct {
	cfg    Config
	logger logging.ContextLogger
	ctx    context.Context
	cancel context.CancelFunc

	pipeMu sync.Mutex
	pipe   *Pipe

	roomMu   sync.Mutex
	room     *lksdk.Room
	closing  bool
	identity string

	disconnected chan struct{}

	publishMu      sync.Mutex
	publishTracker publishFailureTracker

	started  atomic.Bool
	stopped  atomic.Bool
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewProvider(
	ctx context.Context,
	cfg Config,
	logger logging.ContextLogger,
) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	identity := cfg.Identity
	if identity == "" {
		identity = strings.TrimSpace(cfg.UserID)
	}
	if identity == "" {
		identity = names.Random()
	}

	p := &Provider{
		cfg:          cfg,
		logger:       logger,
		identity:     identity,
		disconnected: make(chan struct{}, 1),
	}
	p.ctx, p.cancel = context.WithCancel(ctx)

	if err := p.start(ctx); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Provider) start(ctx context.Context) error {
	if p.stopped.Load() {
		return errors.New("livekit: provider is stopped")
	}
	if !p.started.CompareAndSwap(false, true) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		p.started.Store(false)
		return err
	}
	if err := p.connect(); err != nil {
		if stopErr := p.Stop(context.Background()); stopErr != nil {
			p.logger.Warn("livekit: cleanup after failed start: ", stopErr)
		}
		return err
	}

	p.wg.Add(1)
	go p.supervise()
	return nil
}

func (p *Provider) connect() error {
	token, signalURL, roomName, err := p.connectionToken()
	if err != nil {
		return err
	}

	cb := &lksdk.RoomCallback{
		ParticipantCallback: lksdk.ParticipantCallback{
			OnDataPacket: func(data lksdk.DataPacket, params lksdk.DataReceiveParams) {
				p.pipeMu.Lock()
				pipe := p.pipe
				p.pipeMu.Unlock()
				if pipe == nil {
					return
				}
				switch pkt := data.(type) {
				case *lksdk.UserDataPacket:
					if pkt.Topic == p.cfg.DataTopic {
						pipe.PushIncoming(pkt.Payload, params.SenderIdentity)
					}
				}
			},
		},
		OnParticipantConnected: func(rp *lksdk.RemoteParticipant) {
			p.logger.Info("livekit: remote participant joined: ", rp.Identity())
			p.pipeMu.Lock()
			pipe := p.pipe
			p.pipeMu.Unlock()
			if pipe != nil {
				if pipe.PeerJoined(rp.Identity()) {

					pipe.SignalReset(fmt.Errorf("%w: livekit: peer joined", transport.ErrTransportReset))
				}
			}
		},
		OnParticipantDisconnected: func(rp *lksdk.RemoteParticipant) {
			identity := ""
			if rp != nil {
				identity = rp.Identity()
			}
			if identity != "" {
				p.logger.Warn("livekit: remote participant left: ", identity)
			} else {
				p.logger.Warn("livekit: remote participant left")
			}
			p.pipeMu.Lock()
			pipe := p.pipe
			p.pipeMu.Unlock()
			if pipe == nil {
				return
			}

			if rid := pipe.GetRemoteID(); rid == "" || identity == "" || rid == identity {
				pipe.PeerLeft()
				pipe.SignalReset(fmt.Errorf("%w: livekit: peer left", transport.ErrTransportReset))
			}
		},
		OnDisconnected: func() {
			p.roomMu.Lock()
			closing := p.closing
			p.roomMu.Unlock()
			if closing {
				return
			}
			p.logger.Warn("livekit: room disconnected unexpectedly")
			p.signalDisconnected()
		},
	}

	room, err := lksdk.ConnectToRoomWithToken(
		signalURL,
		token,
		cb,
		lksdk.WithConnectTimeout(connectTimeout),
	)
	if err != nil {
		return fmt.Errorf("livekit: connect: %w", err)
	}

	if p.stopped.Load() {
		room.Disconnect()
		return errors.New("livekit: provider stopped during connect")
	}

	p.roomMu.Lock()
	p.room = room

	p.roomMu.Unlock()

	p.pipeMu.Lock()
	reused := p.pipe != nil
	if p.pipe == nil {
		p.pipe = newPipe(room.LocalParticipant, p.cfg.DataTopic, "")
	} else {
		p.pipe.UpdateLocal(room.LocalParticipant)
	}
	pipe := p.pipe
	p.pipeMu.Unlock()

	pipe.SetDeadHandler(func() {
		p.requestReconnect("peer heartbeat lost")
	})
	pipe.SetPublishErrorHandler(p.notePublishError)

	pipe.PeerLeft()
	pipe.SetAdoptFirstPacket(true)
	if rps := room.GetRemoteParticipants(); len(rps) > 0 {
		pipe.SetRemoteID(rps[0].Identity())
	}
	if reused {
		pipe.SignalReset(fmt.Errorf("%w: livekit: room reconnected", transport.ErrTransportReset))
	}

	p.roomMu.Lock()
	p.closing = false
	p.roomMu.Unlock()

	p.logger.Info("livekit: room=", roomName,
		" identity=", p.identity,
		" remote=", pipe.GetRemoteID(),
		" topic=", p.cfg.DataTopic)

	return nil
}

const publishReconnectGrace = 3 * time.Second

type publishFailureTracker struct {
	grace time.Duration
	first time.Time
}

func (t *publishFailureTracker) observeFailure(now, lastOK time.Time) bool {
	grace := t.grace
	if grace <= 0 {
		grace = publishReconnectGrace
	}
	if !lastOK.IsZero() && now.Sub(lastOK) < grace {
		t.first = time.Time{}
		return false
	}
	if t.first.IsZero() {
		t.first = now
		return false
	}
	if now.Sub(t.first) < grace {
		return false
	}
	t.first = time.Time{}
	return true
}

func (p *Provider) notePublishError(err error) {
	if p.stopped.Load() {
		return
	}
	var lastOK time.Time
	if pipe := p.Pipe(); pipe != nil {
		lastOK = pipe.LastPublishOK()
	}
	p.publishMu.Lock()
	reconnect := p.publishTracker.observeFailure(time.Now(), lastOK)
	p.publishMu.Unlock()
	if !reconnect {
		if p.logger != nil {
			p.logger.Warn("livekit: datachannel publish failed (keeping the room): ", err)
		}
		return
	}
	p.requestReconnect("datachannel publish failed")
}

func (p *Provider) signalDisconnected() {
	select {
	case p.disconnected <- struct{}{}:
	default:
	}
}

func (p *Provider) requestReconnect(reason string) {
	if p.stopped.Load() {
		return
	}
	p.roomMu.Lock()
	busy := p.closing || p.room == nil
	p.roomMu.Unlock()
	if busy {
		return
	}
	if p.logger != nil && reason != "" {
		p.logger.Warn("livekit: ", reason, "; reconnecting")
	}
	p.signalDisconnected()
}

func (p *Provider) supervise() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.disconnected:
		}

		if p.ctx.Err() != nil {
			return
		}

		p.logger.Info("livekit: reconnecting...")

		p.roomMu.Lock()
		p.closing = true
		oldRoom := p.room
		p.room = nil
		p.roomMu.Unlock()
		if oldRoom != nil {
			oldRoom.Disconnect()
		}

		delay := reconnectBaseDelay
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(delay):
			}

			if p.ctx.Err() != nil {
				return
			}

			if err := p.connect(); err != nil {
				p.logger.Warn("livekit: reconnect failed: ", err)
				delay *= 2
				if delay > reconnectMaxDelay {
					delay = reconnectMaxDelay
				}
				continue
			}
			p.logger.Info("livekit: reconnected")
			break
		}
	}
}

func (p *Provider) connectionToken() (token, signalURL, room string, err error) {
	signalURL = p.cfg.SignalURL
	room = p.cfg.Room

	if strings.TrimSpace(p.cfg.Token) != "" {
		return p.cfg.Token, signalURL, room, nil
	}

	token, err = makeToken(p.cfg.APIKey, p.cfg.APISecret, room, p.identity)
	if err != nil {
		return "", "", "", fmt.Errorf("livekit: make token: %w", err)
	}
	return token, signalURL, room, nil
}

func makeToken(apiKey, apiSecret, room, identity string) (string, error) {
	return makeTokenFor(apiKey, apiSecret, room, identity, 24*time.Hour)
}

func (p *Provider) Pipe() *Pipe {
	p.pipeMu.Lock()
	defer p.pipeMu.Unlock()
	return p.pipe
}

func (p *Provider) NotifyNetworkChanged() {
	if p.stopped.Load() {
		return
	}
	p.roomMu.Lock()
	busy := p.closing || p.room == nil
	p.roomMu.Unlock()
	if busy {

		return
	}
	p.signalDisconnected()
}

func (p *Provider) Start(ctx context.Context) error { return p.start(ctx) }

func (p *Provider) Stop(ctx context.Context) error {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		if p.cancel != nil {
			p.cancel()
		}

		p.pipeMu.Lock()
		pipe := p.pipe
		p.pipeMu.Unlock()
		if pipe != nil {
			if err := pipe.Stop(ctx); err != nil {
				p.logger.Warn("livekit: stop pipe: ", err)
			}
		}

		p.roomMu.Lock()
		p.closing = true
		room := p.room
		p.room = nil
		p.roomMu.Unlock()
		if room != nil {
			room.Disconnect()
		}

		p.wg.Wait()
	})
	return nil
}

func (p *Provider) Tunnel() *vp8.Tunnel { return nil }

func (p *Provider) BypassHosts() []string {
	seen := make(map[string]struct{})
	var bypass []string
	add := func(raw string) {
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" {
			return
		}
		host := u.Hostname()
		for _, item := range append([]string{host}, netdial.ResolveIPv4(context.Background(), host)...) {
			if _, ok := seen[item]; ok || item == "" {
				continue
			}
			seen[item] = struct{}{}
			bypass = append(bypass, item)
		}
	}
	add(p.cfg.SignalURL)
	return bypass
}

func (p *Provider) ConnectedIPs() []string { return nil }

func (p *Provider) ConnectedIPsFunc() func() []string { return p.ConnectedIPs }
