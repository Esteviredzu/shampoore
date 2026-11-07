package livekit

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/livekit/protocol/auth"
	lksdk "github.com/livekit/server-sdk-go/v2"

	"github.com/Esteviredzu/shampoore-core/internal/core/channel/vp8"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
)

type Provider struct {
	cfg    Config
	room   *lksdk.Room
	pipe   *Pipe
	logger logging.ContextLogger
	ctx    context.Context
	cancel context.CancelFunc

	pipeMu sync.Mutex
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
		identity = uuid.NewString()
	}

	token, err := makeToken(cfg.APIKey, cfg.APISecret, cfg.Room, identity)
	if err != nil {
		return nil, fmt.Errorf("livekit: make token: %w", err)
	}

	p := &Provider{
		cfg:    cfg,
		logger: logger,
	}

	participantCh := make(chan string, 1)

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
					if pkt.Topic == cfg.DataTopic {
						pipe.PushIncoming(pkt.Payload, params.SenderIdentity)
					}
				}
			},
		},
		OnParticipantConnected: func(rp *lksdk.RemoteParticipant) {
			p.pipeMu.Lock()
			pipe := p.pipe
			p.pipeMu.Unlock()
			if pipe != nil {

				pipe.SetRemoteID(rp.Identity())
			}
			select {
			case participantCh <- rp.Identity():
			default:
			}
		},
	}

	room, err := lksdk.ConnectToRoomWithToken(cfg.SignalURL, token, cb)
	if err != nil {
		return nil, fmt.Errorf("livekit: connect: %w", err)
	}

	p.room = room
	p.ctx, p.cancel = context.WithCancel(ctx)

	p.pipeMu.Lock()
	p.pipe = newPipe(room.LocalParticipant, cfg.DataTopic, "")
	p.pipeMu.Unlock()

	already := room.GetRemoteParticipants()
	if len(already) > 0 {
		p.pipe.SetRemoteID(already[0].Identity())
	} else {
		timeout := 15 * time.Second
		select {
		case id := <-participantCh:
			p.pipe.SetRemoteID(id)
		case <-ctx.Done():
			room.Disconnect()
			p.cancel()
			return nil, ctx.Err()
		case <-time.After(timeout):
			room.Disconnect()
			p.cancel()
			return nil, fmt.Errorf("livekit: timeout waiting for remote participant")
		}
	}

	logger.Info("livekit: room=", cfg.Room,
		" identity=", identity,
		" remote=", p.pipe.GetRemoteID(),
		" topic=", cfg.DataTopic)

	return p, nil
}

func makeToken(apiKey, apiSecret, room, identity string) (string, error) {
	at := auth.NewAccessToken(apiKey, apiSecret)
	grant := &auth.VideoGrant{RoomJoin: true, Room: room}
	at.SetVideoGrant(grant).SetIdentity(identity).SetValidFor(time.Hour)
	return at.ToJWT()
}

func (p *Provider) Pipe() *Pipe { return p.pipe }

func (p *Provider) Start(context.Context) error { return nil }

func (p *Provider) Stop(context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.room != nil {
		p.room.Disconnect()
	}
	return nil
}

func (p *Provider) Tunnel() *vp8.Tunnel { return nil }

func (p *Provider) BypassHosts() []string {
	if u, err := url.Parse(p.cfg.SignalURL); err == nil && u.Hostname() != "" {
		return []string{u.Hostname()}
	}
	return nil
}

func (p *Provider) ConnectedIPs() []string { return nil }

func (p *Provider) ConnectedIPsFunc() func() []string { return p.ConnectedIPs }
