package livekit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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
)

const (
	connectTimeout      = 15 * time.Second
	tokenRequestTimeout = 20 * time.Second
)

type Provider struct {
	cfg    Config
	logger logging.ContextLogger
	ctx    context.Context
	cancel context.CancelFunc

	tokenURL   string
	httpClient *http.Client

	pipeMu sync.Mutex
	pipe   *Pipe

	roomMu   sync.Mutex
	room     *lksdk.Room
	closing  bool
	identity string

	disconnected chan struct{}

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
		tokenURL:     cfg.tokenEndpoint(),
		httpClient:   &http.Client{Timeout: tokenRequestTimeout},
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
				pipe.SetRemoteID(rp.Identity())
			}
		},
		OnParticipantDisconnected: func(rp *lksdk.RemoteParticipant) {
			p.logger.Warn("livekit: remote participant left: ", rp.Identity())
			p.pipeMu.Lock()
			pipe := p.pipe
			p.pipeMu.Unlock()
			if pipe != nil && pipe.GetRemoteID() == rp.Identity() {
				pipe.SetRemoteID("")
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
			select {
			case p.disconnected <- struct{}{}:
			default:
			}
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
	p.closing = false
	p.roomMu.Unlock()

	p.pipeMu.Lock()
	if p.pipe == nil {
		p.pipe = newPipe(room.LocalParticipant, p.cfg.DataTopic, "")
	} else {
		p.pipe.UpdateLocal(room.LocalParticipant)
	}
	pipe := p.pipe
	p.pipeMu.Unlock()

	pipe.SetRemoteID("")
	if rps := room.GetRemoteParticipants(); len(rps) > 0 {
		pipe.SetRemoteID(rps[0].Identity())
	}

	p.logger.Info("livekit: room=", roomName,
		" identity=", p.identity,
		" remote=", pipe.GetRemoteID(),
		" topic=", p.cfg.DataTopic)

	return nil
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

		delay := time.Second
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
				if delay > 30*time.Second {
					delay = 30 * time.Second
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

	if p.tokenURL != "" {
		resp, reqErr := p.requestSession()
		if reqErr != nil {
			return "", "", "", reqErr
		}
		if strings.TrimSpace(resp.URL) != "" {
			signalURL = resp.URL
		}
		return resp.Token, signalURL, resp.Room, nil
	}

	if strings.TrimSpace(p.cfg.Token) != "" {
		return p.cfg.Token, signalURL, room, nil
	}

	token, err = makeToken(p.cfg.APIKey, p.cfg.APISecret, room, p.identity)
	if err != nil {
		return "", "", "", fmt.Errorf("livekit: make token: %w", err)
	}
	return token, signalURL, room, nil
}

func (p *Provider) requestSession() (sessionResponse, error) {
	if p.tokenURL == "" {
		return sessionResponse{}, errors.New("livekit: token endpoint is not configured")
	}
	payload, err := json.Marshal(sessionRequest{
		UserID:     p.cfg.UserID,
		ExitNodeID: p.cfg.ExitNodeID,
	})
	if err != nil {
		return sessionResponse{}, fmt.Errorf("livekit: encode session request: %w", err)
	}

	reqCtx := p.ctx
	if reqCtx == nil {
		reqCtx = context.Background()
	}
	reqCtx, cancel := context.WithTimeout(reqCtx, tokenRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, p.tokenURL, bytes.NewReader(payload))
	if err != nil {
		return sessionResponse{}, fmt.Errorf("livekit: session request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if secret := strings.TrimSpace(p.cfg.TokenSecret); secret != "" {
		req.Header.Set("X-Shampoore-Secret", secret)
	}

	client := p.httpClient
	if client == nil {
		client = &http.Client{Timeout: tokenRequestTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return sessionResponse{}, fmt.Errorf("livekit: session request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return sessionResponse{}, fmt.Errorf("livekit: session endpoint returned %s: %s",
			resp.Status, strings.TrimSpace(string(msg)))
	}
	var out sessionResponse
	dec := json.NewDecoder(io.LimitReader(resp.Body, 64<<10))
	if err := dec.Decode(&out); err != nil {
		return sessionResponse{}, fmt.Errorf("livekit: decode session response: %w", err)
	}
	if strings.TrimSpace(out.Token) == "" || strings.TrimSpace(out.Room) == "" {
		return sessionResponse{}, errors.New("livekit: session endpoint returned an empty token or room")
	}
	if strings.TrimSpace(out.URL) == "" {
		out.URL = p.cfg.SignalURL
	}
	return out, nil
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
	select {
	case p.disconnected <- struct{}{}:
	default:
	}
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
	if p.tokenURL != "" {
		add(p.tokenURL)
	}
	return bypass
}

func (p *Provider) ConnectedIPs() []string { return nil }

func (p *Provider) ConnectedIPsFunc() func() []string { return p.ConnectedIPs }
