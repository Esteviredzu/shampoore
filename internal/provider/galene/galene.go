package galene

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"

	"github.com/Esteviredzu/shampoore-core/internal/core/channel/vp8"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
)

type Provider struct {
	cfg    Config
	sig    *signaling
	tunnel *vp8.Tunnel
	logger logging.ContextLogger

	iceServers []webrtc.ICEServer

	pubPC        *webrtc.PeerConnection
	pubTrack     *webrtc.TrackLocalStaticSample
	pubOfferID   atomic.Value
	pubConnected atomic.Bool

	subPCmu     sync.Mutex
	subPC       *webrtc.PeerConnection
	subRemoteID string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	selfID string
	closed atomic.Bool
}

func NewProvider(ctx context.Context, cfg Config, logger logging.ContextLogger, insecure bool) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	sig := &signaling{cfg: cfg, logger: logger}
	if err := sig.connect(ctx, insecure); err != nil {
		return nil, err
	}

	selfID := uuid.NewString()
	if _, err := sig.handshake(ctx, selfID); err != nil {
		sig.close()
		return nil, fmt.Errorf("galene: handshake: %w", err)
	}

	iceServers, err := sig.join(ctx)
	if err != nil {
		sig.close()
		return nil, fmt.Errorf("galene: join: %w", err)
	}
	if len(iceServers) == 0 {
		sig.close()
		return nil, fmt.Errorf("galene: no TURN servers from join configuration")
	}
	logger.Info("galene: handshake ok, ice_servers=", len(iceServers), " self_id=", selfID)
	logger.Info("galene: joined group=", cfg.Group, " as ", cfg.Username)

	p := &Provider{
		cfg:        cfg,
		sig:        sig,
		logger:     logger,
		selfID:     selfID,
		iceServers: iceServers,
	}
	p.ctx, p.cancel = context.WithCancel(ctx)

	if err := p.setupPubPC(); err != nil {
		p.cancel()
		sig.close()
		return nil, err
	}

	tunnel, err := vp8.AttachTunnel(p.pubPC, nil, p.pubTrack)
	if err != nil {
		_ = p.pubPC.Close()
		p.cancel()
		sig.close()
		return nil, fmt.Errorf("galene: attach tunnel: %w", err)
	}
	p.tunnel = tunnel

	if err := p.sendOffer(); err != nil {
		p.cancel()
		_ = p.pubPC.Close()
		sig.close()
		return nil, err
	}

	p.wg.Add(1)
	go p.msgLoop()

	return p, nil
}

func (p *Provider) setupPubPC() error {
	if p.pubTrack == nil {
		track, err := webrtc.NewTrackLocalStaticSample(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
			"camera", p.cfg.Username,
		)
		if err != nil {
			return fmt.Errorf("galene: new track: %w", err)
		}
		p.pubTrack = track
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers:         p.iceServers,
		ICETransportPolicy: webrtc.ICETransportPolicyRelay,
	})
	if err != nil {
		return fmt.Errorf("galene: new pubPC: %w", err)
	}

	tr, err := pc.AddTransceiverFromTrack(
		p.pubTrack,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly},
	)
	if err != nil {
		_ = pc.Close()
		return fmt.Errorf("galene: add video transceiver: %w", err)
	}
	go drainSenderRTCP(tr.Sender())

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil || p.closed.Load() {
			return
		}
		id, _ := p.pubOfferID.Load().(string)
		if id == "" {
			return
		}
		if err := p.sig.sendICE(id, c.ToJSON()); err != nil {
			p.logger.Debug("galene: send pub ice: ", err)
		}
	})

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		p.logger.Info("galene: pubPC state = ", s.String())
		if s == webrtc.PeerConnectionStateConnected {

			if p.pubConnected.CompareAndSwap(false, true) {
				go p.sendSubscribeRequest()
			}
		}
	})
	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		p.logger.Info("galene: pubPC ice = ", s.String())
	})

	p.pubPC = pc
	return nil
}

func (p *Provider) sendSubscribeRequest() {
	p.logger.Info("galene: requesting media tracks from server...")
	if err := p.sig.sendRequest(); err != nil {
		p.logger.Warn("galene: send request: ", err)
	}
}

func (p *Provider) sendOffer() error {
	pc := p.pubPC
	offerID := uuid.NewString()

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("galene: create offer: %w", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("galene: set local: %w", err)
	}

	p.pubOfferID.Store(offerID)

	if err := p.sig.sendOffer(offerID, "camera", p.selfID, p.cfg.Username, offer.SDP); err != nil {
		return fmt.Errorf("galene: send offer: %w", err)
	}
	p.logger.Info("galene: pub offer sent id=", offerID)
	return nil
}

func (p *Provider) handlePeerOffer(m *inboundMessage) {
	p.subPCmu.Lock()
	defer p.subPCmu.Unlock()

	if p.subPC != nil {
		_ = p.subPC.Close()
		p.subPC = nil
		p.subRemoteID = ""
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers:         p.iceServers,
		ICETransportPolicy: webrtc.ICETransportPolicyRelay,
	})
	if err != nil {
		p.logger.Warn("galene: new subPC: ", err)
		return
	}

	offerID := m.Id
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil || p.closed.Load() {
			return
		}
		if err := p.sig.sendICE(offerID, c.ToJSON()); err != nil {
			p.logger.Debug("galene: send sub ice: ", err)
		}
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		p.logger.Info("galene: subPC state = ", s.String())
	})
	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		p.logger.Info("galene: subPC ice = ", s.String())
	})

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  m.Sdp,
	}); err != nil {
		p.logger.Warn("galene: subPC set remote offer: ", err)
		_ = pc.Close()
		return
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		p.logger.Warn("galene: subPC create answer: ", err)
		_ = pc.Close()
		return
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		p.logger.Warn("galene: subPC set local answer: ", err)
		_ = pc.Close()
		return
	}
	if err := p.sig.sendAnswer(offerID, answer.SDP); err != nil {
		p.logger.Warn("galene: subPC send answer: ", err)
		_ = pc.Close()
		return
	}

	p.subPC = pc
	p.subRemoteID = offerID
	if p.tunnel != nil {
		p.tunnel.AttachSubPC(pc)
	}
	p.logger.Info("galene: subPC answered peer offer id=", offerID)
}

func (p *Provider) msgLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		m, err := p.sig.recv(p.ctx)
		if err != nil {
			if p.ctx.Err() == nil {
				p.logger.Warn("galene: recv: ", err)
			}
			p.closed.Store(true)
			return
		}

		switch m.Type {
		case "answer":
			pubID, _ := p.pubOfferID.Load().(string)
			if m.Id != pubID {
				p.logger.Debug("galene: ignoring answer for unknown id=", m.Id)
				continue
			}
			p.logger.Info("galene: got answer for pub offer id=", m.Id)
			if err := p.pubPC.SetRemoteDescription(webrtc.SessionDescription{
				Type: webrtc.SDPTypeAnswer,
				SDP:  m.Sdp,
			}); err != nil {
				p.logger.Warn("galene: pubPC set answer: ", err)
			}

		case "offer":
			p.logger.Info("galene: got peer offer id=", m.Id,
				" source=", m.Id, " user=", m.Username)
			p.handlePeerOffer(m)

		case "ice":
			if m.Candidate == nil || m.Candidate.Candidate == "" {
				continue
			}
			pubID, _ := p.pubOfferID.Load().(string)
			if m.Id == pubID {
				if err := p.pubPC.AddICECandidate(*m.Candidate); err != nil {
					p.logger.Debug("galene: pubPC add ice: ", err)
				}
				continue
			}
			p.subPCmu.Lock()
			subPC := p.subPC
			subID := p.subRemoteID
			p.subPCmu.Unlock()
			if subPC != nil && m.Id == subID {
				if err := subPC.AddICECandidate(*m.Candidate); err != nil {
					p.logger.Debug("galene: subPC add ice: ", err)
				}
			}

		case "renegotiate":
			p.logger.Info("galene: renegotiate requested (id=", m.Id, ")")

		case "close":
			p.logger.Debug("galene: close id=", m.Id)
			p.subPCmu.Lock()
			if p.subPC != nil && m.Id == p.subRemoteID {
				_ = p.subPC.Close()
				p.subPC = nil
				p.subRemoteID = ""
			}
			p.subPCmu.Unlock()

		case "user":

		case "ping":
			_ = p.sig.send(map[string]any{"type": "pong"})

		case "pong":

		case "usermessage":
			p.logger.Debug("galene: usermessage (ignored)")

		case "error":
			p.logger.Warn("galene: server error")

		case "left":
			p.logger.Info("galene: left")
			p.closed.Store(true)
			return

		default:
			p.logger.Debug("galene: unknown message: ", m.Type)
		}
	}
}

func (p *Provider) Tunnel() *vp8.Tunnel             { return p.tunnel }
func (p *Provider) Start(ctx context.Context) error { return nil }

func (p *Provider) Stop(ctx context.Context) error {
	p.closed.Store(true)
	p.cancel()

	p.subPCmu.Lock()
	if p.subPC != nil {
		_ = p.subPC.Close()
		p.subPC = nil
	}
	p.subPCmu.Unlock()

	if p.pubPC != nil {
		_ = p.pubPC.Close()
	}
	if p.sig != nil {
		p.sig.close()
	}
	p.wg.Wait()
	return nil
}

func drainSenderRTCP(sender *webrtc.RTPSender) {
	if sender == nil {
		return
	}
	buf := make([]byte, 1500)
	for {
		if _, _, err := sender.Read(buf); err != nil {
			return
		}
	}
}
