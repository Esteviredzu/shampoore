package telemost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"

	"github.com/Esteviredzu/shampoore/internal/core/channel/vp8"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/net/utlsdial"
	"github.com/Esteviredzu/shampoore/internal/netdial"
)

const (
	wsPingPeriod       = 5 * time.Second
	wsWriteTimeout     = 10 * time.Second
	wsHandshakeTimeout = 15 * time.Second
	disconnectGrace    = 5 * time.Second
)

type signaling struct {
	cfg    *Config
	boot   *bootstrapResult
	logger logging.ContextLogger

	ws   *websocket.Conn
	wsMu sync.Mutex

	pcMu         sync.RWMutex
	pubPC        *webrtc.PeerConnection
	subPC        *webrtc.PeerConnection
	iceServers   []ICEServer
	sharingTrack *webrtc.TrackLocalStaticSample
	tunnel       *vp8.Tunnel
	pubSeq       int
	subSeq       int
	slotsKey     int

	reuseTunnel *vp8.Tunnel

	pubRemoteSet bool
	subRemoteSet bool
	pubPending   []webrtc.ICECandidateInit
	subPending   []webrtc.ICECandidateInit
	remoteMu     sync.Mutex

	pubReadySent bool
	subReadySent bool
	readyMu      sync.Mutex

	cbMu                  sync.RWMutex
	onPublishReady        func()
	onSubscribe           func()
	onSubscriberReconnect func()
	onSessionLost         func(reason string)

	lostOnce sync.Once

	stateMu        sync.Mutex
	stateTimer     *time.Timer
	disconnectWait time.Duration

	closeOnce sync.Once
	closed    chan struct{}
}

func newSignaling(cfg *Config, boot *bootstrapResult, logger logging.ContextLogger) *signaling {
	return &signaling{
		cfg:    cfg,
		boot:   boot,
		logger: logger,
		closed: make(chan struct{}),
	}
}

func newWebRTCAPI() (*webrtc.API, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	i := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
		return nil, err
	}

	se := webrtc.SettingEngine{}
	pionNet, err := netdial.NewPionNet()
	if err != nil {
		return nil, fmt.Errorf("telemost: create protected pion net: %w", err)
	}
	se.SetNet(pionNet)

	return webrtc.NewAPI(
		webrtc.WithMediaEngine(m),
		webrtc.WithSettingEngine(se),
		webrtc.WithInterceptorRegistry(i),
	), nil
}

func (s *signaling) Tunnel() *vp8.Tunnel {
	s.pcMu.RLock()
	defer s.pcMu.RUnlock()
	return s.tunnel
}

func (s *signaling) setReuseTunnel(t *vp8.Tunnel) {
	s.pcMu.Lock()
	s.reuseTunnel = t
	s.pcMu.Unlock()
}

func (s *signaling) reuseTunnelRef() *vp8.Tunnel {
	s.pcMu.RLock()
	defer s.pcMu.RUnlock()
	return s.reuseTunnel
}

func (s *signaling) publisher() *webrtc.PeerConnection {
	s.pcMu.RLock()
	defer s.pcMu.RUnlock()
	return s.pubPC
}

func (s *signaling) subscriber() *webrtc.PeerConnection {
	s.pcMu.RLock()
	defer s.pcMu.RUnlock()
	return s.subPC
}

func (s *signaling) publishSeq() int {
	s.pcMu.RLock()
	defer s.pcMu.RUnlock()
	return s.pubSeq
}

func (s *signaling) subscriberSeq() int {
	s.pcMu.RLock()
	defer s.pcMu.RUnlock()
	return s.subSeq
}

func (s *signaling) setSubscriberSeq(seq int) {
	s.pcMu.Lock()
	s.subSeq = seq
	s.pcMu.Unlock()
}

func (s *signaling) nextSlotsKey() int {
	s.pcMu.Lock()
	defer s.pcMu.Unlock()
	s.slotsKey++
	return s.slotsKey
}

func (s *signaling) iceServerList() []ICEServer {
	s.pcMu.RLock()
	defer s.pcMu.RUnlock()
	out := make([]ICEServer, len(s.iceServers))
	copy(out, s.iceServers)
	return out
}

func (s *signaling) installPCs(
	pub, sub *webrtc.PeerConnection,
	tunnel *vp8.Tunnel,
	sharing *webrtc.TrackLocalStaticSample,
) (oldPub, oldSub *webrtc.PeerConnection) {
	s.pcMu.Lock()
	oldPub, oldSub = s.pubPC, s.subPC
	s.pubPC, s.subPC, s.tunnel = pub, sub, tunnel
	s.sharingTrack = sharing
	s.pcMu.Unlock()
	return oldPub, oldSub
}

func (s *signaling) setSubscriber(pc *webrtc.PeerConnection) (old *webrtc.PeerConnection) {
	s.pcMu.Lock()
	old = s.subPC
	s.subPC = pc
	s.pcMu.Unlock()
	return old
}

func (s *signaling) setPublishReadyCallback(fn func()) {
	s.cbMu.Lock()
	s.onPublishReady = fn
	s.cbMu.Unlock()
}

func (s *signaling) setSubscribeCallback(fn func()) {
	s.cbMu.Lock()
	s.onSubscribe = fn
	s.cbMu.Unlock()
}

func (s *signaling) setSubscriberReconnectCallback(fn func()) {
	s.cbMu.Lock()
	s.onSubscriberReconnect = fn
	s.cbMu.Unlock()
}

func (s *signaling) setSessionLostCallback(fn func(reason string)) {
	s.cbMu.Lock()
	s.onSessionLost = fn
	s.cbMu.Unlock()
}

func (s *signaling) fireSubscriberReconnect() {
	s.cbMu.RLock()
	cb := s.onSubscriberReconnect
	s.cbMu.RUnlock()
	if cb != nil {
		cb()
	}
}

func (s *signaling) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *signaling) notifySessionLost(reason string) {
	if s.isClosed() {
		return
	}
	s.lostOnce.Do(func() {
		s.logger.Warn("telemost: session lost: ", reason)
		s.cbMu.RLock()
		cb := s.onSessionLost
		s.cbMu.RUnlock()
		if cb != nil {
			cb(reason)
		}
	})
}

func (s *signaling) handlePCState(
	label string,
	pc *webrtc.PeerConnection,
	state webrtc.PeerConnectionState,
) {
	if s.isClosed() {
		return
	}

	switch state {
	case webrtc.PeerConnectionStateConnected:
		s.cancelStateTimer()
		if label == "publisher" {
			s.markPublishReady()
		} else {
			s.markSubscribeReady()
		}
	case webrtc.PeerConnectionStateDisconnected:

		s.logger.Warn("telemost: ", label, " disconnected, waiting for recovery")
		s.armStateTimer(label, pc)
	case webrtc.PeerConnectionStateFailed:
		s.notifySessionLost(label + " peer connection failed")
	}
}

func (s *signaling) armStateTimer(label string, pc *webrtc.PeerConnection) {
	grace := s.disconnectWait
	if grace <= 0 {
		grace = disconnectGrace
	}

	s.stateMu.Lock()
	if s.stateTimer != nil {
		s.stateTimer.Stop()
	}
	s.stateTimer = time.AfterFunc(grace, func() {
		if s.isClosed() {
			return
		}
		if pc != nil && pc.ConnectionState() == webrtc.PeerConnectionStateConnected {
			return
		}
		s.notifySessionLost(label + " connection stalled")
	})
	s.stateMu.Unlock()
}

func (s *signaling) cancelStateTimer() {
	s.stateMu.Lock()
	if s.stateTimer != nil {
		s.stateTimer.Stop()
		s.stateTimer = nil
	}
	s.stateMu.Unlock()
}

func (s *signaling) closePC(pc *webrtc.PeerConnection, label string) {
	if pc == nil {
		return
	}
	if err := pc.Close(); err != nil && !errors.Is(err, webrtc.ErrConnectionClosed) {
		s.logger.Warn("telemost: close ", label, " peer connection: ", err)
	}
}

func (s *signaling) connectWS(ctx context.Context) error {
	u, err := url.Parse(s.boot.info.MediaServerURL)
	if err != nil {
		return fmt.Errorf("telemost: ws url: %w", err)
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "wss" {
			port = "443"
		} else {
			port = "80"
		}
	}

	profile := utlsdial.Profile(s.cfg.UTLSProfile)
	ua := utlsdial.UserAgent(profile)
	addr := net.JoinHostPort(u.Hostname(), port)

	var conn net.Conn
	if u.Scheme == "wss" {
		conn, err = utlsdial.DialContext(ctx, "tcp", addr, u.Hostname(), utlsdial.WebSocketALPN, profile)
	} else {
		dialer := &net.Dialer{Timeout: wsHandshakeTimeout, Control: netdial.Control}
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("telemost: ws dial: %w", err)
	}

	abort := make(chan struct{})
	abortDone := make(chan struct{})
	go func() {
		defer close(abortDone)
		select {
		case <-ctx.Done():
		case <-s.closed:
		case <-abort:
			return
		}
		if cerr := conn.Close(); cerr != nil {
			s.logger.Debug("telemost: abort ws handshake: ", cerr)
		}
	}()

	if err := conn.SetDeadline(time.Now().Add(wsHandshakeTimeout)); err != nil {
		close(abort)
		<-abortDone
		if cerr := conn.Close(); cerr != nil {
			s.logger.Debug("telemost: close ws connection: ", cerr)
		}
		return fmt.Errorf("telemost: ws handshake deadline: %w", err)
	}

	header := http.Header{}
	header.Set("User-Agent", ua)
	header.Set("Origin", pageOrigin)
	header.Set("Referer", pageOrigin+"/")
	if s.boot.jar != nil {
		for _, c := range s.boot.jar.Cookies(u) {
			header.Add("Cookie", c.String())
		}
	}

	ws, resp, err := websocket.NewClient(conn, u, header, 4096, 4096)
	close(abort)
	<-abortDone
	if err != nil {
		if resp != nil && resp.Body != nil {
			if _, cerr := io.Copy(io.Discard, resp.Body); cerr != nil {
				s.logger.Debug("telemost: drain ws handshake response: ", cerr)
			}
			if cerr := resp.Body.Close(); cerr != nil {
				s.logger.Debug("telemost: close ws handshake response: ", cerr)
			}
		}
		if cerr := conn.Close(); cerr != nil {
			s.logger.Debug("telemost: close failed ws connection: ", cerr)
		}
		return fmt.Errorf("telemost: ws handshake: %w", err)
	}
	if resp != nil && s.boot.jar != nil {
		if cookies := resp.Cookies(); len(cookies) > 0 {
			s.boot.jar.SetCookies(u, cookies)
		}
	}

	s.wsMu.Lock()
	s.ws = ws
	s.wsMu.Unlock()
	return nil
}

func (s *signaling) closeWS() {
	s.wsMu.Lock()
	ws := s.ws
	s.ws = nil
	s.wsMu.Unlock()
	if ws != nil {
		if err := ws.Close(); err != nil {
			s.logger.Debug("telemost: close ws: ", err)
		}
	}
}

func (s *signaling) wsSend(v any) error {

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	s.wsMu.Lock()
	defer s.wsMu.Unlock()

	if s.ws == nil {
		return fmt.Errorf("telemost: ws not connected")
	}
	if err := s.ws.SetWriteDeadline(time.Now().Add(wsWriteTimeout)); err != nil {
		return fmt.Errorf("telemost: ws write deadline: %w", err)
	}
	return s.ws.WriteMessage(websocket.TextMessage, data)
}

func (s *signaling) Run(ctx context.Context) error {
	dialCtx, cancelDial := context.WithCancel(ctx)
	defer cancelDial()

	runDone := make(chan struct{})
	defer close(runDone)
	go func() {
		select {
		case <-ctx.Done():
		case <-s.closed:
		case <-runDone:
			return
		}
		cancelDial()
		s.closeWS()
	}()

	if err := s.connectWS(dialCtx); err != nil {
		return err
	}
	defer s.closeWS()

	if err := s.sendHello(); err != nil {
		return fmt.Errorf("telemost: send hello: %w", err)
	}

	pingCtx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	go s.pingLoop(pingCtx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.closed:
			return nil
		default:
		}

		s.wsMu.Lock()
		ws := s.ws
		s.wsMu.Unlock()
		if ws == nil {
			return fmt.Errorf("telemost: ws closed")
		}

		_, raw, err := ws.ReadMessage()
		if err != nil {
			return fmt.Errorf("telemost: ws read: %w", err)
		}
		if err := s.handleMessage(raw); err != nil {
			s.logger.Warn("telemost: handle message: ", err)
		}
	}
}

func (s *signaling) pingLoop(ctx context.Context) {
	t := time.NewTicker(wsPingPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := s.wsSend(map[string]any{
				"uid":  newUUID(),
				"ping": map[string]any{},
			})
			if err != nil {
				if !s.isClosed() {
					s.logger.Warn("telemost: ping failed: ", err)
					s.notifySessionLost("ping failed: " + err.Error())
				}
				return
			}
		}
	}
}

func (s *signaling) sendHello() error {
	role := s.cfg.Role
	if role == "" {
		role = "SPEAKER"
	}

	sendVideo := !s.cfg.UseSharing
	sendSharing := s.cfg.UseSharing

	hello := map[string]any{
		"uid": newUUID(),
		"hello": map[string]any{
			"participantMeta": map[string]any{
				"name":        s.cfg.EffectiveDisplayName(),
				"role":        role,
				"description": "",
				"sendAudio":   false,
				"sendVideo":   sendVideo,
				"sendSharing": sendSharing,
			},
			"participantAttributes": map[string]any{
				"name":        s.cfg.EffectiveDisplayName(),
				"role":        role,
				"description": "",
			},
			"sendAudio":         false,
			"sendVideo":         sendVideo,
			"sendSharing":       sendSharing,
			"participantId":     s.boot.info.PeerID,
			"roomId":            s.boot.info.RoomID,
			"serviceName":       s.boot.info.ServiceName,
			"credentials":       s.boot.info.Credentials,
			"capabilitiesOffer": capabilitiesOffer,
			"sdkInfo": map[string]any{
				"implementation": "browser",
				"version":        s.cfg.SDKVersion,
				"userAgent":      utlsdial.UserAgent(utlsdial.Profile(s.cfg.UTLSProfile)),
				"hwConcurrency":  8,
			},
			"sdkInitializationId":    newUUID(),
			"disablePublisher":       false,
			"disableSubscriber":      false,
			"disableSubscriberAudio": false,
		},
	}
	return s.wsSend(hello)
}

func (s *signaling) handleMessage(raw []byte) error {
	var msg map[string]any
	if err := json.Unmarshal(raw, &msg); err != nil {
		return err
	}
	uid, _ := msg["uid"].(string)

	for k := range msg {
		if k == "uid" || k == "ack" || k == "ping" {
			continue
		}
		s.logger.Debug("telemost: <- ", k)
		break
	}

	switch {
	case msg["serverHello"] != nil:
		s.ack(uid)
		return s.onServerHello(msg["serverHello"])

	case msg["publisherSdpAnswer"] != nil:
		s.ack(uid)
		return s.onPublisherAnswer(msg["publisherSdpAnswer"])

	case msg["subscriberSdpOffer"] != nil:
		s.ack(uid)
		return s.onSubscriberOffer(msg["subscriberSdpOffer"])

	case msg["webrtcIceCandidate"] != nil:
		s.ack(uid)
		return s.onICECandidate(msg["webrtcIceCandidate"])

	case msg["upsertDescription"] != nil:
		s.ack(uid)
		return s.onUpsertDescription(msg["upsertDescription"])

	case msg["ack"] != nil:
		return nil

	case msg["ping"] != nil:
		return nil
	}

	if uid != "" {
		s.ack(uid)
	}
	return nil
}

func (s *signaling) ack(uid string) {
	if uid == "" {
		return
	}
	if err := s.wsSend(map[string]any{
		"uid": uid,
		"ack": map[string]any{
			"status": map[string]any{
				"code":        "OK",
				"description": "",
			},
		},
	}); err != nil && !s.isClosed() {
		s.logger.Warn("telemost: ack failed: ", err)
	}
}

func (s *signaling) onServerHello(raw any) error {
	m, _ := raw.(map[string]any)
	if m == nil {
		return nil
	}

	ice := s.boot.info.ICEServers

	if rtcCfg, ok := m["rtcConfiguration"].(map[string]any); ok {
		if servers, ok := rtcCfg["iceServers"].([]any); ok && len(servers) > 0 {
			var parsed []ICEServer
			for _, srv := range servers {
				sm, _ := srv.(map[string]any)
				if sm == nil {
					continue
				}
				var urls []string
				if u, ok := sm["urls"].([]any); ok {
					for _, item := range u {
						if str, ok := item.(string); ok {
							urls = append(urls, str)
						}
					}
				}
				if len(urls) == 0 {
					continue
				}
				entry := ICEServer{URLs: urls}
				if u, ok := sm["username"].(string); ok && u != "" {
					entry.Username = u
					entry.Credential, _ = sm["credential"].(string)
				}
				parsed = append(parsed, entry)
			}
			if len(parsed) > 0 {
				ice = parsed
			}
		}
	}

	if err := s.wsSend(setSlotsOffsetMessage(0)); err != nil {
		return fmt.Errorf("telemost: set slots offset: %w", err)
	}

	return s.initPeerConnections(ice)
}

func (s *signaling) initPeerConnections(ice []ICEServer) error {
	s.pcMu.Lock()
	s.iceServers = ice
	s.pcMu.Unlock()

	api, err := newWebRTCAPI()
	if err != nil {
		return err
	}

	cfg := webrtc.Configuration{}
	for _, srv := range ice {
		entry := webrtc.ICEServer{URLs: srv.URLs}
		if srv.Username != "" {
			entry.Username = srv.Username
			entry.Credential = srv.Credential
		}
		cfg.ICEServers = append(cfg.ICEServers, entry)
	}

	pubPC, err := api.NewPeerConnection(cfg)
	if err != nil {
		return err
	}

	subPC, err := s.createSubPC()
	if err != nil {
		s.closePC(pubPC, "publisher")
		return err
	}

	trackID := "video-" + s.boot.info.PeerID
	streamID := "shampoore-" + s.boot.info.PeerID
	if s.cfg.UseSharing {
		trackID = "screen-" + s.boot.info.PeerID
		streamID = "shampoore-screen-" + s.boot.info.PeerID
	}
	videoTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
		trackID, streamID,
	)
	if err != nil {
		s.closePC(pubPC, "publisher")
		s.closePC(subPC, "subscriber")
		return err
	}
	videoTr, err := pubPC.AddTransceiverFromTrack(
		videoTrack,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly},
	)
	if err != nil {
		s.closePC(pubPC, "publisher")
		s.closePC(subPC, "subscriber")
		return err
	}
	go drainSenderRTCP(videoTr.Sender())

	var sharing *webrtc.TrackLocalStaticSample
	if s.cfg.UseSharing {
		sharing = videoTrack
	}

	reuse := s.reuseTunnelRef()
	if reuse == nil {

		reuse = s.Tunnel()
	}

	var tunnel *vp8.Tunnel
	if reuse != nil {
		if err := reuse.Rebind(subPC, videoTrack); err != nil {
			s.closePC(pubPC, "publisher")
			s.closePC(subPC, "subscriber")
			return err
		}
		tunnel = reuse
	} else {
		tunnel, err = vp8.AttachTunnel(pubPC, subPC, videoTrack)
		if err != nil {
			s.closePC(pubPC, "publisher")
			s.closePC(subPC, "subscriber")
			return err
		}
	}

	if sharing != nil {
		tunnel.SetSharingTrack(sharing)
		s.logger.Info("telemost: sharing track attached to tunnel")
	}

	pubPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		s.sendICE(c, "PUBLISHER", s.publishSeq())
	})
	pubPC.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		s.logger.Info("telemost: pub ICE state = ", state.String())
	})
	pubPC.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		s.logger.Info("telemost: pub PC state = ", state.String())
		s.handlePCState("publisher", pubPC, state)
	})

	if s.isClosed() {
		s.closePC(pubPC, "publisher")
		s.closePC(subPC, "subscriber")
		return errors.New("telemost: signaling closed during peer connection setup")
	}

	if oldPub, oldSub := s.installPCs(pubPC, subPC, tunnel, sharing); oldPub != nil || oldSub != nil {
		if oldPub != nil && oldPub != pubPC {
			s.closePC(oldPub, "publisher")
		}
		if oldSub != nil && oldSub != subPC {
			s.closePC(oldSub, "subscriber")
		}
	}

	return s.startPublish(pubPC)
}

func (s *signaling) createSubPC() (*webrtc.PeerConnection, error) {
	cfg := webrtc.Configuration{}
	for _, srv := range s.iceServerList() {
		entry := webrtc.ICEServer{URLs: srv.URLs}
		if srv.Username != "" {
			entry.Username = srv.Username
			entry.Credential = srv.Credential
		}
		cfg.ICEServers = append(cfg.ICEServers, entry)
	}

	api, err := newWebRTCAPI()
	if err != nil {
		return nil, err
	}
	subPC, err := api.NewPeerConnection(cfg)
	if err != nil {
		return nil, err
	}

	subPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		s.sendICE(c, "SUBSCRIBER", s.subscriberSeq())
	})
	subPC.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		s.logger.Info("telemost: sub ICE state = ", state.String())
	})
	subPC.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		s.logger.Info("telemost: sub PC state = ", state.String())
		s.handlePCState("subscriber", subPC, state)
	})
	return subPC, nil
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

func (s *signaling) markPublishReady() {
	s.readyMu.Lock()
	if s.pubReadySent {
		s.readyMu.Unlock()
		return
	}
	s.pubReadySent = true
	s.readyMu.Unlock()

	s.cbMu.RLock()
	cb := s.onPublishReady
	s.cbMu.RUnlock()

	s.logger.Info("telemost: publish connected")
	if cb != nil {
		cb()
	}
}

func (s *signaling) markSubscribeReady() {
	s.readyMu.Lock()
	if s.subReadySent {
		s.readyMu.Unlock()
		return
	}
	s.subReadySent = true
	s.readyMu.Unlock()

	s.cbMu.RLock()
	cb := s.onSubscribe
	s.cbMu.RUnlock()

	s.logger.Info("telemost: subscribe connected")
	if cb != nil {
		cb()
	}
}

func (s *signaling) subscriberReady() bool {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	return s.subReadySent
}

func (s *signaling) startPublish(pc *webrtc.PeerConnection) error {
	s.pcMu.Lock()
	s.pubSeq = 1
	seq := s.pubSeq
	s.pcMu.Unlock()

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}

	videoMids, audioMids := parseMids(offer.SDP)

	var tracks []map[string]any
	if len(audioMids) > 0 {
		tracks = append(tracks, map[string]any{
			"mid":            audioMids[0],
			"transceiverMid": audioMids[0],
			"kind":           "AUDIO",
			"priority":       0,
			"label":          "Microphone",
			"codecs":         map[string]any{},
			"groupId":        1,
			"description":    "",
		})
	}
	if len(videoMids) > 0 {
		label := ""
		if s.cfg.UseSharing {
			label = "Screen"
		}
		tracks = append(tracks, DisplayVideoTrack(videoMids[0], label))
	}

	s.logger.Debug("telemost: -> publisherSdpOffer pcSeq=", seq,
		" tracks=", len(tracks), " audioMids=", audioMids, " videoMids=", videoMids,
		" useSharing=", s.cfg.UseSharing)

	return s.wsSend(map[string]any{
		"uid": newUUID(),
		"publisherSdpOffer": map[string]any{
			"pcSeq":  seq,
			"sdp":    offer.SDP,
			"tracks": tracks,
		},
	})
}

func (s *signaling) onPublisherAnswer(raw any) error {
	m, _ := raw.(map[string]any)
	sdp, _ := m["sdp"].(string)
	if sdp == "" {
		return fmt.Errorf("telemost: empty SDP in publisherSdpAnswer")
	}

	pc := s.publisher()
	if pc == nil {
		return errors.New("telemost: publisher peer connection is not ready")
	}

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  sdp,
	}); err != nil {
		return err
	}

	s.remoteMu.Lock()
	s.pubRemoteSet = true
	pending := s.pubPending
	s.pubPending = nil
	s.remoteMu.Unlock()

	for _, c := range pending {
		if err := pc.AddICECandidate(c); err != nil {
			s.logger.Warn("telemost: add pending publisher candidate: ", err)
		}
	}

	if err := s.wsSend(sdkCodecsInfoMessage()); err != nil {
		return fmt.Errorf("telemost: send codecs info: %w", err)
	}

	videoLabel := ""
	if s.cfg.UseSharing {
		videoLabel = "Screen"
	}
	if err := s.wsSend(updatePublisherTrackDescriptionMessage(
		pc,
		"Microphone",
		videoLabel,
	)); err != nil {
		return fmt.Errorf("telemost: send track description: %w", err)
	}

	for i := 0; i < 4; i++ {
		key := s.nextSlotsKey()
		if err := s.wsSend(startupSetSlotsMessage(i, key)); err != nil {
			return fmt.Errorf("telemost: send startup slots: %w", err)
		}
		s.logger.Debug("telemost: -> setSlots key=", key, " startup=", i+1)
	}

	if s.cfg.UseSharing {
		s.logger.Info("telemost: -> updatePublisherSharingTrack Screen")
		if err := s.wsSend(UpdatePublisherSharingTrackMessage("Screen")); err != nil {
			return fmt.Errorf("telemost: send sharing track: %w", err)
		}
		if err := s.wsSend(UpdateMeMessage(s.cfg.EffectiveDisplayName(), false, true)); err != nil {
			return fmt.Errorf("telemost: send update me: %w", err)
		}
	}

	return nil
}

func (s *signaling) onUpsertDescription(raw any) error {
	m, _ := raw.(map[string]any)
	if m == nil {
		return nil
	}
	s.logger.Debug("telemost: upsertDescription received (binding will happen in OnTrack)")
	return nil
}

func (s *signaling) onSubscriberOffer(raw any) error {
	m, _ := raw.(map[string]any)
	sdp, _ := m["sdp"].(string)
	if sdp == "" {
		return fmt.Errorf("telemost: empty SDP in subscriberSdpOffer")
	}
	if pcSeq, ok := m["pcSeq"].(float64); ok {
		s.setSubscriberSeq(int(pcSeq))
	}

	pc := s.subscriber()
	if pc == nil {
		return errors.New("telemost: subscriber peer connection is not ready")
	}

	offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}
	if !s.subscriberReady() {
		if err := pc.SetRemoteDescription(offer); err != nil {
			return err
		}
		return s.answerSubscriber(pc)
	}

	if err := pc.SetRemoteDescription(offer); err == nil {
		s.logger.Info("telemost: subscriber renegotiated on existing PC")
		s.fireSubscriberReconnect()
		return s.answerSubscriber(pc)
	}

	s.logger.Info("telemost: reconnect detected, recreating subscriber PC")

	s.remoteMu.Lock()
	s.subRemoteSet = false
	s.subPending = nil
	s.remoteMu.Unlock()

	newSubPC, err := s.createSubPC()
	if err != nil {
		return err
	}
	if old := s.setSubscriber(newSubPC); old != nil && old != newSubPC {
		s.closePC(old, "subscriber")
	}

	if t := s.Tunnel(); t != nil {
		t.AttachSubPC(newSubPC)
	}
	s.fireSubscriberReconnect()

	if err := newSubPC.SetRemoteDescription(offer); err != nil {
		return err
	}
	return s.answerSubscriber(newSubPC)
}

func (s *signaling) answerSubscriber(pc *webrtc.PeerConnection) error {
	s.remoteMu.Lock()
	s.subRemoteSet = true
	pending := s.subPending
	s.subPending = nil
	s.remoteMu.Unlock()

	for _, c := range pending {
		if err := pc.AddICECandidate(c); err != nil {
			s.logger.Warn("telemost: add pending subscriber candidate: ", err)
		}
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		return err
	}
	return s.wsSend(map[string]any{
		"uid": newUUID(),
		"subscriberSdpAnswer": map[string]any{
			"sdp":   answer.SDP,
			"pcSeq": s.subscriberSeq(),
		},
	})
}

func (s *signaling) onICECandidate(raw any) error {
	m, _ := raw.(map[string]any)
	cand, _ := m["candidate"].(string)
	mid, _ := m["sdpMid"].(string)
	target, _ := m["target"].(string)
	idxF, _ := m["sdpMlineIndex"].(float64)
	idx := uint16(idxF)

	init := webrtc.ICECandidateInit{
		Candidate:     cand,
		SDPMid:        &mid,
		SDPMLineIndex: &idx,
	}

	s.pcMu.RLock()
	pubPC, subPC := s.pubPC, s.subPC
	s.pcMu.RUnlock()

	s.remoteMu.Lock()
	switch target {
	case "PUBLISHER":
		if !s.pubRemoteSet {
			s.pubPending = append(s.pubPending, init)
			s.remoteMu.Unlock()
			return nil
		}
		s.remoteMu.Unlock()
		if pubPC == nil {
			return errors.New("telemost: publisher peer connection is not ready")
		}
		return pubPC.AddICECandidate(init)
	case "SUBSCRIBER":
		if !s.subRemoteSet {
			s.subPending = append(s.subPending, init)
			s.remoteMu.Unlock()
			return nil
		}
		s.remoteMu.Unlock()
		if subPC == nil {
			return errors.New("telemost: subscriber peer connection is not ready")
		}
		return subPC.AddICECandidate(init)
	}
	s.remoteMu.Unlock()
	return nil
}

func (s *signaling) sendICE(c *webrtc.ICECandidate, target string, pcSeq int) {
	jc := c.ToJSON()
	var mid string
	if jc.SDPMid != nil {
		mid = *jc.SDPMid
	}
	var idx uint16
	if jc.SDPMLineIndex != nil {
		idx = *jc.SDPMLineIndex
	}
	if err := s.wsSend(map[string]any{
		"uid": newUUID(),
		"webrtcIceCandidate": map[string]any{
			"candidate":           jc.Candidate,
			"sdpMid":              mid,
			"sdpUsernameFragment": ufragFromCandidate(jc.Candidate),
			"sdpMlineIndex":       idx,
			"target":              target,
			"pcSeq":               pcSeq,
		},
	}); err != nil && !s.isClosed() {
		s.logger.Warn("telemost: send ICE candidate: ", err)
	}
}

func (s *signaling) Close() {
	s.closeOnce.Do(func() { close(s.closed) })
	s.cancelStateTimer()
	s.closeWS()

	s.pcMu.Lock()
	pubPC := s.pubPC
	subPC := s.subPC
	s.pubPC = nil
	s.subPC = nil
	s.tunnel = nil
	s.sharingTrack = nil
	s.reuseTunnel = nil
	s.pcMu.Unlock()

	if pubPC != nil {
		s.closePC(pubPC, "publisher")
	}
	if subPC != nil {
		s.closePC(subPC, "subscriber")
	}
}

func ufragFromCandidate(c string) string {
	parts := strings.Fields(c)
	for i, p := range parts {
		if p == "ufrag" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func parseMids(sdp string) (video []string, audio []string) {
	var media string
	for _, line := range strings.Split(sdp, "\r\n") {
		switch {
		case strings.HasPrefix(line, "m=audio"):
			media = "audio"
		case strings.HasPrefix(line, "m=video"):
			media = "video"
		case strings.HasPrefix(line, "m="):
			media = ""
		case strings.HasPrefix(line, "a=mid:"):
			mid := strings.TrimPrefix(line, "a=mid:")
			switch media {
			case "audio":
				audio = append(audio, mid)
			case "video":
				video = append(video, mid)
			}
		}
	}
	return video, audio
}
