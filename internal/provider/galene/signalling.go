package galene

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"

	"github.com/Esteviredzu/shampoore-core/internal/logging"
)

type signaling struct {
	ws      *websocket.Conn
	readMu  sync.Mutex
	writeMu sync.Mutex
	logger  logging.ContextLogger
	cfg     Config
}

type inboundMessage struct {
	Type       string                   `json:"type"`
	Id         string                   `json:"id,omitempty"`
	Kind       string                   `json:"kind,omitempty"`
	Username   string                   `json:"username,omitempty"`
	Sdp        string                   `json:"sdp,omitempty"`
	Candidate  *webrtc.ICECandidateInit `json:"candidate,omitempty"`
	IceServers []json.RawMessage        `json:"iceServers,omitempty"`
	Error      string                   `json:"error,omitempty"`
	Status     any                      `json:"status,omitempty"`

	RtcConfiguration *struct {
		ICEServers []struct {
			URLs       []string `json:"urls"`
			Username   string   `json:"username,omitempty"`
			Credential string   `json:"credential,omitempty"`
		} `json:"iceServers"`
	} `json:"rtcConfiguration,omitempty"`
}

func (s *signaling) connect(ctx context.Context, insecure bool) error {
	dialer := *websocket.DefaultDialer
	if insecure {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		s.logger.Warn("galene: TLS verification disabled")
	}

	u, err := url.Parse(s.cfg.GroupURL)
	if err != nil {
		return err
	}

	scheme := "wss"
	if u.Scheme == "http" {
		scheme = "ws"
	}

	wsURL := scheme + "://" + u.Host + "/ws"

	header := http.Header{}
	header.Set("Origin", scheme+"://"+u.Host)

	s.logger.Info("galene: dial ", wsURL)
	ws, resp, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		if resp != nil {
			s.logger.Error("galene: ws dial failed: ", err, " | status=", resp.StatusCode)
		}
		return fmt.Errorf("galene: ws dial: %w", err)
	}
	s.ws = ws
	return nil
}

func (s *signaling) send(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.ws == nil {
		return fmt.Errorf("galene: ws not connected")
	}
	s.logger.Trace("galene >>> ", string(data))
	return s.ws.WriteMessage(websocket.TextMessage, data)
}

func (s *signaling) recv(ctx context.Context) (*inboundMessage, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if s.ws == nil {
		return nil, fmt.Errorf("galene: ws not connected")
	}
	_, data, err := s.ws.ReadMessage()
	if err != nil {
		return nil, err
	}
	s.logger.Trace("galene <<< ", string(data))
	var m inboundMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *signaling) handshake(ctx context.Context, id string) ([]webrtc.ICEServer, error) {
	if err := s.send(map[string]any{
		"type":    "handshake",
		"version": []string{"2"},
		"id":      id,
	}); err != nil {
		return nil, err
	}
	for {
		m, err := s.recv(ctx)
		if err != nil {
			return nil, err
		}
		if m.Type != "handshake" {
			continue
		}
		var ice []webrtc.ICEServer
		for _, raw := range m.IceServers {
			var srv struct {
				URLs       []string `json:"urls"`
				Username   string   `json:"username,omitempty"`
				Credential string   `json:"credential,omitempty"`
			}
			if err := json.Unmarshal(raw, &srv); err != nil {
				continue
			}
			ice = append(ice, webrtc.ICEServer{
				URLs:       srv.URLs,
				Username:   srv.Username,
				Credential: srv.Credential,
			})
		}
		return ice, nil
	}
}

func (s *signaling) join(ctx context.Context) ([]webrtc.ICEServer, error) {
	if err := s.send(map[string]any{
		"type":     "join",
		"kind":     "join",
		"group":    s.cfg.Group,
		"username": s.cfg.Username,
		"password": s.cfg.Password,
	}); err != nil {
		return nil, err
	}
	for {
		m, err := s.recv(ctx)
		if err != nil {
			return nil, err
		}
		switch m.Type {
		case "joined":
			if m.Error != "" {
				return nil, fmt.Errorf("galene: join: %s", m.Error)
			}
			var ice []webrtc.ICEServer
			if m.RtcConfiguration != nil {
				for _, srv := range m.RtcConfiguration.ICEServers {
					ice = append(ice, webrtc.ICEServer{
						URLs:       srv.URLs,
						Username:   srv.Username,
						Credential: srv.Credential,
					})
				}
			}
			return ice, nil
		case "rejected":
			return nil, fmt.Errorf("galene: join rejected: %s", m.Error)
		case "usermessage", "offer", "ice", "user", "renegotiate":
			continue
		}
	}
}

func (s *signaling) sendOffer(id, label, source, username, sdp string) error {
	return s.send(map[string]any{
		"type":     "offer",
		"id":       id,
		"label":    label,
		"source":   source,
		"username": username,
		"sdp":      sdp,
	})
}

func (s *signaling) sendAnswer(id, sdp string) error {
	return s.send(map[string]any{
		"type": "answer",
		"id":   id,
		"sdp":  sdp,
	})
}

func (s *signaling) sendICE(id string, c webrtc.ICECandidateInit) error {
	return s.send(map[string]any{
		"type":      "ice",
		"id":        id,
		"candidate": c,
	})
}

func (s *signaling) sendRequest() error {
	requested := map[string][]string{
		"camera": {"audio", "video"},
		"":       {"audio", "video"},
	}
	return s.send(map[string]any{
		"type":    "request",
		"request": requested,
	})
}

func (s *signaling) close() {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.ws != nil {
		_ = s.ws.Close()
		s.ws = nil
	}
}
