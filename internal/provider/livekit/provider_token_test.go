package livekit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProviderRequestSession(t *testing.T) {
	var gotSecret string
	var gotReq sessionRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		gotSecret = r.Header.Get("X-Shampoore-Secret")
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Errorf("decode: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sessionResponse{
			URL:   "wss://issued.example",
			Token: "issued-token",
			Room:  "room-1",
		})
	}))
	defer ts.Close()

	p := &Provider{
		cfg: Config{
			SignalURL:   "wss://fallback.example",
			TokenURL:    ts.URL + "/session",
			TokenSecret: "secret",
			UserID:      "alice",
			ExitNodeID:  "exit-germany",
		},
		logger:     nopLogger{},
		tokenURL:   ts.URL + "/session",
		httpClient: ts.Client(),
	}

	resp, err := p.requestSession()
	if err != nil {
		t.Fatalf("request session: %v", err)
	}
	if gotSecret != "secret" {
		t.Fatalf("secret = %q", gotSecret)
	}
	if gotReq.UserID != "alice" || gotReq.ExitNodeID != "exit-germany" {
		t.Fatalf("request = %+v", gotReq)
	}
	if resp.Token != "issued-token" || resp.Room != "room-1" {
		t.Fatalf("response = %+v", resp)
	}
	if resp.URL != "wss://issued.example" {
		t.Fatalf("url = %q", resp.URL)
	}
}

func TestProviderStaticTokenAndLegacyToken(t *testing.T) {
	static := &Provider{
		cfg: Config{
			SignalURL: "wss://livekit.example",
			Token:     "static-token",
			Room:      "static-room",
		},
		logger: nopLogger{},
	}
	token, url, room, err := static.connectionToken()
	if err != nil {
		t.Fatalf("static token: %v", err)
	}
	if token != "static-token" || url != "wss://livekit.example" || room != "static-room" {
		t.Fatalf("unexpected static token result: %q %q %q", token, url, room)
	}

	legacy := &Provider{
		cfg: Config{
			SignalURL: "wss://livekit.example",
			APIKey:    "key",
			APISecret: "secret",
			Room:      "legacy-room",
		},
		logger:   nopLogger{},
		identity: "alice",
	}
	token, _, room, err = legacy.connectionToken()
	if err != nil {
		t.Fatalf("legacy token: %v", err)
	}
	if token == "" || room != "legacy-room" {
		t.Fatalf("unexpected legacy token result: %q %q", token, room)
	}
}

func TestProviderConnectionTokenFailsWhenEndpointUnavailable(t *testing.T) {
	p := &Provider{
		cfg: Config{
			SignalURL: "wss://livekit.example",
			TokenURL:  "http://127.0.0.1:1/session",
			UserID:    "alice",
		},
		logger:     nopLogger{},
		tokenURL:   "http://127.0.0.1:1/session",
		httpClient: &http.Client{},
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	defer p.cancel()
	if _, _, _, err := p.connectionToken(); err == nil {
		t.Fatal("expected token request error")
	}
}
