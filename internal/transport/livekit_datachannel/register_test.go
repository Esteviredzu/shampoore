package livekit_datachannel

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"
)

func TestBuildMultiuserServer(t *testing.T) {
	settings := &app.Settings{
		Type: "livekit_datachannel",
		Transport: json.RawMessage(`{
			"signal_url":"wss://livekit.example",
			"api_key":"key",
			"api_secret":"secret",
			"multiuser":true,
			"token_listen":"127.0.0.1:0",
			"room_prefix":"test"
		}`),
		Advanced: app.NewValues(),
	}
	res, err := build(context.Background(), settings, "server", logging.New("test", false), defaults)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if res.SessionServer == nil {
		t.Fatal("multiuser build did not return a session server")
	}
	if res.Transport != nil || res.Provider != nil {
		t.Fatal("multiuser build must not create a client provider")
	}
	if res.UserRegistry == nil {
		t.Fatal("multiuser build did not return a user registry")
	}
}

func TestBuildLegacyClientRequiresRoom(t *testing.T) {
	settings := &app.Settings{
		Type:      "livekit_datachannel",
		Transport: json.RawMessage(`{"signal_url":"wss://livekit.example","api_key":"key","api_secret":"secret"}`),
		Advanced:  app.NewValues(),
	}
	if _, err := build(context.Background(), settings, "client", logging.New("test", false), defaults); err == nil {
		t.Fatal("expected legacy client config without room to fail")
	}
}
