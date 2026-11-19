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

func TestBuildMultiuserSubscriptionServer(t *testing.T) {
	settings := &app.Settings{
		Type: "livekit_datachannel",
		Transport: json.RawMessage(`{
			"signal_url":"wss://livekit.example",
			"api_key":"key",
			"api_secret":"secret",
			"multiuser":true,
			"room_prefix":"route-9"
		}`),
		Advanced: app.NewValues(),
	}
	res, err := build(context.Background(), settings, "server", logging.New("test", false), defaults)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if res.SessionServer == nil {
		t.Fatal("subscription multiuser build did not return a session server")
	}
	if res.Transport != nil || res.Provider != nil {
		t.Fatal("multiuser build must not create a client provider")
	}
}

func TestBuildSubscriptionConfigMultiuser(t *testing.T) {
	settings, err := buildSubscriptionConfig(app.RouteParams{
		RouteID:    9,
		RouteName:  "route-9",
		UserID:     "alice",
		ExitNodeID: "2",
		Domain:     "lk.example.com",
		Secrets: map[string]string{
			"livekit_api_key":    "key",
			"livekit_api_secret": "secret",
		},
	})
	if err != nil {
		t.Fatalf("subscription config: %v", err)
	}
	var tr map[string]any
	if err := json.Unmarshal(settings.Transport, &tr); err != nil {
		t.Fatal(err)
	}
	if tr["signal_url"] != "wss://lk.example.com" {
		t.Fatalf("signal_url = %v", tr["signal_url"])
	}
	if tr["api_key"] != "key" || tr["api_secret"] != "secret" {
		t.Fatalf("credentials = %v/%v", tr["api_key"], tr["api_secret"])
	}
	if tr["room"] != "route-9-alice" {
		t.Fatalf("room = %v", tr["room"])
	}
	if tr["room_prefix"] != "route-9" {
		t.Fatalf("room_prefix = %v", tr["room_prefix"])
	}
	if tr["user_id"] != "alice" {
		t.Fatalf("user_id = %v", tr["user_id"])
	}
	if _, ok := tr["token_url"]; ok {
		t.Fatalf("subscription config leaked token_url: %v", tr)
	}
	if _, ok := tr["token_secret"]; ok {
		t.Fatalf("subscription config leaked token_secret: %v", tr)
	}
}

func TestBuildSubscriptionConfigMultiuserRequiresUser(t *testing.T) {
	_, err := buildSubscriptionConfig(app.RouteParams{
		RouteID: 9,
		Domain:  "lk.example.com",
		Secrets: map[string]string{
			"livekit_api_key":    "key",
			"livekit_api_secret": "secret",
		},
	})
	if err == nil {
		t.Fatal("expected missing user_id to fail")
	}
}

func TestBuildServerConfigMultiuser(t *testing.T) {
	secrets := map[string]string{
		"livekit_api_key":    "key",
		"livekit_api_secret": "secret",
	}

	subscription, err := buildServerConfig(app.RouteParams{
		RouteID: 9,
		Domain:  "lk.example.com",
		Secrets: secrets,
	})
	if err != nil {
		t.Fatalf("subscription server config: %v", err)
	}
	var tr map[string]any
	if err := json.Unmarshal(subscription.Transport, &tr); err != nil {
		t.Fatal(err)
	}
	if tr["multiuser"] != true {
		t.Fatalf("multiuser = %v", tr["multiuser"])
	}
	if users, ok := tr["users"].([]any); !ok || len(users) != 0 {
		t.Fatalf("users = %#v, want empty array", tr["users"])
	}
	if tr["room_prefix"] != "route-9" {
		t.Fatalf("room_prefix = %v", tr["room_prefix"])
	}
	for _, removed := range []string{"token_listen", "token_url", "token_secret"} {
		if _, ok := tr[removed]; ok {
			t.Fatalf("server config has removed field %s: %v", removed, tr)
		}
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
