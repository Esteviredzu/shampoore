package livekit

import "testing"

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
