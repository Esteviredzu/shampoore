package livekit

import "testing"

func TestConfigLegacyCompatibility(t *testing.T) {
	cfg := Config{
		SignalURL: "wss://livekit.example",
		APIKey:    "key",
		APISecret: "secret",
		Room:      "room",
	}
	if err := cfg.ValidateForRole("client"); err != nil {
		t.Fatalf("legacy client config: %v", err)
	}
	if err := cfg.ValidateForRole("server"); err != nil {
		t.Fatalf("legacy server config: %v", err)
	}
}

func TestConfigMultiuserValidation(t *testing.T) {
	cfg := Config{
		SignalURL:   "wss://livekit.example",
		APIKey:      "key",
		APISecret:   "secret",
		Multiuser:   true,
		TokenListen: "127.0.0.1:8099",
	}
	if err := cfg.ValidateForRole("server"); err != nil {
		t.Fatalf("multiuser server config: %v", err)
	}

	cfg.TokenListen = ""
	if err := cfg.ValidateForRole("server"); err == nil {
		t.Fatal("expected multiuser config without token_listen to fail")
	}
}

func TestConfigOnDemandClientValidation(t *testing.T) {
	cfg := Config{
		SignalURL: "wss://livekit.example",
		TokenURL:  "https://control.example/session",
		UserID:    "alice",
	}
	if err := cfg.ValidateForRole("client"); err != nil {
		t.Fatalf("on-demand client config: %v", err)
	}

	cfg.UserID = ""
	if err := cfg.ValidateForRole("client"); err == nil {
		t.Fatal("expected on-demand config without user_id to fail")
	}
}

func TestNormalizeTokenURL(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1:8099":                  "http://127.0.0.1:8099/session",
		"http://127.0.0.1:8099":           "http://127.0.0.1:8099/session",
		"https://control.example/session": "https://control.example/session",
		"":                                "",
	}
	for in, want := range cases {
		if got := NormalizeTokenURL(in); got != want {
			t.Fatalf("NormalizeTokenURL(%q) = %q, want %q", in, got, want)
		}
	}
}
