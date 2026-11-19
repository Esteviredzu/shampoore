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
		SignalURL: "wss://livekit.example",
		APIKey:    "key",
		APISecret: "secret",
		Multiuser: true,
	}
	if err := cfg.ValidateForRole("server"); err != nil {
		t.Fatalf("multiuser server config: %v", err)
	}

	cfg.APISecret = ""
	if err := cfg.ValidateForRole("server"); err == nil {
		t.Fatal("expected multiuser config without api_secret to fail")
	}
}

func TestConfigRequiresSignalURL(t *testing.T) {
	cfg := Config{
		APIKey:    "key",
		APISecret: "secret",
		Room:      "room",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing signal_url to fail")
	}
}

func TestConfigStaticToken(t *testing.T) {
	cfg := Config{SignalURL: "wss://livekit.example", Token: "static-token"}
	if err := cfg.ValidateForRole("client"); err != nil {
		t.Fatalf("static token config: %v", err)
	}
}

func TestConfigRejectsRemovedTokenEndpointFields(t *testing.T) {
	cfg := Config{SignalURL: "wss://livekit.example", Room: "room"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing api credentials to fail")
	}
}

func TestRoomForUser(t *testing.T) {
	if got := RoomForUser("route-9", "Alice"); got != "route-9-alice" {
		t.Fatalf("RoomForUser = %q", got)
	}
	if got := RoomForUser("", "alice"); got != defaultRoomPrefix+"-alice" {
		t.Fatalf("default RoomForUser = %q", got)
	}
	if got := RoomForUser("route 9", "bad id!"); got != "route_9-bad_id" {
		t.Fatalf("sanitized RoomForUser = %q", got)
	}
}
