package raw_ws

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
)

func TestValidateRejectsInsecureWSByDefault(t *testing.T) {
	raw, err := json.Marshal(Config{
		ServerURL: "ws://example.test",
		Room:      "room-1",
		Password:  "secret",
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := validate(raw); err == nil {
		t.Fatal("validate accepted ws:// without allow_insecure_ws")
	} else if !strings.Contains(err.Error(), "allow_insecure_ws") {
		t.Fatalf("unexpected error: %v", err)
	}

	settings := &app.Settings{Transport: raw}
	if _, err := build(context.Background(), settings, "client", testLogger{}, nil); err == nil {
		t.Fatal("build accepted ws:// without allow_insecure_ws")
	}
}

func TestValidateAllowsInsecureWSWithFlag(t *testing.T) {
	raw, err := json.Marshal(Config{
		ServerURL:       "ws://example.test",
		Room:            "room-1",
		Password:        "secret",
		AllowInsecureWS: true,
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := validate(raw); err != nil {
		t.Fatalf("validate rejected ws:// with allow_insecure_ws: %v", err)
	}
}

func TestValidateAllowsWSS(t *testing.T) {
	for _, serverURL := range []string{"wss://example.test", "https://example.test"} {
		raw, err := json.Marshal(Config{
			ServerURL: serverURL,
			Room:      "room-1",
			Password:  "secret",
		})
		if err != nil {
			t.Fatalf("marshal config: %v", err)
		}
		if err := validate(raw); err != nil {
			t.Fatalf("validate rejected %s: %v", serverURL, err)
		}
	}
}

func TestValidateSecureServerURLRejectsUnknownScheme(t *testing.T) {
	if err := validateSecureServerURL("ftp://example.test", false); err == nil {
		t.Fatal("validateSecureServerURL accepted an unknown scheme")
	}
}
