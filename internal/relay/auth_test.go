package relay

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func fixedNow() time.Time {
	return time.Unix(1_760_000_000, 0).UTC()
}

func TestBuildAndVerifyAuthToken(t *testing.T) {
	now := fixedNow()
	token := BuildAuthToken("room-1", "secret", now)
	parts := strings.Split(token, ".")
	if len(parts) != 4 {
		t.Fatalf("token = %q", token)
	}
	if parts[0] != authTokenVersion {
		t.Fatalf("version = %q", parts[0])
	}
	if parts[1] != "29333333" {
		t.Fatalf("minute = %q", parts[1])
	}
	if !VerifyAuthToken("room-1", "secret", token, now) {
		t.Fatal("fresh token rejected")
	}

	if !VerifyAuthToken("room-1", "secret", token, now.Add(2*time.Minute)) {
		t.Fatal("token rejected inside the +2m window")
	}
	if !VerifyAuthToken("room-1", "secret", token, now.Add(-2*time.Minute)) {
		t.Fatal("token rejected inside the -2m window")
	}
	if VerifyAuthToken("room-1", "secret", token, now.Add(3*time.Minute)) {
		t.Fatal("stale token accepted")
	}
	if VerifyAuthToken("room-1", "secret", token, now.Add(-3*time.Minute)) {
		t.Fatal("future token accepted")
	}

	for name, mutate := range map[string]func() bool{
		"other room":     func() bool { return VerifyAuthToken("room-2", "secret", token, now) },
		"other password": func() bool { return VerifyAuthToken("room-1", "nope", token, now) },
		"empty password": func() bool { return VerifyAuthToken("room-1", "", token, now) },
		"empty room":     func() bool { return VerifyAuthToken("", "secret", token, now) },
		"missing token":  func() bool { return VerifyAuthToken("room-1", "secret", "", now) },
		"bad format":     func() bool { return VerifyAuthToken("room-1", "secret", "v1.abc", now) },
		"bad version":    func() bool { return VerifyAuthToken("room-1", "secret", "v2"+token[2:], now) },
		"bad nonce":      func() bool { return VerifyAuthToken("room-1", "secret", "v1.29333333.zz.aa", now) },
		"tampered mac":   func() bool { return VerifyAuthToken("room-1", "secret", token[:len(token)-1]+"0", now) },
	} {
		if mutate() {
			t.Fatalf("%s was accepted", name)
		}
	}
}

func TestAuthProtocols(t *testing.T) {
	now := fixedNow()
	protocols := AuthProtocols("room-1", "secret", now)
	if len(protocols) != 2 || protocols[0] != SubprotocolTelemetry {
		t.Fatalf("protocols = %v", protocols)
	}
	if !VerifyAuthToken("room-1", "secret", protocols[1], now) {
		t.Fatal("client token does not verify")
	}
	if got := AuthProtocols("room-1", "", now); len(got) != 1 || got[0] != SubprotocolTelemetry {
		t.Fatalf("passwordless protocols = %v", got)
	}
}

func TestAuthTokenFromRequest(t *testing.T) {
	now := fixedNow()
	token := BuildAuthToken("room-1", "secret", now)
	r := httptest.NewRequest(http.MethodGet, "/ws/stream?room=room-1", nil)
	r.Header.Set("Sec-WebSocket-Protocol", SubprotocolTelemetry+", "+token)
	if got := authTokenFromRequest(r); got != token {
		t.Fatalf("token = %q, want %q", got, token)
	}
	if got := wsProtocols(r); len(got) != 2 {
		t.Fatalf("protocols = %v", got)
	}

	empty := httptest.NewRequest(http.MethodGet, "/ws/stream", nil)
	if got := authTokenFromRequest(empty); got != "" {
		t.Fatalf("unexpected token %q", got)
	}
}

func TestAuthorizedSupportsLegacyPasswordAndHMAC(t *testing.T) {
	now := fixedNow()
	s := New("secret")
	s.now = func() time.Time { return now }

	legacy := httptest.NewRequest(http.MethodGet, "/ws/room-1", nil)
	legacy.Header.Set("X-Room-Password", "secret")
	if !s.authorized(legacy, "room-1") {
		t.Fatal("legacy password rejected")
	}
	query := httptest.NewRequest(http.MethodGet, "/ws/room-1?password=secret", nil)
	if !s.authorized(query, "room-1") {
		t.Fatal("legacy query password rejected")
	}

	token := httptest.NewRequest(http.MethodGet, "/ws/stream", nil)
	token.Header.Set("Sec-WebSocket-Protocol", SubprotocolTelemetry+", "+BuildAuthToken("room-1", "secret", now))
	if !s.authorized(token, "room-1") {
		t.Fatal("HMAC token rejected")
	}

	wrong := httptest.NewRequest(http.MethodGet, "/ws/stream", nil)
	wrong.Header.Set("Sec-WebSocket-Protocol", SubprotocolTelemetry)
	if s.authorized(wrong, "room-1") {
		t.Fatal("request without token was authorized")
	}

	open := New("")
	if !open.authorized(wrong, "anything") {
		t.Fatal("passwordless relay rejected a room join")
	}
}
