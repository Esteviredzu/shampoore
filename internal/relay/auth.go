package relay

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	SubprotocolTelemetry = "telemetry.v1"

	authTokenVersion       = "v1"
	authTokenWindowMinutes = 2
	authNonceBytes         = 12
	maxAuthTokenLen        = 160
)

func BuildAuthToken(room, password string, now time.Time) string {
	minute := now.UTC().Unix() / 60
	nonce := make([]byte, authNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		var fallback [8]byte
		binary.BigEndian.PutUint64(fallback[:], uint64(now.UTC().UnixNano()))
		copy(nonce, fallback[:])
	}
	nonceHex := hex.EncodeToString(nonce)
	minuteStr := strconv.FormatInt(minute, 10)
	return authTokenVersion + "." + minuteStr + "." + nonceHex + "." +
		signAuthToken(room, minuteStr, nonceHex, password)
}

func AuthProtocols(room, password string, now time.Time) []string {
	if strings.TrimSpace(password) == "" {
		return []string{SubprotocolTelemetry}
	}
	return []string{SubprotocolTelemetry, BuildAuthToken(room, password, now)}
}

func VerifyAuthToken(room, password, token string, now time.Time) bool {
	if room == "" || password == "" {
		return false
	}
	token = strings.TrimSpace(token)
	if token == "" || len(token) > maxAuthTokenLen {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0] != authTokenVersion {
		return false
	}
	minuteStr := parts[1]
	minute, err := strconv.ParseInt(minuteStr, 10, 64)
	if err != nil {
		return false
	}
	nonceHex := parts[2]
	if len(nonceHex) < 2 || len(nonceHex) > 64 {
		return false
	}
	if _, err := hex.DecodeString(nonceHex); err != nil {
		return false
	}
	current := now.UTC().Unix() / 60
	diff := current - minute
	if diff > authTokenWindowMinutes || diff < -authTokenWindowMinutes {
		return false
	}
	want := signAuthToken(room, minuteStr, nonceHex, password)
	return hmac.Equal([]byte(want), []byte(strings.ToLower(parts[3])))
}

func signAuthToken(room, minuteStr, nonceHex, password string) string {
	mac := hmac.New(sha256.New, []byte(password))
	_, _ = mac.Write([]byte(room + ":" + minuteStr + ":" + nonceHex))
	return hex.EncodeToString(mac.Sum(nil))
}

func wsProtocols(r *http.Request) []string {
	var out []string
	for _, value := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, part := range strings.Split(value, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

func authTokenFromProtocols(protocols []string) string {
	for _, p := range protocols {
		if strings.HasPrefix(p, authTokenVersion+".") {
			return p
		}
	}
	return ""
}

func authTokenFromRequest(r *http.Request) string {
	return authTokenFromProtocols(wsProtocols(r))
}
