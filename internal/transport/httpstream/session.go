package httpstream

import (
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/logging"
)

const tokenJWTHeader = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"

const (
	RoleClient = "client"

	RoleExit = "exit"
)

const (
	defaultMaxMessageSize   = 16 << 10
	defaultAckInterval      = 2 * time.Second
	defaultMaxUplinkRetries = 1
	defaultReconnectDelay   = 500 * time.Millisecond
	defaultReconnectMax     = 15 * time.Second

	defaultReadIdleTimeout       = 28 * time.Second
	defaultTokenTTL              = 5 * time.Minute
	defaultTokenLeeway           = 90 * time.Second
	defaultSessionTTL            = 2 * time.Minute
	defaultMaxSessions           = 10000
	defaultMaxUplinkBodyBytes    = 1 << 20
	defaultReorderTimeout        = 2 * time.Second
	defaultReorderCapacity       = 4096
	defaultReorderMaxBytes       = 16 << 20
	defaultDownlinkReplayBytes   = 4 << 20
	defaultKeepAliveInterval     = 5 * time.Second
	defaultStableSessionLifetime = 15 * time.Second

	defaultDeliveryTimeout = 30 * time.Second
)

type SessionInfo struct {
	SessionID  string
	Namespace  string
	UserID     string
	ExitNodeID string
	Role       string
}

type tokenPayload struct {
	Version   int    `json:"v"`
	SessionID string `json:"sid"`
	Namespace string `json:"ns,omitempty"`
	UserID    string `json:"uid,omitempty"`
	ExitNode  string `json:"exit,omitempty"`
	Role      string `json:"role,omitempty"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	Nonce     string `json:"n,omitempty"`
}

func normalizeRole(value string) (string, error) {
	switch role := strings.ToLower(strings.TrimSpace(value)); role {
	case "":
		return RoleClient, nil
	case RoleClient, RoleExit:
		return role, nil
	default:
		return "", fmt.Errorf("httpstream: unsupported session role %q", value)
	}
}

func roleAllowed(allowed []string, role string) bool {
	if len(allowed) == 0 {
		return role == "" || role == RoleClient
	}
	for _, item := range allowed {
		if strings.EqualFold(strings.TrimSpace(item), role) {
			return true
		}
	}
	return false
}

func signSessionToken(secret string, info SessionInfo, now time.Time, ttl time.Duration) (string, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return "", errors.New("httpstream: session secret is empty")
	}
	if _, err := parseSessionID(info.SessionID); err != nil {
		return "", err
	}
	role, err := normalizeRole(info.Role)
	if err != nil {
		return "", err
	}
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	payload := tokenPayload{
		Version:   1,
		SessionID: strings.ToLower(strings.TrimSpace(info.SessionID)),
		Namespace: strings.TrimSpace(info.Namespace),
		UserID:    strings.TrimSpace(info.UserID),
		ExitNode:  strings.TrimSpace(info.ExitNodeID),
		Role:      role,
		IssuedAt:  now.UTC().Unix(),
		ExpiresAt: now.Add(ttl).UTC().Unix(),
		Nonce:     randomHex(8),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(raw)
	signed := tokenJWTHeader + "." + body
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signed))
	return signed + "." + hex.EncodeToString(mac.Sum(nil)), nil
}

func verifySessionToken(secret, raw string, now time.Time, maxTTL, leeway time.Duration, namespace string) (SessionInfo, error) {
	var info SessionInfo
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return info, errors.New("httpstream: session secret is empty")
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > 4096 {
		return info, errors.New("httpstream: invalid session token")
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return info, errors.New("httpstream: invalid session token format")
	}
	signed := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signed))
	want := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(strings.ToLower(parts[2]))) {
		return info, errors.New("httpstream: bad session token signature")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return info, fmt.Errorf("httpstream: decode session token: %w", err)
	}
	var p tokenPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return info, fmt.Errorf("httpstream: parse session token: %w", err)
	}
	if p.Version != 1 {
		return info, fmt.Errorf("httpstream: unsupported session token version %d", p.Version)
	}
	if leeway < 0 {
		leeway = 0
	}
	if p.IssuedAt > now.Add(leeway).Unix() {
		return info, errors.New("httpstream: session token issued in the future")
	}
	if p.ExpiresAt > 0 && now.Unix() > p.ExpiresAt+int64(leeway/time.Second) {
		return info, errors.New("httpstream: session token expired")
	}
	if p.ExpiresAt < p.IssuedAt {
		return info, errors.New("httpstream: session token expires before it was issued")
	}
	if maxTTL > 0 && p.ExpiresAt-p.IssuedAt > int64((maxTTL+leeway)/time.Second) {
		return info, errors.New("httpstream: session token lifetime exceeds the configured limit")
	}
	if namespace != "" && strings.TrimSpace(p.Namespace) != namespace {
		return info, errors.New("httpstream: session namespace mismatch")
	}
	if _, err := parseSessionID(p.SessionID); err != nil {
		return info, err
	}
	role, err := normalizeRole(p.Role)
	if err != nil {
		return info, err
	}
	info = SessionInfo{
		SessionID:  strings.ToLower(strings.TrimSpace(p.SessionID)),
		Namespace:  strings.TrimSpace(p.Namespace),
		UserID:     strings.TrimSpace(p.UserID),
		ExitNodeID: strings.TrimSpace(p.ExitNode),
		Role:       role,
	}
	return info, nil
}

func parseSessionID(value string) ([16]byte, error) {
	var sid [16]byte
	value = strings.TrimSpace(value)
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != len(sid) {
		return sid, fmt.Errorf("httpstream: invalid session id %q", value)
	}
	copy(sid[:], raw)
	return sid, nil
}

func newSessionID() (string, [16]byte, error) {
	var sid [16]byte
	if _, err := cryptorand.Read(sid[:]); err != nil {
		return "", sid, fmt.Errorf("httpstream: generate session id: %w", err)
	}
	return hex.EncodeToString(sid[:]), sid, nil
}

func randomUint32() uint32 {
	var buf [4]byte
	if _, err := cryptorand.Read(buf[:]); err != nil {
		return uint32(time.Now().UnixNano())
	}
	v := uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3])
	if v == 0 {
		v = 1
	}
	return v
}

func randomHex(n int) string {
	if n <= 0 {
		n = 8
	}
	buf := make([]byte, n)
	if _, err := cryptorand.Read(buf); err != nil {
		v := uint64(time.Now().UnixNano())
		for i := range buf {
			buf[i] = byte(v)
			v >>= 8
		}
	}
	return hex.EncodeToString(buf)
}

func (p MimicProfile) applySessionToken(req *http.Request, token string) error {
	if req == nil {
		return errors.New("httpstream: nil request")
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return errors.New("httpstream: empty session token")
	}
	switch p.SessionPlacement.Mode {
	case PlaceCookie:
		req.AddCookie(&http.Cookie{Name: p.SessionPlacement.Name, Value: token, Path: "/"})
	case PlaceBearer:
		req.Header.Set("Authorization", "Bearer "+token)
	case PlaceHeader:
		req.Header.Set(p.SessionPlacement.Name, token)
	case PlaceQuery:
		q := req.URL.Query()
		q.Set(p.SessionPlacement.Name, token)
		req.URL.RawQuery = q.Encode()
	default:
		return fmt.Errorf("httpstream: unsupported session placement %q", p.SessionPlacement.Mode)
	}
	return nil
}

func (p MimicProfile) extractSessionToken(r *http.Request) string {
	if r == nil {
		return ""
	}
	switch p.SessionPlacement.Mode {
	case PlaceCookie:
		cookie, err := r.Cookie(p.SessionPlacement.Name)
		if err != nil || cookie == nil {
			return ""
		}
		return strings.TrimSpace(cookie.Value)
	case PlaceBearer:
		value := strings.TrimSpace(r.Header.Get("Authorization"))
		if len(value) < 7 || !strings.EqualFold(value[:7], "Bearer ") {
			return ""
		}
		return strings.TrimSpace(value[7:])
	case PlaceHeader:
		return strings.TrimSpace(r.Header.Get(p.SessionPlacement.Name))
	case PlaceQuery:
		return strings.TrimSpace(r.URL.Query().Get(p.SessionPlacement.Name))
	default:
		return ""
	}
}

func requestCursor(r *http.Request) uint64 {
	if r == nil {
		return 0
	}
	if value := strings.TrimSpace(r.URL.Query().Get(cursorQueryParam)); value != "" {
		if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
			return parsed
		}
	}
	if value := strings.TrimSpace(r.Header.Get(cursorHeaderName)); value != "" {
		if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
			return parsed
		}
	}
	return 0
}

func setNoBufferHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Cache-Control", "no-store, no-transform")
	h.Set("Pragma", "no-cache")
	h.Set("Expires", "0")
	h.Set("X-Accel-Buffering", "no")
}

func setStreamHeaders(w http.ResponseWriter) {
	setNoBufferHeaders(w)
	w.Header().Set("Content-Type", "application/octet-stream")
}

func flushResponse(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

type ClientConfig struct {
	BaseURL    string
	Secret     string
	Namespace  string
	SessionID  string
	UserID     string
	ExitNodeID string
	Role       string

	HTTPClient *http.Client
	Logger     logging.ContextLogger

	AllowInsecureHTTP bool

	MaxMessageSize   int
	MaxUplinkRetries int
	AckInterval      time.Duration
	ReadIdleTimeout  time.Duration
	TokenTTL         time.Duration
	ReconnectDelay   time.Duration
	ReconnectMax     time.Duration
}

type ServerConfig struct {
	Secret    string
	Namespace string

	PinnedUserID     string
	PinnedExitNodeID string
	RequireUser      bool

	AllowedRoles []string

	SessionSeed func(info SessionInfo) (epoch uint32, delivered uint64)

	Authorize func(ctx context.Context, info SessionInfo) error

	FallbackHandler http.Handler
	Logger          logging.ContextLogger

	TokenTTL    time.Duration
	TokenLeeway time.Duration
	SessionTTL  time.Duration
	MaxSessions int

	MaxUplinkBodyBytes  int
	MaxMessageSize      int
	ReorderTimeout      time.Duration
	ReorderCapacity     int
	ReorderMaxBytes     int
	DownlinkReplayBytes int
	PollHold            time.Duration
	KeepAliveInterval   time.Duration

	DeliveryTimeout time.Duration

	ReapStaleSessions bool
}

func normalizeClientConfig(profile MimicProfile, cfg ClientConfig) (ClientConfig, error) {
	cfg.BaseURL = strings.TrimSpace(cfg.BaseURL)
	cfg.Secret = strings.TrimSpace(cfg.Secret)
	cfg.Namespace = strings.TrimSpace(cfg.Namespace)
	cfg.SessionID = strings.TrimSpace(cfg.SessionID)
	cfg.UserID = strings.TrimSpace(cfg.UserID)
	cfg.ExitNodeID = strings.TrimSpace(cfg.ExitNodeID)
	role, err := normalizeRole(cfg.Role)
	if err != nil {
		return cfg, err
	}
	cfg.Role = role

	if cfg.BaseURL == "" {
		return cfg, errors.New("httpstream: base URL is required")
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || u.Host == "" {
		return cfg, fmt.Errorf("httpstream: invalid base URL %q", cfg.BaseURL)
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
	case "http":
		if !cfg.AllowInsecureHTTP {
			return cfg, errors.New("httpstream: insecure http:// is disabled")
		}
	default:
		return cfg, fmt.Errorf("httpstream: unsupported base URL scheme %q", u.Scheme)
	}
	if cfg.Secret == "" {
		return cfg, errors.New("httpstream: session secret is required")
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}
	if cfg.SessionID == "" {
		id, _, err := newSessionID()
		if err != nil {
			return cfg, err
		}
		cfg.SessionID = id
	} else if _, err := parseSessionID(cfg.SessionID); err != nil {
		return cfg, err
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = defaultMaxMessageSize
	}
	if cfg.MaxMessageSize > profile.MaxUplinkBatchBytes {
		return cfg, fmt.Errorf("httpstream: max message size %d exceeds uplink batch budget %d",
			cfg.MaxMessageSize, profile.MaxUplinkBatchBytes)
	}
	if cfg.MaxUplinkRetries <= 0 {
		cfg.MaxUplinkRetries = defaultMaxUplinkRetries
	}
	if cfg.AckInterval <= 0 {
		cfg.AckInterval = profile.AckInterval
	}
	if cfg.AckInterval <= 0 {
		cfg.AckInterval = defaultAckInterval
	}
	if cfg.ReadIdleTimeout <= 0 {
		cfg.ReadIdleTimeout = defaultReadIdleTimeout
	}
	if cfg.TokenTTL <= 0 {
		cfg.TokenTTL = defaultTokenTTL
	}
	if cfg.ReconnectDelay <= 0 {
		cfg.ReconnectDelay = defaultReconnectDelay
	}
	if cfg.ReconnectMax <= 0 {
		cfg.ReconnectMax = defaultReconnectMax
	}
	if cfg.ReconnectMax < cfg.ReconnectDelay {
		cfg.ReconnectMax = cfg.ReconnectDelay
	}
	if cfg.Logger == nil {
		cfg.Logger = logging.New("httpstream", false)
	}
	return cfg, nil
}

func normalizeServerConfig(profile MimicProfile, cfg ServerConfig) (ServerConfig, error) {
	cfg.Secret = strings.TrimSpace(cfg.Secret)
	cfg.Namespace = strings.TrimSpace(cfg.Namespace)
	cfg.PinnedUserID = strings.TrimSpace(cfg.PinnedUserID)
	cfg.PinnedExitNodeID = strings.TrimSpace(cfg.PinnedExitNodeID)
	if cfg.Secret == "" {
		return cfg, errors.New("httpstream: session secret is required")
	}
	if len(cfg.AllowedRoles) == 0 {
		cfg.AllowedRoles = []string{RoleClient}
	} else {
		roles := make([]string, 0, len(cfg.AllowedRoles))
		for _, raw := range cfg.AllowedRoles {
			role, err := normalizeRole(raw)
			if err != nil {
				return cfg, err
			}
			roles = append(roles, role)
		}
		cfg.AllowedRoles = roles
	}
	if cfg.TokenTTL <= 0 {
		cfg.TokenTTL = defaultTokenTTL
	}
	if cfg.TokenLeeway < 0 {
		cfg.TokenLeeway = 0
	}
	if cfg.TokenLeeway == 0 {
		cfg.TokenLeeway = defaultTokenLeeway
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = defaultSessionTTL
	}
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = defaultMaxSessions
	}
	if cfg.MaxUplinkBodyBytes <= 0 {
		cfg.MaxUplinkBodyBytes = defaultMaxUplinkBodyBytes
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = defaultMaxMessageSize
	}
	if cfg.ReorderTimeout <= 0 {
		cfg.ReorderTimeout = defaultReorderTimeout
	}
	if cfg.ReorderCapacity <= 0 {
		cfg.ReorderCapacity = defaultReorderCapacity
	}
	if cfg.ReorderMaxBytes <= 0 {
		cfg.ReorderMaxBytes = defaultReorderMaxBytes
	}
	if cfg.DownlinkReplayBytes <= 0 {
		cfg.DownlinkReplayBytes = defaultDownlinkReplayBytes
	}
	if cfg.PollHold <= 0 {
		cfg.PollHold = profile.PollHold
	}
	if cfg.KeepAliveInterval <= 0 {
		cfg.KeepAliveInterval = defaultKeepAliveInterval
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = defaultDeliveryTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = logging.New("httpstream", false)
	}
	return cfg, nil
}
