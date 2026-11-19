package panel

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	authCookieName     = "shampoore_panel_token"
	authCookieTTL      = 12 * time.Hour
	loginMaxFailures   = 5
	loginFailureWindow = 5 * time.Minute
	loginBlockDuration = 5 * time.Minute
)

func EnsureToken(token string) (string, error) {
	token = strings.TrimSpace(token)
	if token != "" {
		return token, nil
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("panel: generate token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func tokenEqual(got, want string) bool {
	if got == "" || want == "" || len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

type loginEntry struct {
	failures     int
	windowStart  time.Time
	blockedUntil time.Time
	lastSeen     time.Time
}

type loginLimiter struct {
	mu            sync.Mutex
	now           func() time.Time
	entries       map[string]*loginEntry
	maxFailures   int
	failureWindow time.Duration
	blockDuration time.Duration
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{
		now:           time.Now,
		entries:       make(map[string]*loginEntry),
		maxFailures:   loginMaxFailures,
		failureWindow: loginFailureWindow,
		blockDuration: loginBlockDuration,
	}
}

func (l *loginLimiter) allow(ip string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.pruneLocked(now)
	e := l.entries[ip]
	if e == nil {
		return true, 0
	}
	if now.Before(e.blockedUntil) {
		return false, e.blockedUntil.Sub(now)
	}
	return true, 0
}

func (l *loginLimiter) fail(ip string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.pruneLocked(now)
	e := l.entries[ip]
	if e == nil {
		e = &loginEntry{windowStart: now}
		l.entries[ip] = e
	}
	e.lastSeen = now
	if now.Sub(e.windowStart) > l.failureWindow {
		e.failures = 0
		e.windowStart = now
	}
	e.failures++
	if e.failures >= l.maxFailures {
		e.failures = 0
		e.windowStart = now
		e.blockedUntil = now.Add(l.blockDuration)
		return true, l.blockDuration
	}
	return false, 0
}

func (l *loginLimiter) reset(ip string) {
	l.mu.Lock()
	delete(l.entries, ip)
	l.mu.Unlock()
}

func (l *loginLimiter) pruneLocked(now time.Time) {
	cutoff := now.Add(-2*l.failureWindow - l.blockDuration)
	for ip, e := range l.entries {
		if e.lastSeen.Before(cutoff) && !e.blockedUntil.After(now) {
			delete(l.entries, ip)
		}
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	return r.RemoteAddr
}

func retryAfterSeconds(d time.Duration) string {
	secs := int(d / time.Second)
	if secs < 1 {
		secs = 1
	}
	return fmt.Sprintf("%d", secs)
}

func (a *API) credentials(r *http.Request) (string, bool) {
	if header := r.Header.Get("Authorization"); header != "" {
		const prefix = "bearer "
		if len(header) > len(prefix) && strings.EqualFold(header[:len(prefix)], prefix) {
			return strings.TrimSpace(header[len(prefix):]), true
		}
		return "", true
	}
	if cookie, err := r.Cookie(authCookieName); err == nil {
		return cookie.Value, true
	}
	return "", false
}

func (a *API) authorized(r *http.Request) bool {
	if a.token == "" {
		return false
	}
	got, ok := a.credentials(r)
	if !ok {
		return false
	}
	return tokenEqual(got, a.token)
}

func (a *API) setAuthCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     authCookieName,
		Value:    a.token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(authCookieTTL.Seconds()),
		Expires:  time.Now().Add(authCookieTTL),
		Secure:   r.TLS != nil,
	})
}

func (a *API) clearAuthCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     authCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
		Expires:  time.Unix(1, 0),
	})
}
