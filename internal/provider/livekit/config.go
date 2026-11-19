package livekit

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/auth"
)

const (
	defaultDataTopic      = "shampoore"
	defaultRoomPrefix     = "shampoore"
	defaultIdleTimeout    = 5 * time.Minute
	defaultClientTokenTTL = 10 * time.Minute
	defaultServerTokenTTL = time.Hour
)

// Config is the transport configuration for the LiveKit data channel.
//
// The legacy fields (signal_url, api_key, api_secret, room) keep the original
// single-user behaviour intact. New multiuser / on-demand fields are only
// consulted when explicitly set.
type Config struct {
	SignalURL string `json:"signal_url"`
	APIKey    string `json:"api_key"`
	APISecret string `json:"api_secret"`
	Room      string `json:"room"`
	Identity  string `json:"identity,omitempty"`
	DataTopic string `json:"data_topic,omitempty"`

	UserID     string `json:"user_id,omitempty"`
	ExitNodeID string `json:"exit_node_id,omitempty"`

	// Token may contain a pre-issued LiveKit JWT. When set, api_key and
	// api_secret are not required for a client.
	Token string `json:"token,omitempty"`

	Multiuser bool            `json:"multiuser,omitempty"`
	Users     []auth.UserSpec `json:"users,omitempty"`

	RoomPrefix string `json:"room_prefix,omitempty"`

	// TokenListen is the address (or HTTP URL) the multiuser server binds
	// its session endpoint to, for example "127.0.0.1:8099" or
	// "http://127.0.0.1:8099/session".
	TokenListen string `json:"token_listen,omitempty"`
	// TokenURL is the client endpoint that issues on-demand sessions.
	TokenURL string `json:"token_url,omitempty"`
	// TokenSecret protects the session endpoint. It is sent in the
	// X-Shampoore-Secret header (or as a Bearer token).
	TokenSecret string `json:"token_secret,omitempty"`

	IdleTimeoutMs         int `json:"idle_timeout_ms,omitempty"`
	TokenTTLSeconds       int `json:"token_ttl_seconds,omitempty"`
	ServerTokenTTLSeconds int `json:"server_token_ttl_seconds,omitempty"`
}

func (c *Config) applyDefaults() {
	if strings.TrimSpace(c.DataTopic) == "" {
		c.DataTopic = defaultDataTopic
	}
	if strings.TrimSpace(c.RoomPrefix) == "" {
		c.RoomPrefix = defaultRoomPrefix
	}
	if c.IdleTimeoutMs <= 0 {
		c.IdleTimeoutMs = int(defaultIdleTimeout / time.Millisecond)
	}
	if c.TokenTTLSeconds <= 0 {
		c.TokenTTLSeconds = int(defaultClientTokenTTL / time.Second)
	}
	if c.ServerTokenTTLSeconds <= 0 {
		c.ServerTokenTTLSeconds = int(defaultServerTokenTTL / time.Second)
	}
}

func (c *Config) idleTimeout() time.Duration {
	if c.IdleTimeoutMs <= 0 {
		return defaultIdleTimeout
	}
	return time.Duration(c.IdleTimeoutMs) * time.Millisecond
}

func (c *Config) clientTokenTTL() time.Duration {
	if c.TokenTTLSeconds <= 0 {
		return defaultClientTokenTTL
	}
	return time.Duration(c.TokenTTLSeconds) * time.Second
}

func (c *Config) serverTokenTTL() time.Duration {
	if c.ServerTokenTTLSeconds <= 0 {
		return defaultServerTokenTTL
	}
	return time.Duration(c.ServerTokenTTLSeconds) * time.Second
}

func (c *Config) hasTokenEndpoint() bool {
	return strings.TrimSpace(c.TokenURL) != "" || strings.TrimSpace(c.TokenListen) != ""
}

// Validate validates transport settings without knowing the runtime role.
// It is used by the panel/manager before a runtime is built.
func (c *Config) Validate() error {
	c.applyDefaults()

	if strings.TrimSpace(c.SignalURL) == "" {
		// On-demand clients receive the actual URL with the session response.
		if c.Multiuser || !c.hasTokenEndpoint() {
			return fmt.Errorf("livekit: signal_url is required")
		}
	}

	if c.Multiuser {
		if c.APIKey == "" || c.APISecret == "" {
			return fmt.Errorf("livekit: api_key and api_secret are required for multiuser mode")
		}
		if strings.TrimSpace(c.TokenListen) == "" {
			return fmt.Errorf("livekit: token_listen is required for multiuser mode")
		}
		return nil
	}

	if c.hasTokenEndpoint() || strings.TrimSpace(c.Token) != "" {
		// On-demand client or pre-issued token: the room is supplied by the
		// session endpoint (or embedded in the token).
		return nil
	}

	if c.APIKey == "" || c.APISecret == "" {
		return fmt.Errorf("livekit: api_key and api_secret are required")
	}
	if c.Room == "" {
		return fmt.Errorf("livekit: room is required")
	}
	return nil
}

// ValidateForRole performs the role-specific validation used by the
// transport builder.
func (c *Config) ValidateForRole(role string) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if role == "server" && c.Multiuser {
		if _, _, err := parseTokenListen(c.TokenListen); err != nil {
			return err
		}
		return nil
	}

	endpoint := c.tokenEndpoint()
	if endpoint != "" {
		if strings.TrimSpace(c.UserID) == "" {
			return errors.New("livekit: user_id is required when token_url/token_listen is configured")
		}
		if _, err := parseEndpointURL(endpoint); err != nil {
			return err
		}
		return nil
	}

	if strings.TrimSpace(c.Token) == "" {
		if c.APIKey == "" || c.APISecret == "" {
			return fmt.Errorf("livekit: api_key and api_secret are required")
		}
		if c.Room == "" {
			return fmt.Errorf("livekit: room is required")
		}
	}
	return nil
}

// NormalizeTokenURL converts a token endpoint into an absolute HTTP URL.
func NormalizeTokenURL(raw string) string {
	return normalizeTokenURL(raw)
}

func normalizeTokenURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.HasPrefix(raw, "/") {
		return ""
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		u, err := parseEndpointURL(raw)
		if err != nil {
			return raw
		}
		if u.Path == "" {
			u.Path = "/session"
		}
		return u.String()
	}
	return "http://" + raw + "/session"
}

func parseEndpointURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("livekit: invalid token endpoint %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("livekit: token endpoint must use http or https: %q", raw)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("livekit: token endpoint has no host: %q", raw)
	}
	return u, nil
}

// parseTokenListen accepts "host:port", ":port", a path ("/session") or an
// HTTP URL. It returns the TCP listen address and the endpoint path.
func parseTokenListen(raw string) (listenAddr, path string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", errors.New("livekit: token_listen is required")
	}
	if strings.Contains(raw, "://") {
		u, perr := parseEndpointURL(raw)
		if perr != nil {
			return "", "", perr
		}
		path = u.Path
		if path == "" {
			path = "/session"
		}
		return u.Host, path, nil
	}
	if strings.HasPrefix(raw, "/") {
		return "127.0.0.1:8099", raw, nil
	}
	return raw, "/session", nil
}

func (c *Config) tokenEndpoint() string {
	if endpoint := normalizeTokenURL(c.TokenURL); endpoint != "" {
		return endpoint
	}
	return normalizeTokenURL(c.TokenListen)
}
