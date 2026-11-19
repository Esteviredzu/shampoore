package livekit

import (
	"fmt"
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
	defaultSyncInterval   = 30 * time.Second
)

type Config struct {
	SignalURL string `json:"signal_url"`
	APIKey    string `json:"api_key"`
	APISecret string `json:"api_secret"`
	Room      string `json:"room"`
	Identity  string `json:"identity,omitempty"`
	DataTopic string `json:"data_topic,omitempty"`

	UserID     string `json:"user_id,omitempty"`
	ExitNodeID string `json:"exit_node_id,omitempty"`

	Token string `json:"token,omitempty"`

	Multiuser bool            `json:"multiuser,omitempty"`
	Users     []auth.UserSpec `json:"users,omitempty"`

	RoomPrefix string `json:"room_prefix,omitempty"`

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

func (c *Config) Validate() error {
	c.applyDefaults()

	if strings.TrimSpace(c.SignalURL) == "" {
		return fmt.Errorf("livekit: signal_url is required")
	}

	if c.Multiuser {
		if c.APIKey == "" || c.APISecret == "" {
			return fmt.Errorf("livekit: api_key and api_secret are required for multiuser mode")
		}
		return nil
	}

	if strings.TrimSpace(c.Token) != "" {
		return nil
	}

	if c.APIKey == "" || c.APISecret == "" {
		return fmt.Errorf("livekit: api_key and api_secret are required")
	}
	if strings.TrimSpace(c.Room) == "" {
		return fmt.Errorf("livekit: room is required")
	}
	return nil
}

func (c *Config) ValidateForRole(role string) error {
	return c.Validate()
}
