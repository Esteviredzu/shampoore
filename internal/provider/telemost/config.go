package telemost

import (
	"errors"
	"fmt"
)

type Config struct {
	ConferenceID string `json:"conference_id,omitempty"`
	ConferenceLink string `json:"conference_link,omitempty"`

	DisplayName string `json:"display_name,omitempty"`
	AppVersion  string `json:"app_version,omitempty"`
	SDKVersion  string `json:"sdk_version,omitempty"`

	Role string `json:"role,omitempty"`

	UseSharing bool `json:"use_sharing,omitempty"`

	LocalSignalingURL string `json:"local_signaling_url,omitempty"`
}

func DefaultConfig() Config {
	return Config{
		DisplayName: "Гость",
		AppVersion:  "213.7.0",
		SDKVersion:  "5.28.0",
		Role:        "SPEAKER",
	}
}

func (c *Config) EffectiveConferenceLink() string {
	if c.ConferenceID != "" {
		return "https://telemost.yandex.ru/j/" + c.ConferenceID
	}
	return c.ConferenceLink
}

func (c *Config) Validate() error {
	if c.LocalSignalingURL == "" && c.ConferenceID == "" && c.ConferenceLink == "" {
		return errors.New("telemost: conference_id or local_signaling_url is required")
	}
	if c.ConferenceID != "" && !isValidConferenceID(c.ConferenceID) {
		return fmt.Errorf("telemost: invalid conference_id %q", c.ConferenceID)
	}
	if c.Role == "" {
		c.Role = "SPEAKER"
	}
	if c.Role != "SPEAKER" {
		return errors.New("telemost: only SPEAKER role is supported")
	}
	return nil
}

func (c *Config) EffectiveDisplayName() string {
	if c.DisplayName == "" {
		return "Гость"
	}
	return c.DisplayName
}

func isValidConferenceID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for _, ch := range id {
		switch {
		case ch >= '0' && ch <= '9':
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch == '-' || ch == '_':
		default:
			return false
		}
	}
	return true
}