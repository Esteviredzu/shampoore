package galene

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/Esteviredzu/shampoore-core/internal/names"
)

type Config struct {
	GroupURL string `json:"group_url"`
	Group    string `json:"group,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	TurnURL  string `json:"turn_url,omitempty"`
}

func (c *Config) Validate() error {
	if c.GroupURL == "" {
		return fmt.Errorf("galene: group_url is required")
	}
	if c.Group == "" {
		u, err := url.Parse(c.GroupURL)
		if err != nil {
			return fmt.Errorf("galene: parse group_url: %w", err)
		}
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		for i, p := range parts {
			if p == "group" && i+1 < len(parts) {
				c.Group = parts[i+1]
				break
			}
		}
		if c.Group == "" {
			return fmt.Errorf("galene: cannot determine group from URL")
		}
	}
	if c.Username == "" {
		c.Username = names.Random()
	}
	return nil
}
