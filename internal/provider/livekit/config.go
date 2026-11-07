package livekit

import "fmt"

type Config struct {
	SignalURL string `json:"signal_url"`
	APIKey    string `json:"api_key"`
	APISecret string `json:"api_secret"`
	Room      string `json:"room"`
	Identity  string `json:"identity,omitempty"`
	DataTopic string `json:"data_topic,omitempty"`
}

func (c *Config) Validate() error {
	if c.SignalURL == "" {
		return fmt.Errorf("livekit: signal_url is required")
	}
	if c.APIKey == "" || c.APISecret == "" {
		return fmt.Errorf("livekit: api_key and api_secret are required")
	}
	if c.Room == "" {
		return fmt.Errorf("livekit: room is required")
	}
	if c.DataTopic == "" {
		c.DataTopic = "shampoore"
	}
	return nil
}
