package silo

import (
	"errors"
	"fmt"
	"strings"
)

type Config struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Bucket    string `json:"bucket"`
	UseSSL    bool   `json:"use_ssl"`
	TunnelID  string `json:"tunnel_id"`

	EncryptionPassword string `json:"encryption_password,omitempty"`

	MinFileSizeKB  int `json:"min_file_size_kb"`
	MaxFileSizeKB  int `json:"max_file_size_kb"`
	FlushMs        int `json:"flush_ms"`
	PollIntervalMs int `json:"poll_interval_ms"`
}

func DefaultConfig() Config {
	return Config{
		Endpoint:       "s3.kakaobaobab.casa",
		Bucket:         "shampoore-tunnel",
		UseSSL:         true,
		TunnelID:       "default",
		MinFileSizeKB:  256,
		MaxFileSizeKB:  512,
		FlushMs:        50,
		PollIntervalMs: 10,
	}
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("silo: endpoint is required")
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return errors.New("silo: access_key and secret_key are required")
	}
	if c.Bucket == "" {
		return errors.New("silo: bucket is required")
	}
	if c.TunnelID == "" {
		c.TunnelID = "default"
	}
	if c.EncryptionPassword != "" && strings.TrimSpace(c.EncryptionPassword) == "" {
		return errors.New("silo: encryption_password must not be blank")
	}
	if c.MinFileSizeKB <= 0 || c.MaxFileSizeKB < c.MinFileSizeKB {
		return fmt.Errorf("silo: invalid file size bounds (min=%d KB, max=%d KB)", c.MinFileSizeKB, c.MaxFileSizeKB)
	}
	if c.FlushMs <= 0 {
		c.FlushMs = 50
	}
	if c.PollIntervalMs <= 0 {
		c.PollIntervalMs = 10
	}
	return nil
}
