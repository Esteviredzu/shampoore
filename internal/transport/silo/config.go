package silo

import (
	"errors"
	"fmt"
)

type Config struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Bucket    string `json:"bucket"`
	UseSSL    bool   `json:"use_ssl"`
	TunnelID  string `json:"tunnel_id"`

	MinFileSizeKB int `json:"min_file_size_kb"`
	MaxFileSizeKB int `json:"max_file_size_kb"`
	FlushMs       int `json:"flush_ms"`
	JitterMs      int `json:"jitter_ms"`
}

func DefaultConfig() Config {
	return Config{
		Endpoint:      "s3.kakaobaobab.casa",
		Bucket:        "shampoore-tunnel",
		UseSSL:        true,
		TunnelID:      "default",
		MinFileSizeKB: 10,
		MaxFileSizeKB: 500,
		FlushMs:       100,
		JitterMs:      20,
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
	if c.MinFileSizeKB <= 0 || c.MaxFileSizeKB < c.MinFileSizeKB {
		return fmt.Errorf("silo: invalid file size bounds (min=%d KB, max=%d KB)", c.MinFileSizeKB, c.MaxFileSizeKB)
	}
	if c.FlushMs <= 0 {
		c.FlushMs = 100
	}
	return nil
}