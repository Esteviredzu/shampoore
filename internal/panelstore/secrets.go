package panelstore

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("panelstore: random: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func randomBase64(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("panelstore: random: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}

func ensureSecrets(n *Node) error {
	if n.NodeSecret == "" {
		secret, err := randomHex(32)
		if err != nil {
			return err
		}
		n.NodeSecret = secret
	}
	if n.GeneratedSecrets == nil {
		n.GeneratedSecrets = make(map[string]string)
	}
	gen := n.GeneratedSecrets

	needed := []struct {
		key string
		gen func() (string, error)
	}{
		{"encryption_password", func() (string, error) { return randomHex(32) }},
		{"raw_ws_password", func() (string, error) { return randomHex(32) }},
		{"livekit_api_key", func() (string, error) { return randomHex(8) }},
		{"livekit_api_secret", func() (string, error) { return randomBase64(32) }},
		{"livekit_token_secret", func() (string, error) { return randomHex(32) }},
		{"livekit_token_listen", func() (string, error) { return "127.0.0.1:8099", nil }},
		{"minio_root_user", func() (string, error) {
			token, err := randomHex(6)
			return "shampoore-" + token, err
		}},
		{"minio_root_password", func() (string, error) { return randomHex(24) }},
		{"s3_bucket", func() (string, error) {
			token, err := randomHex(4)
			return "shampoore-" + token, err
		}},
	}
	for _, item := range needed {
		if gen[item.key] != "" {
			continue
		}
		value, err := item.gen()
		if err != nil {
			return err
		}
		gen[item.key] = value
	}
	if gen["tunnel_id"] == "" {
		gen["tunnel_id"] = "shampoore"
	}

	if gen["s3_access_key"] == "" {
		gen["s3_access_key"] = gen["minio_root_user"]
	}
	if gen["s3_secret_key"] == "" {
		gen["s3_secret_key"] = gen["minio_root_password"]
	}
	return nil
}
