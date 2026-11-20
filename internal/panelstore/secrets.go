package panelstore

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

var editableSecretKeys = []string{
	"encryption_password",
	"raw_ws_password",
	"livekit_api_key",
	"livekit_api_secret",
	"minio_root_user",
	"minio_root_password",
	"s3_bucket",
}

func editableSecret(key string) bool {
	for _, candidate := range editableSecretKeys {
		if key == candidate {
			return true
		}
	}
	return false
}

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

func validateSecretValue(key, value string) error {
	if strings.TrimSpace(value) == "" {
		return invalidf("secret %q must not be empty", key)
	}
	if len(value) > 512 {
		return invalidf("secret %q is too long", key)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return invalidf("secret %q contains control characters", key)
		}
	}
	return nil
}

func applyRequestedSecrets(n *Node) error {
	if len(n.Secrets) == 0 {
		return nil
	}
	if n.GeneratedSecrets == nil {
		n.GeneratedSecrets = make(map[string]string)
	}
	for rawKey, rawValue := range n.Secrets {
		key := strings.TrimSpace(rawKey)
		if !editableSecret(key) {
			return invalidf("node %q: unsupported secret %q", n.Name, rawKey)
		}
		if err := validateSecretValue(key, rawValue); err != nil {
			return fmt.Errorf("node %q: %w", n.Name, err)
		}
		n.GeneratedSecrets[key] = rawValue
		switch key {
		case "minio_root_user":
			n.GeneratedSecrets["s3_access_key"] = rawValue
		case "minio_root_password":
			n.GeneratedSecrets["s3_secret_key"] = rawValue
		}
	}
	return nil
}

func pickNodeSecrets(gen map[string]string) map[string]string {
	out := make(map[string]string, len(editableSecretKeys))
	for _, key := range editableSecretKeys {
		out[key] = gen[key]
	}
	return out
}

func hydrateNodeView(n *Node) {
	n.EncryptionCipher = EncryptionCipherName
	n.Secrets = pickNodeSecrets(n.GeneratedSecrets)
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
