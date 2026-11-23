package panelstore

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/capability"
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

func generateSecret(gen capability.SecretGenerator) (string, error) {
	switch gen.Kind {
	case "literal":
		return gen.Prefix, nil
	case "base64":
		value, err := randomBase64(gen.Bytes)
		if err != nil {
			return "", err
		}
		return gen.Prefix + value, nil
	case "hex", "":
		value, err := randomHex(gen.Bytes)
		if err != nil {
			return "", err
		}
		return gen.Prefix + value, nil
	default:
		value, err := randomHex(24)
		if err != nil {
			return "", err
		}
		return gen.Prefix + value, nil
	}
}

func applyRequestedSecrets(n *Node, policy TransportPolicy) error {
	if len(n.Secrets) == 0 {
		return nil
	}
	if n.GeneratedSecrets == nil {
		n.GeneratedSecrets = make(map[string]string)
	}
	specs := secretSpecIndex(policy)
	for rawKey, rawValue := range n.Secrets {
		key := strings.TrimSpace(rawKey)
		spec, ok := specs[key]
		if !ok || !spec.Editable || !policy.EditableSecret(key) {
			return invalidf("node %q: unsupported secret %q", n.Name, rawKey)
		}
		if err := validateSecretValue(key, rawValue); err != nil {
			return fmt.Errorf("node %q: %w", n.Name, err)
		}
		n.GeneratedSecrets[key] = rawValue
		for _, alias := range spec.Aliases {
			n.GeneratedSecrets[alias] = rawValue
		}
	}
	return nil
}

func pickNodeSecrets(gen map[string]string, policy TransportPolicy) map[string]string {
	specs := policy.Secrets()
	out := make(map[string]string, len(specs))
	for _, spec := range specs {
		if !spec.Editable {
			continue
		}
		out[spec.Key] = gen[spec.Key]
	}
	return out
}

func hydrateNodeView(n *Node, policy TransportPolicy) {
	n.EncryptionCipher = EncryptionCipherName
	n.Secrets = pickNodeSecrets(n.GeneratedSecrets, policy)
}

func ensureSecrets(n *Node, policy TransportPolicy) error {
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
	specs := policy.Secrets()
	for _, spec := range specs {
		if spec.Key == "" || gen[spec.Key] != "" {
			continue
		}
		value, err := generateSecret(spec.Generator)
		if err != nil {
			return err
		}
		gen[spec.Key] = value
	}
	for _, spec := range specs {
		value := gen[spec.Key]
		if value == "" {
			continue
		}
		for _, alias := range spec.Aliases {
			if gen[alias] == "" {
				gen[alias] = value
			}
		}
	}
	return nil
}

func secretSpecIndex(policy TransportPolicy) map[string]capability.SecretSpec {
	specs := policy.Secrets()
	out := make(map[string]capability.SecretSpec, len(specs))
	for _, spec := range specs {
		out[spec.Key] = spec
	}
	return out
}
