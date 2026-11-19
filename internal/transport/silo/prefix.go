package silo

import (
	"fmt"
	"strings"
)

func sanitizeUserID(userID string) (string, error) {
	id := strings.TrimSpace(userID)
	if id == "" {
		return "", nil
	}
	if len(id) > 64 {
		return "", fmt.Errorf("silo: user_id is too long (%d > 64)", len(id))
	}
	if id == "." || id == ".." {
		return "", fmt.Errorf("silo: invalid user_id %q", id)
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return "", fmt.Errorf("silo: user_id contains invalid character %q", r)
		}
	}
	return id, nil
}

func UserPrefix(userID string) string {
	return "users/" + userID + "/"
}

func buildPrefixes(cfg *Config, role string) (writePrefix, readPrefix string) {
	base := cfg.TunnelID
	if cfg.UserID != "" {
		base = "users/" + cfg.UserID
	}
	writePrefix = base + "/c2s/"
	readPrefix = base + "/s2c/"
	if role == "server" {
		writePrefix, readPrefix = readPrefix, writePrefix
	}
	return writePrefix, readPrefix
}

func ParseUserFromKey(key string) (string, bool) {
	if !strings.HasPrefix(key, "users/") {
		return "", false
	}
	rest := strings.TrimPrefix(key, "users/")
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		return "", false
	}
	return rest[:slash], true
}
