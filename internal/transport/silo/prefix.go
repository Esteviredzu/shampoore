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

func namespaceRoot(tunnelID string) string {
	ns := strings.Trim(strings.TrimSpace(tunnelID), "/")
	if ns == "" {
		ns = "default"
	}
	return ns + "/"
}

func UserPrefix(tunnelID, userID string) string {
	return namespaceRoot(tunnelID) + "users/" + userID + "/"
}

func buildPrefixes(cfg *Config, role string) (writePrefix, readPrefix string) {
	base := namespaceRoot(cfg.TunnelID)
	if cfg.UserID != "" {
		base += "users/" + cfg.UserID + "/"
	}
	writePrefix = base + "c2s/"
	readPrefix = base + "s2c/"
	if role == "server" {
		writePrefix, readPrefix = readPrefix, writePrefix
	}
	return writePrefix, readPrefix
}

func C2SPrefix(tunnelID, userID string) string {
	write, _ := buildPrefixes(&Config{TunnelID: tunnelID, UserID: userID}, "client")
	return write
}

func ParseUserFromKey(key string) (string, bool) {
	var rest string
	if strings.HasPrefix(key, "users/") {
		rest = strings.TrimPrefix(key, "users/")
	} else {
		slash := strings.Index(key, "/users/")
		if slash < 0 {
			return "", false
		}
		rest = key[slash+len("/users/"):]
	}
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		return "", false
	}
	return rest[:slash], true
}
