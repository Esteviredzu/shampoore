package panelstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const DefaultPublicListen = "127.0.0.1:8443"

type Settings struct {
	PublicDomain         string    `json:"public_domain"`
	PublicURL            string    `json:"public_url"`
	PublicListen         string    `json:"public_listen,omitempty"`
	SubscriptionsEnabled bool      `json:"subscriptions_enabled"`
	UpdatedAt            time.Time `json:"updated_at,omitempty"`

	subscriptionsSet bool
}

func DefaultSettings() Settings {
	return Settings{PublicListen: DefaultPublicListen}
}

func (s *Settings) SetSubscriptionsEnabled(on bool) {
	s.SubscriptionsEnabled = on
	s.subscriptionsSet = true
}

func (s *Settings) Normalize() error {
	domain, err := normalizeDomain(s.PublicDomain)
	if err != nil {
		return err
	}
	s.PublicDomain = domain
	if domain == "" {
		s.SubscriptionsEnabled = false
	} else if !s.subscriptionsSet && !s.SubscriptionsEnabled {
		s.SubscriptionsEnabled = true
	}

	publicURL := strings.TrimSpace(s.PublicURL)
	if publicURL == "" && domain != "" {
		publicURL = "https://" + domain
	}
	if publicURL != "" {
		u, err := url.Parse(publicURL)
		if err != nil {
			return invalidf("public_url: %v", err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return invalidf("public_url must use http or https")
		}
		if strings.TrimSpace(u.Host) == "" {
			return invalidf("public_url has no host")
		}
		u.Path = strings.TrimRight(u.Path, "/")
		publicURL = strings.TrimRight(u.String(), "/")
	}
	s.PublicURL = publicURL

	listen := strings.TrimSpace(s.PublicListen)
	if listen == "" {
		listen = DefaultPublicListen
	}
	if _, _, err := net.SplitHostPort(listen); err != nil {
		return invalidf("public_listen: %v", err)
	}
	s.PublicListen = listen
	return nil
}

func NormalizeDomain(raw string) (string, error) {
	return normalizeDomain(raw)
}

func normalizeDomain(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", invalidf("public_domain: %v", err)
		}
		raw = u.Hostname()
	}
	raw = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(raw)), ".")
	if raw == "" {
		return "", nil
	}
	if len(raw) > 253 {
		return "", invalidf("public_domain is too long")
	}
	if !strings.Contains(raw, ".") {
		return "", invalidf("public_domain must contain a dot")
	}
	for _, label := range strings.Split(raw, ".") {
		if label == "" || len(label) > 63 {
			return "", invalidf("public_domain has an invalid label")
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return "", invalidf("public_domain has an invalid label")
		}
		for _, r := range label {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
				continue
			}
			return "", invalidf("public_domain contains invalid characters")
		}
	}
	return raw, nil
}

func (s *Store) GetSettings(ctx context.Context) (Settings, error) {
	out := DefaultSettings()
	enabledSet := false
	rows, err := s.db.QueryContext(ctx, `SELECT key, value, updated_at FROM settings`)
	if err != nil {
		return Settings{}, wrapDBErr(err, "get settings")
	}
	defer rows.Close()
	for rows.Next() {
		var key, value, updated string
		if err := rows.Scan(&key, &value, &updated); err != nil {
			return Settings{}, wrapDBErr(err, "scan settings")
		}
		switch key {
		case "public_domain":
			out.PublicDomain = value
		case "public_url":
			out.PublicURL = value
		case "public_listen":
			out.PublicListen = value
		case "subscriptions_enabled":
			enabled, parseErr := strconv.ParseBool(strings.TrimSpace(value))
			if parseErr != nil {
				return Settings{}, fmt.Errorf("panelstore: settings: invalid subscriptions_enabled %q: %w", value, parseErr)
			}
			out.SubscriptionsEnabled = enabled
			enabledSet = true
		}
		if ts := parseTime(updated); ts.After(out.UpdatedAt) {
			out.UpdatedAt = ts
		}
	}
	if err := rows.Err(); err != nil {
		return Settings{}, wrapDBErr(err, "list settings")
	}
	if strings.TrimSpace(out.PublicListen) == "" {
		out.PublicListen = DefaultPublicListen
	}
	if strings.TrimSpace(out.PublicDomain) == "" {
		out.SubscriptionsEnabled = false
		enabledSet = false
	} else if !enabledSet {
		out.SubscriptionsEnabled = true
	}
	out.subscriptionsSet = enabledSet
	return out, nil
}

func (s *Store) UpdateSettings(ctx context.Context, in Settings) (Settings, error) {
	if err := in.Normalize(); err != nil {
		return Settings{}, err
	}
	now := time.Now().UTC()
	in.UpdatedAt = now
	values := map[string]string{
		"public_domain":         in.PublicDomain,
		"public_url":            in.PublicURL,
		"public_listen":         in.PublicListen,
		"subscriptions_enabled": strconv.FormatBool(in.SubscriptionsEnabled),
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Settings{}, fmt.Errorf("panelstore: settings transaction: %w", err)
	}
	for key, value := range values {
		if _, err := tx.ExecContext(ctx, `INSERT INTO settings (key, value, updated_at)
			VALUES (?, ?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
			key, value, formatTime(now)); err != nil {
			_ = tx.Rollback()
			return Settings{}, wrapDBErr(err, "update settings")
		}
	}
	if err := tx.Commit(); err != nil {
		return Settings{}, fmt.Errorf("panelstore: commit settings: %w", err)
	}
	return in, nil
}

func (s *Store) SettingsExists(ctx context.Context) (bool, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM settings`).Scan(&count); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, wrapDBErr(err, "count settings")
	}
	return count > 0, nil
}
