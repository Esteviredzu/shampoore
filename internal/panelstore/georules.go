package panelstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path"
	"strings"
	"time"
)

type GeoRuleSource struct {
	Name                  string    `json:"name"`
	URL                   string    `json:"url"`
	UpdateIntervalMinutes int       `json:"update_interval_minutes"`
	LastUpdatedAt         time.Time `json:"last_updated_at,omitempty"`
	LastError             string    `json:"last_error,omitempty"`
	ExitNodeIDs           []int64   `json:"exit_node_ids"`
}

func NormalizeGeoRuleURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", invalidf("geo rule url is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", invalidf("geo rule url: %v", err)
	}
	scheme := strings.ToLower(strings.TrimSpace(u.Scheme))
	if scheme != "http" && scheme != "https" {
		return "", invalidf("geo rule url must use http or https")
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if host == "" {
		return "", invalidf("geo rule url has no host")
	}
	port := u.Port()

	if host == "github.com" || host == "www.github.com" {
		segments := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(segments) < 5 || segments[2] != "blob" {
			return "", invalidf("geo rule url: expected https://github.com/<user>/<repo>/blob/<branch>/<path>")
		}
		branch := strings.TrimSpace(segments[3])
		filePath := strings.Join(segments[4:], "/")
		if branch == "" || filePath == "" {
			return "", invalidf("geo rule url: github blob url has no branch or path")
		}
		host = "raw.githubusercontent.com"
		u.Path = "/" + segments[0] + "/" + segments[1] + "/" + branch + "/" + filePath
	}
	if strings.TrimSpace(strings.Trim(u.Path, "/")) == "" {
		return "", invalidf("geo rule url has no file path")
	}

	u.Scheme = scheme
	if port != "" {
		u.Host = net.JoinHostPort(host, port)
	} else {
		u.Host = host
	}
	u.Fragment = ""
	return u.String(), nil
}

func normalizeGeoRuleSource(src GeoRuleSource) (GeoRuleSource, error) {
	name := strings.TrimSpace(src.Name)
	if name == "" {
		return GeoRuleSource{}, invalidf("geo rule name is required")
	}
	if len(name) > 128 {
		return GeoRuleSource{}, invalidf("geo rule name is too long")
	}
	if name == "." || name == ".." || path.Base(name) != name {
		return GeoRuleSource{}, invalidf("geo rule name %q is invalid", src.Name)
	}
	if strings.ContainsAny(name, "/\\") {
		return GeoRuleSource{}, invalidf("geo rule name must not contain path separators")
	}
	for _, r := range name {
		if r < 32 {
			return GeoRuleSource{}, invalidf("geo rule name contains control characters")
		}
	}
	if src.UpdateIntervalMinutes < 0 {
		return GeoRuleSource{}, invalidf("geo rule update_interval_minutes must not be negative")
	}
	normalizedURL, err := NormalizeGeoRuleURL(src.URL)
	if err != nil {
		return GeoRuleSource{}, err
	}
	src.Name = name
	src.URL = normalizedURL
	return src, nil
}

const geoRuleSourceColumns = `name, url, update_interval_minutes, last_updated_at, last_error`

func scanGeoRuleSource(scanner interface{ Scan(...any) error }) (GeoRuleSource, error) {
	var (
		src       GeoRuleSource
		updatedAt string
	)
	if err := scanner.Scan(&src.Name, &src.URL, &src.UpdateIntervalMinutes, &updatedAt, &src.LastError); err != nil {
		return GeoRuleSource{}, err
	}
	src.LastUpdatedAt = parseTime(updatedAt)
	return src, nil
}

func (s *Store) UpsertGeoRuleSource(ctx context.Context, src GeoRuleSource) (GeoRuleSource, error) {
	normalized, err := normalizeGeoRuleSource(src)
	if err != nil {
		return GeoRuleSource{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err = s.db.ExecContext(ctx, `INSERT INTO geo_rule_sources
		(name, url, update_interval_minutes, last_updated_at, last_error)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			url = excluded.url,
			update_interval_minutes = excluded.update_interval_minutes,
			last_updated_at = excluded.last_updated_at,
			last_error = excluded.last_error`,
		normalized.Name, normalized.URL, normalized.UpdateIntervalMinutes,
		formatTime(normalized.LastUpdatedAt), normalized.LastError)
	if err != nil {
		return GeoRuleSource{}, wrapDBErr(err, "upsert geo rule source")
	}
	return normalized, nil
}

func (s *Store) GetGeoRuleSource(ctx context.Context, name string) (GeoRuleSource, error) {
	name = strings.TrimSpace(name)
	row := s.db.QueryRowContext(ctx,
		`SELECT `+geoRuleSourceColumns+` FROM geo_rule_sources WHERE name = ?`, name)
	src, err := scanGeoRuleSource(row)
	if errors.Is(err, sql.ErrNoRows) {
		return GeoRuleSource{}, fmt.Errorf("%w: geo rule source %s", ErrNotFound, name)
	}
	if err != nil {
		return GeoRuleSource{}, wrapDBErr(err, "get geo rule source")
	}
	ids, err := s.GeoRuleNodes(ctx, src.Name)
	if err != nil {
		return GeoRuleSource{}, err
	}
	src.ExitNodeIDs = ids
	return src, nil
}

func (s *Store) ListGeoRuleSources(ctx context.Context) ([]GeoRuleSource, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+geoRuleSourceColumns+` FROM geo_rule_sources ORDER BY name`)
	if err != nil {
		return nil, wrapDBErr(err, "list geo rule sources")
	}
	out := []GeoRuleSource{}
	for rows.Next() {
		src, err := scanGeoRuleSource(rows)
		if err != nil {
			_ = rows.Close()
			return nil, wrapDBErr(err, "scan geo rule source")
		}
		out = append(out, src)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, wrapDBErr(err, "list geo rule sources")
	}
	if err := rows.Close(); err != nil {
		return nil, wrapDBErr(err, "close geo rule sources")
	}
	bindings, err := s.ListGeoRuleBindings(ctx)
	if err != nil {
		return nil, err
	}
	for i := range out {
		if ids, ok := bindings[out[i].Name]; ok {
			out[i].ExitNodeIDs = ids
		} else {
			out[i].ExitNodeIDs = []int64{}
		}
	}
	return out, nil
}

func (s *Store) DeleteGeoRuleSource(ctx context.Context, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wrapDBErr(err, "delete geo rule source")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM geo_rule_nodes WHERE rule_name = ?`, name); err != nil {
		_ = tx.Rollback()
		return wrapDBErr(err, "delete geo rule nodes")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM geo_rule_sources WHERE name = ?`, name); err != nil {
		_ = tx.Rollback()
		return wrapDBErr(err, "delete geo rule source")
	}
	if err := tx.Commit(); err != nil {
		return wrapDBErr(err, "delete geo rule source")
	}
	return nil
}

func (s *Store) GeoRuleNodes(ctx context.Context, ruleName string) ([]int64, error) {
	ruleName = strings.TrimSpace(ruleName)
	if ruleName == "" {
		return nil, invalidf("geo rule name is required")
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT node_id FROM geo_rule_nodes WHERE rule_name = ? ORDER BY node_id`, ruleName)
	if err != nil {
		return nil, wrapDBErr(err, "list geo rule nodes")
	}
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, wrapDBErr(err, "scan geo rule node")
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBErr(err, "list geo rule nodes")
	}
	return out, nil
}

func (s *Store) ListGeoRuleBindings(ctx context.Context) (map[string][]int64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT rule_name, node_id FROM geo_rule_nodes ORDER BY rule_name, node_id`)
	if err != nil {
		return nil, wrapDBErr(err, "list geo rule bindings")
	}
	defer rows.Close()
	out := make(map[string][]int64)
	for rows.Next() {
		var (
			name string
			id   int64
		)
		if err := rows.Scan(&name, &id); err != nil {
			return nil, wrapDBErr(err, "scan geo rule binding")
		}
		out[name] = append(out[name], id)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBErr(err, "list geo rule bindings")
	}
	return out, nil
}

func (s *Store) ValidateExitNodeIDs(ctx context.Context, ids []int64) error {
	normalized, err := normalizeNodeIDs(ids)
	if err != nil {
		return err
	}
	if len(normalized) == 0 {
		return nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(normalized)), ",")
	args := make([]any, 0, len(normalized))
	for _, id := range normalized {
		args = append(args, id)
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM nodes WHERE is_exit = 1 AND id IN (`+placeholders+`)`, args...)
	if err != nil {
		return wrapDBErr(err, "validate exit node ids")
	}
	defer rows.Close()
	found := make(map[int64]struct{}, len(normalized))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return wrapDBErr(err, "scan exit node id")
		}
		found[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return wrapDBErr(err, "validate exit node ids")
	}
	for _, id := range normalized {
		if _, ok := found[id]; !ok {
			return invalidf("node %d is not an exit server", id)
		}
	}
	return nil
}

func (s *Store) SetGeoRuleNodes(ctx context.Context, ruleName string, ids []int64) ([]int64, error) {
	ruleName = strings.TrimSpace(ruleName)
	if ruleName == "" {
		return nil, invalidf("geo rule name is required")
	}
	normalized, err := normalizeNodeIDs(ids)
	if err != nil {
		return nil, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.ValidateExitNodeIDs(ctx, normalized); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, wrapDBErr(err, "set geo rule nodes")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM geo_rule_nodes WHERE rule_name = ?`, ruleName); err != nil {
		_ = tx.Rollback()
		return nil, wrapDBErr(err, "clear geo rule nodes")
	}
	for _, id := range normalized {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO geo_rule_nodes (rule_name, node_id) VALUES (?, ?)`, ruleName, id); err != nil {
			_ = tx.Rollback()
			return nil, wrapDBErr(err, "insert geo rule node")
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, wrapDBErr(err, "set geo rule nodes")
	}
	return normalized, nil
}

func (s *Store) MarkGeoRuleSourceUpdated(ctx context.Context, name string, at time.Time) error {
	name = strings.TrimSpace(name)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx,
		`UPDATE geo_rule_sources SET last_updated_at = ?, last_error = '' WHERE name = ?`,
		formatTime(at), name)
	if err != nil {
		return wrapDBErr(err, "mark geo rule source updated")
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("%w: geo rule source %s", ErrNotFound, name)
	}
	return nil
}

func (s *Store) SetGeoRuleSourceError(ctx context.Context, name, message string) error {
	name = strings.TrimSpace(name)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx,
		`UPDATE geo_rule_sources SET last_error = ? WHERE name = ?`, message, name)
	if err != nil {
		return wrapDBErr(err, "set geo rule source error")
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("%w: geo rule source %s", ErrNotFound, name)
	}
	return nil
}

func (s *Store) DueGeoRuleSources(ctx context.Context, now time.Time) ([]GeoRuleSource, error) {
	sources, err := s.ListGeoRuleSources(ctx)
	if err != nil {
		return nil, err
	}
	now = now.UTC()
	out := make([]GeoRuleSource, 0, len(sources))
	for _, src := range sources {
		if src.UpdateIntervalMinutes <= 0 {
			continue
		}
		if src.LastUpdatedAt.IsZero() {
			out = append(out, src)
			continue
		}
		next := src.LastUpdatedAt.Add(time.Duration(src.UpdateIntervalMinutes) * time.Minute)
		if !now.Before(next) {
			out = append(out, src)
		}
	}
	return out, nil
}
