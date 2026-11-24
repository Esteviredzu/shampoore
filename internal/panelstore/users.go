package panelstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/google/uuid"
)

const userColumns = `id, name, sub_token, enabled, expires_at, traffic_limit_bytes,
	used_upload_bytes, used_download_bytes, allowed_route_ids_json, updated_at`

func scanUser(scanner interface{ Scan(...any) error }) (User, error) {
	var (
		u          User
		enabled    int
		expires    int64
		allowedRaw string
		updatedRaw string
	)
	err := scanner.Scan(&u.ID, &u.Name, &u.SubToken, &enabled, &expires,
		&u.TrafficLimitBytes, &u.UsedUploadBytes, &u.UsedDownloadBytes, &allowedRaw, &updatedRaw)
	if err != nil {
		return User{}, err
	}
	u.Enabled = enabled != 0
	u.ExpiresAt = unixExpires(expires)
	if err := json.Unmarshal([]byte(allowedRaw), &u.AllowedRouteIDs); err != nil {
		return User{}, fmt.Errorf("panelstore: decode allowed routes: %w", err)
	}
	if u.AllowedRouteIDs == nil {
		u.AllowedRouteIDs = []int64{}
	}
	return u, nil
}

func normalizeUser(u *User) error {
	id, err := auth.NormalizeUserID(u.ID)
	if err != nil {
		if u.ID == "" {
			id = uuid.NewString()
		} else {
			return invalidf("user id: %v", err)
		}
	}
	u.ID = id
	u.Name = strings.TrimSpace(u.Name)
	if u.Name == "" {
		u.Name = u.ID
	}
	u.SubToken = strings.TrimSpace(u.SubToken)
	if u.SubToken == "" {
		token, err := randomHex(32)
		if err != nil {
			return err
		}
		u.SubToken = token
	}
	if len(u.SubToken) < 16 || len(u.SubToken) > 128 {
		return invalidf("user %q: invalid subscription token length", u.ID)
	}
	for _, r := range u.SubToken {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return invalidf("user %q: invalid subscription token", u.ID)
		}
	}
	if u.TrafficLimitBytes < 0 {
		return invalidf("user %q: traffic limit must not be negative", u.ID)
	}
	u.AllowedRouteIDs = normalizeAllowedRoutes(u.AllowedRouteIDs)
	return nil
}

func (s *Store) CreateUser(ctx context.Context, u User) (User, error) {
	if err := normalizeUser(&u); err != nil {
		return User{}, err
	}
	if err := s.validateUserRoutes(ctx, u.AllowedRouteIDs); err != nil {
		return User{}, err
	}
	allowedRaw, err := json.Marshal(u.AllowedRouteIDs)
	if err != nil {
		return User{}, err
	}
	now := time.Now().UTC()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err = s.db.ExecContext(ctx, `INSERT INTO users
		(id, name, sub_token, enabled, expires_at, traffic_limit_bytes,
		 used_upload_bytes, used_download_bytes, allowed_route_ids_json, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		u.ID, u.Name, u.SubToken, boolInt(u.Enabled), expiresUnix(u.ExpiresAt),
		u.TrafficLimitBytes, u.UsedUploadBytes, u.UsedDownloadBytes,
		string(allowedRaw), formatTime(now))
	if err != nil {
		return User{}, wrapDBErr(err, "create user")
	}
	return u, nil
}

func (s *Store) GetUser(ctx context.Context, id string) (User, error) {
	normalized, err := auth.NormalizeUserID(id)
	if err != nil {
		return User{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	row := s.db.QueryRowContext(ctx, `SELECT `+userColumns+` FROM users WHERE id = ?`, normalized)
	u, err := scanUser(row)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, fmt.Errorf("%w: user %s", ErrNotFound, normalized)
	}
	if err != nil {
		return User{}, wrapDBErr(err, "get user")
	}
	return u, nil
}

func (s *Store) FindUserBySubToken(ctx context.Context, token string) (User, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return User{}, fmt.Errorf("%w: empty subscription token", ErrNotFound)
	}
	row := s.db.QueryRowContext(ctx, `SELECT `+userColumns+` FROM users WHERE sub_token = ?`, token)
	u, err := scanUser(row)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, fmt.Errorf("%w: subscription token", ErrNotFound)
	}
	if err != nil {
		return User{}, wrapDBErr(err, "find user by token")
	}
	return u, nil
}

func (s *Store) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+userColumns+` FROM users ORDER BY id`)
	if err != nil {
		return nil, wrapDBErr(err, "list users")
	}
	defer rows.Close()
	out := []User{}
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, wrapDBErr(err, "scan user")
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBErr(err, "list users")
	}
	return out, nil
}

func (s *Store) UpdateUser(ctx context.Context, u User) (User, error) {
	existing, err := s.GetUser(ctx, u.ID)
	if err != nil {
		return User{}, err
	}
	u.ID = existing.ID
	u.SubToken = strings.TrimSpace(u.SubToken)
	if u.Name == "" {
		u.Name = existing.Name
	}
	if u.SubToken == "" {
		u.SubToken = existing.SubToken
	}
	if u.AllowedRouteIDs == nil {
		u.AllowedRouteIDs = existing.AllowedRouteIDs
	}
	u.UsedUploadBytes = existing.UsedUploadBytes
	u.UsedDownloadBytes = existing.UsedDownloadBytes
	if u.TrafficLimitBytes < 0 {
		u.TrafficLimitBytes = existing.TrafficLimitBytes
	}
	if err := normalizeUser(&u); err != nil {
		return User{}, err
	}
	if err := s.validateUserRoutes(ctx, u.AllowedRouteIDs); err != nil {
		return User{}, err
	}
	allowedRaw, err := json.Marshal(u.AllowedRouteIDs)
	if err != nil {
		return User{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `UPDATE users SET
		name = ?, sub_token = ?, enabled = ?, expires_at = ?, traffic_limit_bytes = ?,
		allowed_route_ids_json = ?, updated_at = ?
		WHERE id = ?`,
		u.Name, u.SubToken, boolInt(u.Enabled), expiresUnix(u.ExpiresAt),
		u.TrafficLimitBytes, string(allowedRaw), formatTime(time.Now()), u.ID)
	if err != nil {
		return User{}, wrapDBErr(err, "update user")
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return User{}, fmt.Errorf("%w: user %s", ErrNotFound, u.ID)
	}
	return u, nil
}

func (s *Store) DeleteUser(ctx context.Context, id string) error {
	normalized, err := auth.NormalizeUserID(id)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `DELETE FROM users WHERE id = ?`, normalized)
	if err != nil {
		return wrapDBErr(err, "delete user")
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("%w: user %s", ErrNotFound, normalized)
	}
	return nil
}

func (s *Store) AddTraffic(ctx context.Context, deltas []TrafficDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	prepared := make([]TrafficDelta, 0, len(deltas))
	for _, d := range deltas {
		if d.UploadBytes < 0 || d.DownloadBytes < 0 {
			return invalidf("traffic delta for %q must not be negative", d.UserID)
		}
		id, err := auth.NormalizeUserID(d.UserID)
		if err != nil {
			return fmt.Errorf("%w: user %q: %v", ErrInvalid, d.UserID, err)
		}
		if d.UploadBytes == 0 && d.DownloadBytes == 0 {
			continue
		}
		prepared = append(prepared, TrafficDelta{UserID: id, UploadBytes: d.UploadBytes, DownloadBytes: d.DownloadBytes})
	}
	if len(prepared) == 0 {
		return nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("panelstore: traffic transaction: %w", err)
	}
	now := formatTime(time.Now())
	for _, d := range prepared {
		res, err := tx.ExecContext(ctx, `UPDATE users SET
			used_upload_bytes = used_upload_bytes + ?,
			used_download_bytes = used_download_bytes + ?,
			updated_at = ?
			WHERE id = ?`, d.UploadBytes, d.DownloadBytes, now, d.UserID)
		if err != nil {
			_ = tx.Rollback()
			return wrapDBErr(err, "add traffic")
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			_ = tx.Rollback()
			return fmt.Errorf("%w: user %s", ErrNotFound, d.UserID)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("panelstore: commit traffic: %w", err)
	}
	return nil
}

func (s *Store) ResetUserTraffic(ctx context.Context, id string) error {
	normalized, err := auth.NormalizeUserID(id)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `UPDATE users SET
		used_upload_bytes = 0, used_download_bytes = 0, updated_at = ? WHERE id = ?`,
		formatTime(time.Now()), normalized)
	if err != nil {
		return wrapDBErr(err, "reset traffic")
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("%w: user %s", ErrNotFound, normalized)
	}
	return nil
}

func (s *Store) ActiveUsers(ctx context.Context, now time.Time) ([]User, error) {
	users, err := s.ListUsers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]User, 0, len(users))
	for _, u := range users {
		if u.Active(now) {
			out = append(out, u)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Store) ActiveUsersForExit(ctx context.Context, exitNodeID int64, now time.Time) ([]User, error) {
	routes, err := s.ListRoutesForExit(ctx, exitNodeID)
	if err != nil {
		return nil, err
	}
	if len(routes) == 0 {
		return []User{}, nil
	}
	users, err := s.ListUsers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]User, 0, len(users))
	for _, u := range users {
		if !u.Active(now) {
			continue
		}
		for _, r := range routes {
			if u.RouteAllowed(r.ID) {
				out = append(out, u)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Store) validateUserRoutes(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(ids))
	unique := make([]int64, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	placeholders := make([]string, len(unique))
	args := make([]any, len(unique))
	for i, id := range unique {
		placeholders[i] = "?"
		args[i] = id
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM routes WHERE id IN (`+strings.Join(placeholders, ",")+`)`, args...)
	if err != nil {
		return wrapDBErr(err, "validate user routes")
	}
	found := make(map[int64]struct{}, len(unique))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return wrapDBErr(err, "scan user route")
		}
		found[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return wrapDBErr(err, "validate user routes")
	}
	rows.Close()
	missing := make([]int64, 0)
	for _, id := range unique {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return invalidf("user routes %v do not exist", missing)
	}
	return nil
}

func (s *Store) pruneUserRouteRefs(ctx context.Context, ids ...int64) error {
	if len(ids) == 0 {
		return nil
	}
	drop := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id > 0 {
			drop[id] = struct{}{}
		}
	}
	if len(drop) == 0 {
		return nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, allowed_route_ids_json FROM users`)
	if err != nil {
		return wrapDBErr(err, "list users for route cleanup")
	}
	type pending struct {
		id    string
		order []int64
	}
	updates := make([]pending, 0)
	for rows.Next() {
		var (
			id  string
			raw string
		)
		if err := rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return wrapDBErr(err, "scan user route refs")
		}
		var current []int64
		if strings.TrimSpace(raw) != "" {
			if err := json.Unmarshal([]byte(raw), &current); err != nil {
				rows.Close()
				return fmt.Errorf("panelstore: decode user %s routes: %w", id, err)
			}
		}
		next := make([]int64, 0, len(current))
		for _, routeID := range current {
			if _, gone := drop[routeID]; gone {
				continue
			}
			next = append(next, routeID)
		}
		if len(next) == len(current) {
			continue
		}
		updates = append(updates, pending{id: id, order: next})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return wrapDBErr(err, "list users for route cleanup")
	}
	rows.Close()
	if len(updates) == 0 {
		return nil
	}
	now := formatTime(time.Now())
	for _, item := range updates {
		encoded, err := json.Marshal(item.order)
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx,
			`UPDATE users SET allowed_route_ids_json = ?, updated_at = ? WHERE id = ?`,
			string(encoded), now, item.id); err != nil {
			return wrapDBErr(err, "prune user routes")
		}
	}
	return nil
}
