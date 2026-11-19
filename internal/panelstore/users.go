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

// CreateUser inserts a subscription user.
func (s *Store) CreateUser(ctx context.Context, u User) (User, error) {
	if err := normalizeUser(&u); err != nil {
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

// GetUser returns a user by ID.
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

// FindUserBySubToken returns a user by subscription token.
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

// ListUsers returns all users ordered by ID.
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

// UpdateUser updates subscription metadata without resetting traffic counters.
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

// DeleteUser removes a user.
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

// AddTraffic atomically applies upload/download deltas for users.
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

// ResetUserTraffic zeroes a user's counters.
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

// ActiveUsers returns users that may currently use the service.
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

// ActiveUsersForExit returns active users that may route through exitNodeID.
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
