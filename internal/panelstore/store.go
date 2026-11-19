package panelstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// Store is a concurrency-safe SQLite backed control-plane store.
type Store struct {
	db *sql.DB

	// writeMu serializes write transactions. database/sql is safe on its own,
	// but serializing writers keeps SQLite's single-writer semantics explicit
	// and avoids SQLITE_BUSY under load.
	writeMu sync.Mutex
}

var memorySeq atomic.Uint64

var migrations = []string{
	`CREATE TABLE IF NOT EXISTS nodes (
		id                     INTEGER PRIMARY KEY AUTOINCREMENT,
		name                   TEXT    NOT NULL UNIQUE,
		ssh_host               TEXT    NOT NULL DEFAULT '',
		ssh_port               INTEGER NOT NULL DEFAULT 22,
		ssh_user               TEXT    NOT NULL DEFAULT 'root',
		ssh_private_key        TEXT    NOT NULL DEFAULT '',
		ssh_password           TEXT    NOT NULL DEFAULT '',
		is_relay               INTEGER NOT NULL DEFAULT 0,
		is_exit                INTEGER NOT NULL DEFAULT 0,
		transports_json        TEXT    NOT NULL DEFAULT '[]',
		node_secret            TEXT    NOT NULL UNIQUE,
		generated_secrets_json TEXT    NOT NULL DEFAULT '{}',
		socks_port             INTEGER NOT NULL DEFAULT 0,
		status                 TEXT    NOT NULL DEFAULT 'new',
		last_error             TEXT    NOT NULL DEFAULT '',
		updated_at             TEXT    NOT NULL
	);
	CREATE TABLE IF NOT EXISTS routes (
		id             INTEGER PRIMARY KEY AUTOINCREMENT,
		name           TEXT    NOT NULL UNIQUE,
		entry_node_id  INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
		exit_node_id   INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
		transport_type TEXT    NOT NULL CHECK (transport_type IN ('raw_ws','silo','livekit')),
		enabled        INTEGER NOT NULL DEFAULT 1
	);
	CREATE TABLE IF NOT EXISTS users (
		id                     TEXT PRIMARY KEY,
		name                   TEXT NOT NULL,
		sub_token              TEXT NOT NULL UNIQUE,
		enabled                INTEGER NOT NULL DEFAULT 1,
		expires_at             INTEGER NOT NULL DEFAULT 0,
		traffic_limit_bytes    INTEGER NOT NULL DEFAULT 0,
		used_upload_bytes      INTEGER NOT NULL DEFAULT 0,
		used_download_bytes    INTEGER NOT NULL DEFAULT 0,
		allowed_route_ids_json TEXT NOT NULL DEFAULT '[]',
		updated_at             TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_routes_entry ON routes(entry_node_id);
	CREATE INDEX IF NOT EXISTS idx_routes_exit  ON routes(exit_node_id);
	CREATE INDEX IF NOT EXISTS idx_users_token  ON users(sub_token);
	CREATE INDEX IF NOT EXISTS idx_nodes_secret ON nodes(node_secret);`,
}

// Open opens (and migrates) a SQLite database. Use ":memory:" for tests.
func Open(ctx context.Context, path string) (*Store, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("panelstore: database path is empty")
	}

	dsn := path
	if path == ":memory:" {
		dsn = fmt.Sprintf("file:shampoore-memory-%d?mode=memory&cache=shared", memorySeq.Add(1))
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("panelstore: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	s := &Store{db: db}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("panelstore: ping: %w", err)
	}

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
		"PRAGMA synchronous=NORMAL",
	}
	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("panelstore: %s: %w", p, err)
		}
	}

	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	if path != ":memory:" {
		if err := os.Chmod(path, 0o600); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("panelstore: chmod database: %w", err)
		}
		if dir := filepath.Dir(path); dir != "." {
			_ = os.Chmod(dir, 0o700)
		}
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("panelstore: read schema version: %w", err)
	}
	if version > len(migrations) {
		return fmt.Errorf("panelstore: database schema version %d is newer than supported %d", version, len(migrations))
	}
	for i := version; i < len(migrations); i++ {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("panelstore: begin migration %d: %w", i+1, err)
		}
		if _, err := tx.ExecContext(ctx, migrations[i]); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("panelstore: migration %d: %w", i+1, err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version=%d", i+1)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("panelstore: set schema version %d: %w", i+1, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("panelstore: commit migration %d: %w", i+1, err)
		}
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC()
	}
	return time.Time{}
}

func expiresUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().Unix()
}

func unixExpires(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(v, 0).UTC()
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func isUniqueErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint") || strings.Contains(msg, "constraint failed")
}

func isForeignErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "foreign key constraint")
}

func wrapDBErr(err error, what string) error {
	if err == nil {
		return nil
	}
	if isUniqueErr(err) {
		return fmt.Errorf("%w: %s: %v", ErrConflict, what, err)
	}
	if isForeignErr(err) {
		return fmt.Errorf("%w: %s: %v", ErrInvalid, what, err)
	}
	return fmt.Errorf("panelstore: %s: %w", what, err)
}
