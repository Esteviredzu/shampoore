package panelstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const routeColumns = `id, name, entry_node_id, exit_node_id, transport_type, enabled`

func scanRoute(scanner interface{ Scan(...any) error }) (Route, error) {
	var (
		r       Route
		enabled int
	)
	if err := scanner.Scan(&r.ID, &r.Name, &r.EntryNodeID, &r.ExitNodeID, &r.TransportType, &enabled); err != nil {
		return Route{}, err
	}
	r.Enabled = enabled != 0
	return r, nil
}

func (s *Store) validateRoute(ctx context.Context, r *Route) error {
	r.Name = strings.TrimSpace(r.Name)
	if r.Name == "" {
		return invalidf("route name is required")
	}
	if len(r.Name) > 128 {
		return invalidf("route name is too long")
	}
	r.TransportType = strings.TrimSpace(r.TransportType)
	if !validTransport(r.TransportType) {
		return invalidf("route %q: unsupported transport type %q", r.Name, r.TransportType)
	}
	if r.EntryNodeID <= 0 || r.ExitNodeID <= 0 {
		return invalidf("route %q: entry and exit nodes are required", r.Name)
	}
	entry, err := s.GetNode(ctx, r.EntryNodeID)
	if err != nil {
		return fmt.Errorf("route %q: entry node: %w", r.Name, err)
	}
	if _, ok := entry.EnabledTransport(r.TransportType); !ok {
		return invalidf("route %q: entry node %q has no enabled %s transport", r.Name, entry.Name, r.TransportType)
	}
	exit, err := s.GetNode(ctx, r.ExitNodeID)
	if err != nil {
		return fmt.Errorf("route %q: exit node: %w", r.Name, err)
	}
	if !exit.IsExit {
		return invalidf("route %q: node %q is not an exit node", r.Name, exit.Name)
	}
	return nil
}

func (s *Store) CreateRoute(ctx context.Context, r Route) (Route, error) {
	if err := s.validateRoute(ctx, &r); err != nil {
		return Route{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `INSERT INTO routes
		(name, entry_node_id, exit_node_id, transport_type, enabled)
		VALUES (?, ?, ?, ?, ?)`,
		r.Name, r.EntryNodeID, r.ExitNodeID, r.TransportType, boolInt(r.Enabled))
	if err != nil {
		return Route{}, wrapDBErr(err, "create route")
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Route{}, fmt.Errorf("panelstore: create route id: %w", err)
	}
	r.ID = id
	return r, nil
}

func (s *Store) GetRoute(ctx context.Context, id int64) (Route, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+routeColumns+` FROM routes WHERE id = ?`, id)
	r, err := scanRoute(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Route{}, fmt.Errorf("%w: route %d", ErrNotFound, id)
	}
	if err != nil {
		return Route{}, wrapDBErr(err, "get route")
	}
	return r, nil
}

func (s *Store) ListRoutes(ctx context.Context) ([]Route, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+routeColumns+` FROM routes ORDER BY id`)
	if err != nil {
		return nil, wrapDBErr(err, "list routes")
	}
	defer rows.Close()
	out := []Route{}
	for rows.Next() {
		r, err := scanRoute(rows)
		if err != nil {
			return nil, wrapDBErr(err, "scan route")
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBErr(err, "list routes")
	}
	return out, nil
}

func (s *Store) ListRoutesForExit(ctx context.Context, exitNodeID int64) ([]Route, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+routeColumns+` FROM routes WHERE exit_node_id = ? AND enabled = 1 ORDER BY id`, exitNodeID)
	if err != nil {
		return nil, wrapDBErr(err, "list routes for exit")
	}
	defer rows.Close()
	out := []Route{}
	for rows.Next() {
		r, err := scanRoute(rows)
		if err != nil {
			return nil, wrapDBErr(err, "scan route")
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBErr(err, "list routes for exit")
	}
	return out, nil
}

func (s *Store) UpdateRoute(ctx context.Context, r Route) (Route, error) {
	if _, err := s.GetRoute(ctx, r.ID); err != nil {
		return Route{}, err
	}
	if err := s.validateRoute(ctx, &r); err != nil {
		return Route{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `UPDATE routes SET
		name = ?, entry_node_id = ?, exit_node_id = ?, transport_type = ?, enabled = ?
		WHERE id = ?`,
		r.Name, r.EntryNodeID, r.ExitNodeID, r.TransportType, boolInt(r.Enabled), r.ID)
	if err != nil {
		return Route{}, wrapDBErr(err, "update route")
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return Route{}, fmt.Errorf("%w: route %d", ErrNotFound, r.ID)
	}
	return r, nil
}

func (s *Store) DeleteRoute(ctx context.Context, id int64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `DELETE FROM routes WHERE id = ?`, id)
	if err != nil {
		return wrapDBErr(err, "delete route")
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("%w: route %d", ErrNotFound, id)
	}
	return nil
}
