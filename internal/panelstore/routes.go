package panelstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const routeColumns = `id, name, entry_node_id, exit_node_id, transport_type, multi_strategy, sub_route_ids_json, enabled`

func scanRoute(scanner interface{ Scan(...any) error }) (Route, error) {
	var (
		r              Route
		subRouteIDsRaw string
		enabled        int
	)
	if err := scanner.Scan(&r.ID, &r.Name, &r.EntryNodeID, &r.ExitNodeID, &r.TransportType,
		&r.MultiStrategy, &subRouteIDsRaw, &enabled); err != nil {
		return Route{}, err
	}
	r.Enabled = enabled != 0
	ids, err := decodeRouteIDs(subRouteIDsRaw)
	if err != nil {
		return Route{}, err
	}
	r.SubRouteIDs = ids
	return r, nil
}

func decodeRouteIDs(raw string) ([]int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []int64{}, nil
	}
	var ids []int64
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, fmt.Errorf("panelstore: decode sub-route ids: %w", err)
	}
	return normalizeRouteIDs(ids), nil
}

func normalizeRouteIDs(in []int64) []int64 {
	out := make([]int64, 0, len(in))
	seen := make(map[int64]struct{}, len(in))
	for _, id := range in {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
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
	if !s.transportPolicy().Known(r.TransportType) {
		return invalidf("route %q: unsupported transport type %q", r.Name, r.TransportType)
	}
	if s.transportPolicy().IsMulti(r.TransportType) {
		return s.validateMultiRoute(ctx, r)
	}
	r.MultiStrategy = ""
	r.SubRouteIDs = []int64{}
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

func (s *Store) validateMultiRoute(ctx context.Context, r *Route) error {
	r.MultiStrategy = strings.TrimSpace(r.MultiStrategy)
	strategies := s.transportPolicy().MultiStrategies(r.TransportType)
	if !containsString(strategies, r.MultiStrategy) {
		return invalidf("route %q: multi_strategy %q is not supported by %q", r.Name, r.MultiStrategy, r.TransportType)
	}
	r.SubRouteIDs = normalizeRouteIDs(r.SubRouteIDs)
	if len(r.SubRouteIDs) < 2 {
		return invalidf("route %q: a multi-route requires at least two sub-routes", r.Name)
	}
	var entryID, exitID int64
	for _, id := range r.SubRouteIDs {
		child, err := s.GetRoute(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return invalidf("route %q: sub-route %d not found", r.Name, id)
		}
		if err != nil {
			return fmt.Errorf("route %q: sub-route %d: %w", r.Name, id, err)
		}
		if s.transportPolicy().IsMulti(child.TransportType) {
			return invalidf("route %q: sub-route %d is itself a multi-route (recursion is not allowed)", r.Name, id)
		}
		if entryID == 0 {
			entryID, exitID = child.EntryNodeID, child.ExitNodeID
			continue
		}
		if child.EntryNodeID != entryID || child.ExitNodeID != exitID {
			return invalidf("route %q: all sub-routes must share the same relay and exit", r.Name)
		}
	}
	r.EntryNodeID = entryID
	r.ExitNodeID = exitID
	return nil
}

func (s *Store) routeConflict(ctx context.Context, r Route, excludeID int64) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `SELECT id FROM routes
		WHERE entry_node_id = ? AND exit_node_id = ? AND transport_type = ?
		  AND multi_strategy = ? AND id <> ?
		LIMIT 1`,
		r.EntryNodeID, r.ExitNodeID, r.TransportType, r.MultiStrategy, excludeID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, wrapDBErr(err, "check route uniqueness")
	}
	return id, nil
}

func (s *Store) routeConflictErr(r Route, existingID int64) error {
	if s.transportPolicy().IsMulti(r.TransportType) {
		return fmt.Errorf("%w: multi-route between entry node %d and exit node %d with strategy %q already exists (route %d)",
			ErrConflict, r.EntryNodeID, r.ExitNodeID, r.MultiStrategy, existingID)
	}
	return fmt.Errorf("%w: route between entry node %d and exit node %d with transport %q already exists (route %d)",
		ErrConflict, r.EntryNodeID, r.ExitNodeID, r.TransportType, existingID)
}

func (s *Store) CreateRoute(ctx context.Context, r Route) (Route, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.validateRoute(ctx, &r); err != nil {
		return Route{}, err
	}
	if existingID, err := s.routeConflict(ctx, r, 0); err != nil {
		return Route{}, err
	} else if existingID != 0 {
		return Route{}, s.routeConflictErr(r, existingID)
	}
	subRouteIDsRaw, err := json.Marshal(r.SubRouteIDs)
	if err != nil {
		return Route{}, err
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO routes
		(name, entry_node_id, exit_node_id, transport_type, multi_strategy, sub_route_ids_json, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.Name, r.EntryNodeID, r.ExitNodeID, r.TransportType, r.MultiStrategy,
		string(subRouteIDsRaw), boolInt(r.Enabled))
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
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.GetRoute(ctx, r.ID); err != nil {
		return Route{}, err
	}
	if err := s.validateRoute(ctx, &r); err != nil {
		return Route{}, err
	}
	if existingID, err := s.routeConflict(ctx, r, r.ID); err != nil {
		return Route{}, err
	} else if existingID != 0 {
		return Route{}, s.routeConflictErr(r, existingID)
	}
	subRouteIDsRaw, err := json.Marshal(r.SubRouteIDs)
	if err != nil {
		return Route{}, err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE routes SET
		name = ?, entry_node_id = ?, exit_node_id = ?, transport_type = ?,
		multi_strategy = ?, sub_route_ids_json = ?, enabled = ?
		WHERE id = ?`,
		r.Name, r.EntryNodeID, r.ExitNodeID, r.TransportType, r.MultiStrategy,
		string(subRouteIDsRaw), boolInt(r.Enabled), r.ID)
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
	if multi, ok, err := s.routeUsedByMulti(ctx, id); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("%w: route %d is used by multi-route %q (%d); remove it from the multi-route first",
			ErrConflict, id, multi.Name, multi.ID)
	}
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

func (s *Store) routeUsedByMulti(ctx context.Context, id int64) (Route, bool, error) {
	kinds := s.transportPolicy().MultiKinds()
	if len(kinds) == 0 {
		return Route{}, false, nil
	}
	placeholders := make([]string, len(kinds))
	args := make([]any, len(kinds))
	for i, kind := range kinds {
		placeholders[i] = "?"
		args[i] = kind
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+routeColumns+` FROM routes WHERE transport_type IN (`+strings.Join(placeholders, ",")+`)`,
		args...)
	if err != nil {
		return Route{}, false, wrapDBErr(err, "list multi routes")
	}
	defer rows.Close()
	for rows.Next() {
		r, err := scanRoute(rows)
		if err != nil {
			return Route{}, false, wrapDBErr(err, "scan multi route")
		}
		for _, childID := range r.SubRouteIDs {
			if childID == id {
				return r, true, nil
			}
		}
	}
	if err := rows.Err(); err != nil {
		return Route{}, false, wrapDBErr(err, "list multi routes")
	}
	return Route{}, false, nil
}
