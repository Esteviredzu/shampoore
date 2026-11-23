package panelstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const nodeColumns = `id, name, ssh_host, ssh_port, ssh_user, ssh_private_key, ssh_password,
	is_relay, is_exit, transports_json, node_secret, generated_secrets_json,
	socks_port, status, last_error, updated_at`

func scanNode(scanner interface{ Scan(...any) error }, policy TransportPolicy) (Node, error) {
	var (
		n               Node
		isRelay, isExit int
		transportsRaw   string
		secretsRaw      string
		updatedRaw      string
	)
	err := scanner.Scan(
		&n.ID, &n.Name, &n.SSHHost, &n.SSHPort, &n.SSHUser, &n.SSHPrivateKey,
		&n.SSHPassword, &isRelay, &isExit, &transportsRaw, &n.NodeSecret,
		&secretsRaw, &n.SocksPort, &n.Status, &n.LastError, &updatedRaw,
	)
	if err != nil {
		return Node{}, err
	}
	n.IsRelay = isRelay != 0
	n.IsExit = isExit != 0
	n.UpdatedAt = parseTime(updatedRaw)
	if err := json.Unmarshal([]byte(transportsRaw), &n.Transports); err != nil {
		return Node{}, fmt.Errorf("panelstore: decode transports: %w", err)
	}
	if n.Transports == nil {
		n.Transports = []Transport{}
	}
	if err := json.Unmarshal([]byte(secretsRaw), &n.GeneratedSecrets); err != nil {
		return Node{}, fmt.Errorf("panelstore: decode generated secrets: %w", err)
	}
	if n.GeneratedSecrets == nil {
		n.GeneratedSecrets = map[string]string{}
	}
	hydrateNodeView(&n, policy)
	return n, nil
}

func (s *Store) validateNode(n *Node) error {
	n.Name = strings.TrimSpace(n.Name)
	if n.Name == "" {
		return invalidf("node name is required")
	}
	if len(n.Name) > 128 {
		return invalidf("node name is too long")
	}
	if !n.IsRelay && !n.IsExit {
		return invalidf("node %q must be a relay or an exit", n.Name)
	}
	n.SSHHost = strings.TrimSpace(n.SSHHost)
	n.SSHUser = strings.TrimSpace(n.SSHUser)
	n.NodeSecret = strings.TrimSpace(n.NodeSecret)
	if n.SSHUser == "" {
		n.SSHUser = "root"
	}
	if n.SSHPort == 0 {
		n.SSHPort = 22
	}
	if n.SSHPort < 1 || n.SSHPort > 65535 {
		return invalidf("node %q: invalid ssh port %d", n.Name, n.SSHPort)
	}
	if n.SocksPort < 0 || n.SocksPort > 65535 {
		return invalidf("node %q: invalid socks port %d", n.Name, n.SocksPort)
	}
	if n.IsExit && n.SocksPort == 0 {
		n.SocksPort = 1080
	}
	if n.IsRelay && !n.IsExit {
		n.SocksPort = 0
	}
	transports, err := normalizeTransports(n.Transports, s.transportPolicy())
	if err != nil {
		return fmt.Errorf("node %q: %w", n.Name, err)
	}
	n.Transports = transports
	if n.Status == "" {
		n.Status = NodeStatusNew
	}
	if n.GeneratedSecrets == nil {
		n.GeneratedSecrets = map[string]string{}
	}
	return nil
}

func (s *Store) CreateNode(ctx context.Context, n Node) (Node, error) {
	policy := s.transportPolicy()
	if err := s.validateNode(&n); err != nil {
		return Node{}, err
	}
	if err := applyRequestedSecrets(&n, policy); err != nil {
		return Node{}, err
	}
	if err := ensureSecrets(&n, policy); err != nil {
		return Node{}, err
	}
	transportsRaw, err := json.Marshal(n.Transports)
	if err != nil {
		return Node{}, err
	}
	secretsRaw, err := json.Marshal(n.GeneratedSecrets)
	if err != nil {
		return Node{}, err
	}
	n.UpdatedAt = time.Now().UTC()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `INSERT INTO nodes
		(name, ssh_host, ssh_port, ssh_user, ssh_private_key, ssh_password,
		 is_relay, is_exit, transports_json, node_secret, generated_secrets_json,
		 socks_port, status, last_error, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		n.Name, n.SSHHost, n.SSHPort, n.SSHUser, n.SSHPrivateKey, n.SSHPassword,
		boolInt(n.IsRelay), boolInt(n.IsExit), string(transportsRaw), n.NodeSecret,
		string(secretsRaw), n.SocksPort, n.Status, n.LastError, formatTime(n.UpdatedAt))
	if err != nil {
		return Node{}, wrapDBErr(err, "create node")
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Node{}, fmt.Errorf("panelstore: create node id: %w", err)
	}
	n.ID = id
	hydrateNodeView(&n, policy)
	return n, nil
}

func (s *Store) GetNode(ctx context.Context, id int64) (Node, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes WHERE id = ?`, id)
	n, err := scanNode(row, s.transportPolicy())
	if errors.Is(err, sql.ErrNoRows) {
		return Node{}, fmt.Errorf("%w: node %d", ErrNotFound, id)
	}
	if err != nil {
		return Node{}, wrapDBErr(err, "get node")
	}
	return n, nil
}

func (s *Store) FindNodeBySecret(ctx context.Context, secret string) (Node, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return Node{}, fmt.Errorf("%w: empty node secret", ErrNotFound)
	}
	row := s.db.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes WHERE node_secret = ?`, secret)
	n, err := scanNode(row, s.transportPolicy())
	if errors.Is(err, sql.ErrNoRows) {
		return Node{}, fmt.Errorf("%w: node secret", ErrNotFound)
	}
	if err != nil {
		return Node{}, wrapDBErr(err, "find node by secret")
	}
	return n, nil
}

func (s *Store) ListNode(ctx context.Context) ([]Node, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+nodeColumns+` FROM nodes ORDER BY name`)
	if err != nil {
		return nil, wrapDBErr(err, "list nodes")
	}
	defer rows.Close()
	out := []Node{}
	for rows.Next() {
		n, err := scanNode(rows, s.transportPolicy())
		if err != nil {
			return nil, wrapDBErr(err, "scan node")
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBErr(err, "list nodes")
	}
	return out, nil
}

func (s *Store) UpdateNode(ctx context.Context, n Node) (Node, error) {
	policy := s.transportPolicy()
	existing, err := s.GetNode(ctx, n.ID)
	if err != nil {
		return Node{}, err
	}
	n.NodeSecret = strings.TrimSpace(n.NodeSecret)
	if n.NodeSecret == "" {
		n.NodeSecret = existing.NodeSecret
	}
	if n.GeneratedSecrets == nil {
		n.GeneratedSecrets = existing.GeneratedSecrets
	} else {
		merged := make(map[string]string, len(existing.GeneratedSecrets)+len(n.GeneratedSecrets))
		for k, v := range existing.GeneratedSecrets {
			merged[k] = v
		}
		for k, v := range n.GeneratedSecrets {
			merged[k] = v
		}
		n.GeneratedSecrets = merged
	}
	if n.Status == "" {
		n.Status = existing.Status
	}
	if n.LastError == "" {
		n.LastError = existing.LastError
	}
	if err := s.validateNode(&n); err != nil {
		return Node{}, err
	}
	if err := applyRequestedSecrets(&n, policy); err != nil {
		return Node{}, err
	}
	if err := ensureSecrets(&n, policy); err != nil {
		return Node{}, err
	}
	n.UpdatedAt = time.Now().UTC()
	transportsRaw, err := json.Marshal(n.Transports)
	if err != nil {
		return Node{}, err
	}
	secretsRaw, err := json.Marshal(n.GeneratedSecrets)
	if err != nil {
		return Node{}, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `UPDATE nodes SET
		name = ?, ssh_host = ?, ssh_port = ?, ssh_user = ?, ssh_private_key = ?,
		ssh_password = ?, is_relay = ?, is_exit = ?, transports_json = ?,
		node_secret = ?, generated_secrets_json = ?, socks_port = ?, status = ?,
		last_error = ?, updated_at = ?
		WHERE id = ?`,
		n.Name, n.SSHHost, n.SSHPort, n.SSHUser, n.SSHPrivateKey, n.SSHPassword,
		boolInt(n.IsRelay), boolInt(n.IsExit), string(transportsRaw), n.NodeSecret,
		string(secretsRaw), n.SocksPort, n.Status, n.LastError, formatTime(n.UpdatedAt), n.ID)
	if err != nil {
		return Node{}, wrapDBErr(err, "update node")
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return Node{}, fmt.Errorf("%w: node %d", ErrNotFound, n.ID)
	}
	hydrateNodeView(&n, policy)
	return n, nil
}

func (s *Store) DeleteNode(ctx context.Context, id int64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.pruneDependentMultiRoutes(ctx, id); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM nodes WHERE id = ?`, id)
	if err != nil {
		return wrapDBErr(err, "delete node")
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("%w: node %d", ErrNotFound, id)
	}
	return nil
}

func (s *Store) pruneDependentMultiRoutes(ctx context.Context, nodeID int64) error {
	childIDs := make(map[int64]struct{})
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM routes WHERE entry_node_id = ? OR exit_node_id = ?`, nodeID, nodeID)
	if err != nil {
		return wrapDBErr(err, "list node routes")
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return wrapDBErr(err, "scan node route")
		}
		childIDs[id] = struct{}{}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return wrapDBErr(err, "list node routes")
	}
	if len(childIDs) == 0 {
		return nil
	}

	multiKinds := s.transportPolicy().MultiKinds()
	if len(multiKinds) == 0 {
		return nil
	}
	placeholders := make([]string, len(multiKinds))
	multiArgs := make([]any, len(multiKinds))
	for i, kind := range multiKinds {
		placeholders[i] = "?"
		multiArgs[i] = kind
	}
	multiRows, err := s.db.QueryContext(ctx,
		`SELECT id, sub_route_ids_json FROM routes WHERE transport_type IN (`+strings.Join(placeholders, ",")+`)`,
		multiArgs...)
	if err != nil {
		return wrapDBErr(err, "list multi routes")
	}
	var stale []int64
	for multiRows.Next() {
		var id int64
		var raw string
		if err := multiRows.Scan(&id, &raw); err != nil {
			multiRows.Close()
			return wrapDBErr(err, "scan multi route")
		}
		ids, err := decodeRouteIDs(raw)
		if err != nil {
			multiRows.Close()
			return err
		}
		for _, childID := range ids {
			if _, ok := childIDs[childID]; ok {
				stale = append(stale, id)
				break
			}
		}
	}
	err = multiRows.Err()
	multiRows.Close()
	if err != nil {
		return wrapDBErr(err, "list multi routes")
	}
	for _, id := range stale {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM routes WHERE id = ?`, id); err != nil {
			return wrapDBErr(err, "delete stale multi route")
		}
	}
	return nil
}

func (s *Store) SetNodeStatus(ctx context.Context, id int64, status, lastErr string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx,
		`UPDATE nodes SET status = ?, last_error = ?, updated_at = ? WHERE id = ?`,
		status, lastErr, formatTime(time.Now()), id)
	if err != nil {
		return wrapDBErr(err, "set node status")
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("%w: node %d", ErrNotFound, id)
	}
	return nil
}

func (s *Store) TouchNode(ctx context.Context, id int64) error {
	return s.SetNodeStatus(ctx, id, NodeStatusOnline, "")
}
