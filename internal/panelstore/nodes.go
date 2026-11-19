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

func scanNode(scanner interface{ Scan(...any) error }) (Node, error) {
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
	return n, nil
}

func validateNode(n *Node) error {
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
	transports, err := normalizeTransports(n.Transports)
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

// CreateNode inserts a node and generates its secrets.
func (s *Store) CreateNode(ctx context.Context, n Node) (Node, error) {
	if err := validateNode(&n); err != nil {
		return Node{}, err
	}
	if err := ensureSecrets(&n); err != nil {
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
	return n, nil
}

// GetNode returns a node by ID.
func (s *Store) GetNode(ctx context.Context, id int64) (Node, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes WHERE id = ?`, id)
	n, err := scanNode(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Node{}, fmt.Errorf("%w: node %d", ErrNotFound, id)
	}
	if err != nil {
		return Node{}, wrapDBErr(err, "get node")
	}
	return n, nil
}

// FindNodeBySecret returns a node by its node_secret.
func (s *Store) FindNodeBySecret(ctx context.Context, secret string) (Node, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return Node{}, fmt.Errorf("%w: empty node secret", ErrNotFound)
	}
	row := s.db.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes WHERE node_secret = ?`, secret)
	n, err := scanNode(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Node{}, fmt.Errorf("%w: node secret", ErrNotFound)
	}
	if err != nil {
		return Node{}, wrapDBErr(err, "find node by secret")
	}
	return n, nil
}

// ListNode returns all nodes ordered by name.
func (s *Store) ListNode(ctx context.Context) ([]Node, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+nodeColumns+` FROM nodes ORDER BY name`)
	if err != nil {
		return nil, wrapDBErr(err, "list nodes")
	}
	defer rows.Close()
	out := []Node{}
	for rows.Next() {
		n, err := scanNode(rows)
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

// UpdateNode updates a node. Secret material is preserved unless explicitly provided.
func (s *Store) UpdateNode(ctx context.Context, n Node) (Node, error) {
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
	if err := validateNode(&n); err != nil {
		return Node{}, err
	}
	if err := ensureSecrets(&n); err != nil {
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
	return n, nil
}

// DeleteNode removes a node and its routes.
func (s *Store) DeleteNode(ctx context.Context, id int64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
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

// SetNodeStatus updates the deployment status and last error.
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

// TouchNode marks a node as online after a successful sync.
func (s *Store) TouchNode(ctx context.Context, id int64) error {
	return s.SetNodeStatus(ctx, id, NodeStatusOnline, "")
}
