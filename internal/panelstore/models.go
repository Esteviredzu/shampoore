package panelstore

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	TransportRawWS   = "raw_ws"
	TransportSilo    = "silo"
	TransportLiveKit = "livekit"

	NodeStatusNew       = "new"
	NodeStatusDeploying = "deploying"
	NodeStatusDeployed  = "deployed"
	NodeStatusOnline    = "online"
	NodeStatusError     = "error"
)

var (
	ErrNotFound = errors.New("panelstore: not found")
	ErrConflict = errors.New("panelstore: conflict")
	ErrInvalid  = errors.New("panelstore: invalid")
)

// Transport describes one enabled service on a node.
type Transport struct {
	Type    string `json:"type"`
	Domain  string `json:"domain"`
	Enabled bool   `json:"enabled"`
}

// Node is a managed VPS that runs either the relay stack or the exit server.
type Node struct {
	ID               int64             `json:"id"`
	Name             string            `json:"name"`
	SSHHost          string            `json:"ssh_host"`
	SSHPort          int               `json:"ssh_port"`
	SSHUser          string            `json:"ssh_user"`
	SSHPrivateKey    string            `json:"ssh_private_key,omitempty"`
	SSHPassword      string            `json:"ssh_password,omitempty"`
	IsRelay          bool              `json:"is_relay"`
	IsExit           bool              `json:"is_exit"`
	Transports       []Transport       `json:"transports"`
	NodeSecret       string            `json:"node_secret"`
	GeneratedSecrets map[string]string `json:"generated_secrets"`
	SocksPort        int               `json:"socks_port"`
	Status           string            `json:"status"`
	LastError        string            `json:"last_error,omitempty"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

// EnabledTransport returns the first enabled transport of the given type.
func (n Node) EnabledTransport(kind string) (Transport, bool) {
	for _, tr := range n.Transports {
		if tr.Enabled && tr.Type == kind && strings.TrimSpace(tr.Domain) != "" {
			return tr, true
		}
	}
	return Transport{}, false
}

// Route binds an entry node transport to an exit node.
type Route struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	EntryNodeID   int64  `json:"entry_node_id"`
	ExitNodeID    int64  `json:"exit_node_id"`
	TransportType string `json:"transport_type"`
	Enabled       bool   `json:"enabled"`
}

// User is a subscription user.
type User struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	SubToken          string    `json:"sub_token"`
	Enabled           bool      `json:"enabled"`
	ExpiresAt         time.Time `json:"expires_at,omitempty"`
	TrafficLimitBytes int64     `json:"traffic_limit_bytes"`
	UsedUploadBytes   int64     `json:"used_upload_bytes"`
	UsedDownloadBytes int64     `json:"used_download_bytes"`
	AllowedRouteIDs   []int64   `json:"allowed_route_ids"`
}

// UsedBytes returns total accounted traffic.
func (u User) UsedBytes() int64 { return u.UsedUploadBytes + u.UsedDownloadBytes }

// Active reports whether the user may use the service at now.
func (u User) Active(now time.Time) bool {
	if !u.Enabled {
		return false
	}
	if !u.ExpiresAt.IsZero() && !now.Before(u.ExpiresAt) {
		return false
	}
	if u.TrafficLimitBytes > 0 && u.UsedBytes() >= u.TrafficLimitBytes {
		return false
	}
	return true
}

// RouteAllowed reports whether the user may use the route.
func (u User) RouteAllowed(routeID int64) bool {
	if len(u.AllowedRouteIDs) == 0 {
		return true
	}
	for _, id := range u.AllowedRouteIDs {
		if id == routeID {
			return true
		}
	}
	return false
}

// TrafficDelta is an incremental traffic report from a node.
type TrafficDelta struct {
	UserID        string `json:"user_id"`
	UploadBytes   int64  `json:"upload_bytes"`
	DownloadBytes int64  `json:"download_bytes"`
}

func validTransport(kind string) bool {
	switch kind {
	case TransportRawWS, TransportSilo, TransportLiveKit:
		return true
	default:
		return false
	}
}

func normalizeTransports(in []Transport) ([]Transport, error) {
	out := make([]Transport, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, tr := range in {
		kind := strings.TrimSpace(tr.Type)
		domain := strings.TrimSpace(tr.Domain)
		if !validTransport(kind) {
			return nil, invalidf("unsupported transport type %q", tr.Type)
		}
		if tr.Enabled && domain == "" {
			return nil, invalidf("transport %q requires a domain", kind)
		}
		key := kind + "\x00" + domain
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, Transport{Type: kind, Domain: domain, Enabled: tr.Enabled})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Type == out[j].Type {
			return out[i].Domain < out[j].Domain
		}
		return out[i].Type < out[j].Type
	})
	return out, nil
}

func normalizeAllowedRoutes(in []int64) []int64 {
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

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalid}, args...)...)
}
