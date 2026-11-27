package panelstore

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	TransportRawWS   = "raw_ws"
	TransportSilo    = "silo"
	TransportLiveKit = "livekit"
	TransportMulti   = "multi"

	MultiStrategyBond    = "bond"
	MultiStrategyFlowMux = "flowmux"

	EncryptionCipherName = "XChaCha20-Poly1305"

	NodeStatusNew       = "new"
	NodeStatusDeploying = "deploying"
	NodeStatusDeployed  = "deployed"
	NodeStatusOnline    = "online"
	NodeStatusStopped   = "stopped"
	NodeStatusError     = "error"
)

var (
	ErrNotFound = errors.New("panelstore: not found")
	ErrConflict = errors.New("panelstore: conflict")
	ErrInvalid  = errors.New("panelstore: invalid")
)

type Transport struct {
	Type         string `json:"type"`
	Domain       string `json:"domain"`
	DecoyRepoURL string `json:"decoy_repo_url,omitempty"`
	Enabled      bool   `json:"enabled"`
}

func normalizeNodeIDs(in []int64) ([]int64, error) {
	out := make([]int64, 0, len(in))
	seen := make(map[int64]struct{}, len(in))
	for _, id := range in {
		if id <= 0 {
			return nil, invalidf("invalid node id %d", id)
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

type NodeMetrics struct {
	CPUPercent     float64 `json:"cpu_percent"`
	MemUsedBytes   uint64  `json:"mem_used_bytes"`
	MemTotalBytes  uint64  `json:"mem_total_bytes"`
	DiskUsedBytes  uint64  `json:"disk_used_bytes"`
	DiskTotalBytes uint64  `json:"disk_total_bytes"`
}

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
	GeneratedSecrets map[string]string `json:"-"`
	Secrets          map[string]string `json:"secrets,omitempty"`
	EncryptionCipher string            `json:"encryption_cipher,omitempty"`
	Status           string            `json:"status"`
	LastError        string            `json:"last_error,omitempty"`
	CPUPercent       float64           `json:"cpu_percent"`
	MemUsedBytes     uint64            `json:"mem_used_bytes"`
	MemTotalBytes    uint64            `json:"mem_total_bytes"`
	DiskUsedBytes    uint64            `json:"disk_used_bytes"`
	DiskTotalBytes   uint64            `json:"disk_total_bytes"`
	LastSeen         time.Time         `json:"last_seen,omitempty"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

func (n Node) EnabledTransport(kind string) (Transport, bool) {
	for _, tr := range n.Transports {
		if tr.Enabled && tr.Type == kind && strings.TrimSpace(tr.Domain) != "" {
			return tr, true
		}
	}
	return Transport{}, false
}

type Route struct {
	ID            int64   `json:"id"`
	Name          string  `json:"name"`
	EntryNodeID   int64   `json:"entry_node_id"`
	ExitNodeID    int64   `json:"exit_node_id"`
	TransportType string  `json:"transport_type"`
	MultiStrategy string  `json:"multi_strategy,omitempty"`
	SubRouteIDs   []int64 `json:"sub_route_ids"`
	Enabled       bool    `json:"enabled"`
}

const UserOnlineWindow = 45 * time.Second

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

	MaxSpeedKBps int64     `json:"max_speed_kbps"`
	LastSeenAt   time.Time `json:"last_seen_at,omitempty"`
	LastSeenNode int64     `json:"last_seen_node,omitempty"`
}

func (u User) UsedBytes() int64 { return u.UsedUploadBytes + u.UsedDownloadBytes }

func (u User) Online(now time.Time) bool {
	if u.LastSeenAt.IsZero() {
		return false
	}
	return now.Sub(u.LastSeenAt) <= UserOnlineWindow
}

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

type TrafficDelta struct {
	UserID        string `json:"user_id"`
	UploadBytes   int64  `json:"upload_bytes"`
	DownloadBytes int64  `json:"download_bytes"`
}

func normalizeTransports(in []Transport, policy TransportPolicy) ([]Transport, error) {
	out := make([]Transport, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, tr := range in {
		kind := strings.TrimSpace(tr.Type)
		domain := strings.TrimSpace(tr.Domain)
		if !policy.Known(kind) {
			return nil, invalidf("unsupported transport type %q", tr.Type)
		}
		if tr.Enabled && domain == "" {
			return nil, invalidf("transport %q requires a domain", kind)
		}
		if _, ok := seen[kind]; ok {
			return nil, invalidf("duplicate transport type %q", kind)
		}
		decoyURL, err := normalizeDecoyRepoURL(kind, tr.DecoyRepoURL, policy)
		if err != nil {
			return nil, err
		}
		seen[kind] = struct{}{}
		out = append(out, Transport{Type: kind, Domain: domain, DecoyRepoURL: decoyURL, Enabled: tr.Enabled})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Type == out[j].Type {
			return out[i].Domain < out[j].Domain
		}
		return out[i].Type < out[j].Type
	})
	return out, nil
}

func normalizeDecoyRepoURL(kind, raw string, policy TransportPolicy) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if !policy.AllowsDecoy(kind) {
		return "", invalidf("decoy repo URL is not supported for transport %q", kind)
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return "", invalidf("decoy repo URL %q must be an https link", raw)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", invalidf("decoy repo URL %q must not contain credentials, query or fragment", raw)
	}
	if strings.HasSuffix(strings.ToLower(parsed.Path), ".zip") {
		return parsed.String(), nil
	}
	host := strings.ToLower(parsed.Host)
	if host != "github.com" && host != "www.github.com" {
		return "", invalidf("decoy repo URL %q must point to github.com or a .zip archive", raw)
	}
	segments := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(segments) != 2 {
		return "", invalidf("decoy repo URL %q must look like https://github.com/<owner>/<repo>", raw)
	}
	for _, segment := range segments {
		if !validGitHubSegment(segment) {
			return "", invalidf("decoy repo URL %q has an invalid owner or repository name", raw)
		}
	}
	return "https://github.com/" + segments[0] + "/" + segments[1], nil
}

func validGitHubSegment(segment string) bool {
	if segment == "" || segment == "." || segment == ".." {
		return false
	}
	for _, r := range segment {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return false
		}
	}
	return true
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
	return out
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalid}, args...)...)
}
