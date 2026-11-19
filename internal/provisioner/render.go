package provisioner

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

//go:embed templates
var templateFS embed.FS

// RenderOptions controls node config rendering.
type RenderOptions struct {
	PanelURL string
}

type relayData struct {
	Node           panelstore.Node
	Secrets        map[string]string
	RawWSDomains   []string
	SiloDomains    []string
	LiveKitDomains []string
}

type exitData struct {
	Node          panelstore.Node
	Secrets       map[string]string
	PanelURL      string
	SocksPort     int
	ExitUpstreams []string
	ConfigFiles   []string
}

// RenderRelay renders the relay node stack (Caddy, WS relay, LiveKit, MinIO).
func RenderRelay(node panelstore.Node, opts RenderOptions) ([]File, error) {
	if !node.IsRelay {
		return nil, fmt.Errorf("provisioner: node %q is not a relay", node.Name)
	}
	data := relayData{
		Node:    node,
		Secrets: cloneSecrets(node.GeneratedSecrets),
	}
	for _, tr := range node.Transports {
		if !tr.Enabled || strings.TrimSpace(tr.Domain) == "" {
			continue
		}
		switch tr.Type {
		case panelstore.TransportRawWS:
			data.RawWSDomains = append(data.RawWSDomains, siteAddress(tr.Domain))
		case panelstore.TransportSilo:
			data.SiloDomains = append(data.SiloDomains, siteAddress(tr.Domain))
		case panelstore.TransportLiveKit:
			data.LiveKitDomains = append(data.LiveKitDomains, siteAddress(tr.Domain))
		}
	}
	if len(data.RawWSDomains)+len(data.SiloDomains)+len(data.LiveKitDomains) == 0 {
		return nil, fmt.Errorf("provisioner: relay %q has no enabled transports with domains", node.Name)
	}
	if len(data.RawWSDomains) > 0 && data.Secrets["raw_ws_password"] == "" {
		return nil, fmt.Errorf("provisioner: relay %q has no raw_ws_password", node.Name)
	}
	if len(data.LiveKitDomains) > 0 && (data.Secrets["livekit_api_key"] == "" || data.Secrets["livekit_api_secret"] == "") {
		return nil, fmt.Errorf("provisioner: relay %q has no LiveKit credentials", node.Name)
	}
	if len(data.SiloDomains) > 0 && (data.Secrets["minio_root_user"] == "" || data.Secrets["minio_root_password"] == "" || data.Secrets["s3_bucket"] == "") {
		return nil, fmt.Errorf("provisioner: relay %q has no MinIO credentials", node.Name)
	}
	return renderTemplates("templates/relay/*.tmpl", data)
}

// RenderExit renders the exit node server, agent and multiuser configs.
func RenderExit(node panelstore.Node, routes []panelstore.Route, nodes []panelstore.Node, opts RenderOptions) ([]File, error) {
	if !node.IsExit {
		return nil, fmt.Errorf("provisioner: node %q is not an exit", node.Name)
	}
	panelURL := strings.TrimSpace(opts.PanelURL)
	if panelURL == "" {
		return nil, fmt.Errorf("provisioner: panel public URL is required for the exit agent")
	}
	byID := make(map[int64]panelstore.Node, len(nodes))
	for _, n := range nodes {
		byID[n.ID] = n
	}
	data := exitData{
		Node:      node,
		Secrets:   cloneSecrets(node.GeneratedSecrets),
		PanelURL:  panelURL,
		SocksPort: node.SocksPort,
	}
	if data.SocksPort == 0 {
		data.SocksPort = 1080
	}
	for _, other := range nodes {
		if !other.IsExit || other.ID == node.ID || other.SocksPort <= 0 || strings.TrimSpace(other.SSHHost) == "" {
			continue
		}
		addr := net.JoinHostPort(strings.TrimSpace(other.SSHHost), strconv.Itoa(other.SocksPort))
		data.ExitUpstreams = append(data.ExitUpstreams, fmt.Sprintf("%d=socks5://%s", other.ID, addr))
	}

	var configFiles []File
	for _, route := range routes {
		if !route.Enabled {
			continue
		}
		entry, ok := byID[route.EntryNodeID]
		if !ok {
			return nil, fmt.Errorf("provisioner: route %d references missing entry node %d", route.ID, route.EntryNodeID)
		}
		settings, err := exitRouteSettings(node, entry, route)
		if err != nil {
			return nil, err
		}
		raw, err := json.MarshalIndent(settings, "", "  ")
		if err != nil {
			return nil, err
		}
		raw = append(raw, '\n')
		name := "configs/" + configFileName(route) + ".json"
		data.ConfigFiles = append(data.ConfigFiles, name)
		configFiles = append(configFiles, File{Name: name, Mode: 0o600, Data: raw})
	}

	out, err := renderTemplates("templates/exit/*.tmpl", data)
	if err != nil {
		return nil, err
	}
	out = append(out, File{Name: "rules/.keep", Mode: 0o644})
	out = append(out, configFiles...)
	return out, nil
}

// mergeRoleFiles combines relay and exit outputs for nodes that host both roles.
func mergeRoleFiles(relayFiles, exitFiles []File) ([]File, error) {
	var relayCompose, exitCompose []byte
	out := make([]File, 0, len(relayFiles)+len(exitFiles))
	for _, f := range relayFiles {
		if f.Name == "docker-compose.yml" {
			relayCompose = f.Data
			continue
		}
		out = append(out, f)
	}
	for _, f := range exitFiles {
		if f.Name == "docker-compose.yml" {
			exitCompose = f.Data
			continue
		}
		out = append(out, f)
	}
	if len(relayCompose) == 0 || len(exitCompose) == 0 {
		return nil, fmt.Errorf("provisioner: combined role is missing a compose file")
	}
	var relayMap, exitMap map[string]any
	if err := yaml.Unmarshal(relayCompose, &relayMap); err != nil {
		return nil, fmt.Errorf("provisioner: decode relay compose: %w", err)
	}
	if err := yaml.Unmarshal(exitCompose, &exitMap); err != nil {
		return nil, fmt.Errorf("provisioner: decode exit compose: %w", err)
	}
	mergeMaps(relayMap, exitMap)
	merged, err := yaml.Marshal(relayMap)
	if err != nil {
		return nil, fmt.Errorf("provisioner: encode combined compose: %w", err)
	}
	out = append(out, File{Name: "docker-compose.yml", Mode: 0o600, Data: merged})
	return out, nil
}

func mergeMaps(dst, src map[string]any) {
	for k, v := range src {
		if dm, ok := dst[k].(map[string]any); ok {
			if sm, ok := v.(map[string]any); ok {
				mergeMaps(dm, sm)
				continue
			}
		}
		dst[k] = v
	}
}

func cloneSecrets(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func configFileName(route panelstore.Route) string {
	return fmt.Sprintf("route-%d", route.ID)
}

func exitRouteSettings(node, entry panelstore.Node, route panelstore.Route) (map[string]any, error) {
	tr, ok := entry.EnabledTransport(route.TransportType)
	if !ok {
		return nil, fmt.Errorf("provisioner: route %d entry node %q has no enabled %s transport", route.ID, entry.Name, route.TransportType)
	}
	secrets := entry.GeneratedSecrets
	transport := map[string]any{
		"multiuser": true,
		"users":     []any{},
	}
	var kind string
	switch route.TransportType {
	case panelstore.TransportRawWS:
		if secrets["raw_ws_password"] == "" {
			return nil, fmt.Errorf("provisioner: route %d entry node has no raw_ws_password", route.ID)
		}
		kind = "raw_ws"
		serverURL := wsURL(tr.Domain)
		transport["server_url"] = serverURL
		transport["room"] = configFileName(route)
		transport["password"] = secrets["raw_ws_password"]
		transport["batch"] = true
		transport["padding"] = true
		if strings.HasPrefix(serverURL, "ws://") {
			transport["allow_insecure_ws"] = true
		}
	case panelstore.TransportSilo:
		if secrets["s3_access_key"] == "" || secrets["s3_secret_key"] == "" || secrets["s3_bucket"] == "" {
			return nil, fmt.Errorf("provisioner: route %d entry node has no Silo credentials", route.ID)
		}
		kind = "silo"
		transport["endpoint"] = hostOnly(tr.Domain)
		transport["access_key"] = secrets["s3_access_key"]
		transport["secret_key"] = secrets["s3_secret_key"]
		transport["bucket"] = secrets["s3_bucket"]
		transport["use_ssl"] = !strings.HasPrefix(strings.TrimSpace(tr.Domain), "http://")
		transport["tunnel_id"] = configFileName(route)
	case panelstore.TransportLiveKit:
		if secrets["livekit_api_key"] == "" || secrets["livekit_api_secret"] == "" {
			return nil, fmt.Errorf("provisioner: route %d entry node has no LiveKit credentials", route.ID)
		}
		kind = "livekit_datachannel"
		transport["signal_url"] = wsURL(tr.Domain)
		transport["api_key"] = secrets["livekit_api_key"]
		transport["api_secret"] = secrets["livekit_api_secret"]
		transport["room_prefix"] = "shampoore-" + configFileName(route)
		transport["data_topic"] = "shampoore"
		tokenListen := secrets["livekit_token_listen"]
		if tokenListen == "" {
			tokenListen = "127.0.0.1:8099"
		}
		transport["token_listen"] = tokenListen
	default:
		return nil, fmt.Errorf("provisioner: unsupported route transport %q", route.TransportType)
	}
	settings := map[string]any{
		"name":         route.Name,
		"type":         kind,
		"transport":    transport,
		"user_id":      "",
		"exit_node_id": strconv.FormatInt(node.ID, 10),
	}
	if password := secrets["encryption_password"]; password != "" {
		settings["encryption_password"] = password
	}
	return settings, nil
}

func wsURL(domain string) string {
	domain = strings.TrimSpace(domain)
	switch {
	case strings.HasPrefix(domain, "wss://"), strings.HasPrefix(domain, "ws://"):
		return domain
	case strings.HasPrefix(domain, "https://"):
		return "wss://" + strings.TrimPrefix(domain, "https://")
	case strings.HasPrefix(domain, "http://"):
		return "ws://" + strings.TrimPrefix(domain, "http://")
	default:
		return "wss://" + domain
	}
}

func siteAddress(domain string) string {
	domain = strings.TrimSpace(domain)
	for _, prefix := range []string{"wss://", "ws://", "https://", "http://"} {
		domain = strings.TrimPrefix(domain, prefix)
	}
	return strings.TrimSuffix(domain, "/")
}

func hostOnly(domain string) string {
	domain = strings.TrimSpace(domain)
	domain = strings.TrimPrefix(domain, "https://")
	domain = strings.TrimPrefix(domain, "http://")
	return strings.TrimSuffix(domain, "/")
}

func renderTemplates(pattern string, data any) ([]File, error) {
	tmpl, err := template.New("").Option("missingkey=error").ParseFS(templateFS, pattern)
	if err != nil {
		return nil, fmt.Errorf("provisioner: parse templates: %w", err)
	}
	out := make([]File, 0, len(tmpl.Templates()))
	for _, t := range tmpl.Templates() {
		name := t.Name()
		if !strings.HasSuffix(name, ".tmpl") {
			continue
		}
		var buf bytes.Buffer
		if err := t.Execute(&buf, data); err != nil {
			return nil, fmt.Errorf("provisioner: render %s: %w", name, err)
		}
		outName := strings.TrimPrefix(name, "templates/relay/")
		outName = strings.TrimPrefix(outName, "templates/exit/")
		outName = strings.TrimSuffix(outName, ".tmpl")
		mode := int64(0o644)
		if strings.HasSuffix(outName, ".env") || strings.HasSuffix(outName, ".yaml") ||
			strings.HasSuffix(outName, ".yml") {
			mode = 0o600
		}
		out = append(out, File{Name: outName, Mode: mode, Data: buf.Bytes()})
	}
	return out, nil
}
