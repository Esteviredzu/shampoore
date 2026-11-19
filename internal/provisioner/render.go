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

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

//go:embed templates
var templateFS embed.FS

type RenderOptions struct {
	PanelURL   string
	LocalRelay bool
	RuleFiles  []File
}

type relaySite struct {
	Upstream string
	Domains  []string
}

type relayData struct {
	Node       panelstore.Node
	Secrets    map[string]string
	Sites      []relaySite
	Domains    map[string]string
	HasRawWS   bool
	HasLiveKit bool
	HasSilo    bool
	LocalRelay bool
}

type exitData struct {
	Node          panelstore.Node
	Secrets       map[string]string
	PanelURL      string
	SocksPort     int
	ExitUpstreams []string
	ConfigFiles   []string
}

func RenderRelay(node panelstore.Node, opts RenderOptions) ([]File, error) {
	if !node.IsRelay {
		return nil, fmt.Errorf("provisioner: node %q is not a relay", node.Name)
	}
	data := relayData{
		Node:       node,
		Secrets:    cloneSecrets(node.GeneratedSecrets),
		Domains:    make(map[string]string),
		LocalRelay: opts.LocalRelay,
	}
	siteIndex := make(map[string]int)
	for _, tr := range node.Transports {
		if !tr.Enabled || strings.TrimSpace(tr.Domain) == "" {
			continue
		}
		descriptor, ok := app.DescriptorFor(tr.Type)
		if !ok || strings.TrimSpace(descriptor.RelayUpstream) == "" {
			continue
		}
		for _, key := range descriptor.RelaySecrets {
			if strings.TrimSpace(data.Secrets[key]) == "" {
				return nil, fmt.Errorf("provisioner: relay %q has no %s for %s", node.Name, key, tr.Type)
			}
		}
		domain := app.SiteAddress(tr.Domain)
		if _, ok := data.Domains[descriptor.Kind]; !ok {
			data.Domains[descriptor.Kind] = domain
		}
		switch descriptor.Kind {
		case panelstore.TransportRawWS:
			data.HasRawWS = true
		case "livekit_datachannel":

			data.HasLiveKit = true
		case panelstore.TransportSilo:
			data.HasSilo = true
		}
		idx, ok := siteIndex[descriptor.RelayUpstream]
		if !ok {
			idx = len(data.Sites)
			siteIndex[descriptor.RelayUpstream] = idx
			data.Sites = append(data.Sites, relaySite{Upstream: descriptor.RelayUpstream})
		}
		data.Sites[idx].Domains = append(data.Sites[idx].Domains, domain)
	}
	if len(data.Sites) == 0 {
		return nil, fmt.Errorf("provisioner: relay %q has no enabled transports with domains", node.Name)
	}
	return renderTemplates("templates/relay/*.tmpl", data)
}

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
	if len(opts.RuleFiles) == 0 {
		out = append(out, File{Name: "rules/.keep", Mode: 0o644})
	} else {
		out = append(out, opts.RuleFiles...)
	}
	out = append(out, configFiles...)
	return out, nil
}

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

func exitRouteSettings(node, entry panelstore.Node, route panelstore.Route) (app.Settings, error) {
	tr, ok := entry.EnabledTransport(route.TransportType)
	if !ok {
		return app.Settings{}, fmt.Errorf("provisioner: route %d entry node %q has no enabled %s transport", route.ID, entry.Name, route.TransportType)
	}
	descriptor, ok := app.DescriptorFor(route.TransportType)
	if !ok || descriptor.BuildServerConfig == nil {
		return app.Settings{}, fmt.Errorf("provisioner: unsupported route transport %q", route.TransportType)
	}
	settings, err := descriptor.BuildServerConfig(app.RouteParams{
		RouteID:    route.ID,
		RouteName:  route.Name,
		ExitNodeID: strconv.FormatInt(node.ID, 10),
		Domain:     tr.Domain,
		Secrets:    entry.GeneratedSecrets,
	})
	if err != nil {
		return app.Settings{}, fmt.Errorf("provisioner: route %d: %w", route.ID, err)
	}
	if strings.TrimSpace(settings.Name) == "" {
		settings.Name = route.Name
	}
	return settings, nil
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
