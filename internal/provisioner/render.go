package provisioner

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/capability"
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

type relayService struct {
	Name    string
	Compose []byte
}

type relayData struct {
	Node       panelstore.Node
	Secrets    map[string]string
	Sites      []relaySite
	Services   []relayService
	LocalRelay bool
	PanelURL   string
}

func (d relayData) UsesAgent() bool {
	return strings.TrimSpace(d.PanelURL) != "" && strings.TrimSpace(d.Node.NodeSecret) != ""
}

type RenderResult struct {
	Files []File
	Steps []capability.NodeStep
}

type exitData struct {
	Node        panelstore.Node
	Secrets     map[string]string
	PanelURL    string
	ConfigFiles []string
}

func RenderRelay(node panelstore.Node, opts RenderOptions) ([]File, error) {
	result, err := RenderRelayPlan(node, opts)
	if err != nil {
		return nil, err
	}
	return result.Files, nil
}

func RenderRelayPlan(node panelstore.Node, opts RenderOptions) (RenderResult, error) {
	if !node.IsRelay {
		return RenderResult{}, fmt.Errorf("provisioner: node %q is not a relay", node.Name)
	}
	data := relayData{
		Node:       node,
		Secrets:    cloneSecrets(node.GeneratedSecrets),
		LocalRelay: opts.LocalRelay,
		PanelURL:   strings.TrimSpace(opts.PanelURL),
	}
	siteIndex := make(map[string]int)
	seenServices := make(map[string]struct{})
	var steps []capability.NodeStep
	var extraFiles []File
	for _, tr := range node.Transports {
		if !tr.Enabled || strings.TrimSpace(tr.Domain) == "" {
			continue
		}
		descriptor, ok := app.DescriptorFor(tr.Type)
		if !ok || descriptor.Caps.Relay == nil {
			continue
		}
		for _, key := range descriptor.Caps.Relay.RelaySecrets() {
			if strings.TrimSpace(data.Secrets[key]) == "" {
				return RenderResult{}, fmt.Errorf("provisioner: relay %q has no %s for %s", node.Name, key, tr.Type)
			}
		}
		domain := app.SiteAddress(tr.Domain)
		asset, err := descriptor.Caps.Relay.RelayAsset(capability.RelayContext{
			NodeID:     node.ID,
			NodeName:   node.Name,
			Domain:     domain,
			LocalRelay: opts.LocalRelay,
			Secrets:    data.Secrets,
		})
		if err != nil {
			return RenderResult{}, fmt.Errorf("provisioner: relay %q: %w", node.Name, err)
		}
		name := strings.TrimSpace(asset.Name)
		if name == "" {
			name = descriptor.Kind
		}
		if _, ok := seenServices[name]; !ok && len(asset.Compose) > 0 {
			seenServices[name] = struct{}{}
			data.Services = append(data.Services, relayService{Name: name, Compose: asset.Compose})
		}
		for _, file := range asset.Files {
			extraFiles = append(extraFiles, File{Name: file.Name, Mode: file.Mode, Data: file.Data})
		}
		steps = append(steps, asset.Steps...)

		upstream := strings.TrimSpace(asset.Upstream)
		if upstream == "" {
			continue
		}
		idx, ok := siteIndex[upstream]
		if !ok {
			idx = len(data.Sites)
			siteIndex[upstream] = idx
			data.Sites = append(data.Sites, relaySite{Upstream: upstream})
		}
		data.Sites[idx].Domains = append(data.Sites[idx].Domains, domain)
	}
	if len(data.Sites) == 0 {
		return RenderResult{}, fmt.Errorf("provisioner: relay %q has no enabled transports with domains", node.Name)
	}
	fileList, err := renderTemplates("templates/relay/*.tmpl", data)
	if err != nil {
		return RenderResult{}, err
	}
	fileList = append(fileList, extraFiles...)
	if data.UsesAgent() {
		fileList = append(fileList, agentEnvFile(data.Node, data.PanelURL))
	}
	return RenderResult{Files: fileList, Steps: steps}, nil
}

func agentEnvFile(node panelstore.Node, panelURL string) File {
	data := fmt.Sprintf("PANEL_URL=%s\nNODE_ID=%d\nNODE_SECRET=%s\n"+
		"CONTROL_SOCKET=/run/shampoore/shampoore.sock\n"+
		"CONFIGS_DIR=/opt/shampoore/configs\n"+
		"STATE_FILE=/opt/shampoore/state/agent-state.json\nINTERVAL=2s\n",
		panelURL, node.ID, node.NodeSecret)
	return File{Name: "agent.env", Mode: 0o600, Data: []byte(data)}
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
		Node:     node,
		Secrets:  cloneSecrets(node.GeneratedSecrets),
		PanelURL: panelURL,
	}

	var configFiles []File
	routesByID := make(map[int64]panelstore.Route, len(routes))
	for _, route := range routes {
		routesByID[route.ID] = route
	}
	for _, route := range routes {
		if !route.Enabled {
			continue
		}
		entry, ok := byID[route.EntryNodeID]
		if !ok {
			return nil, fmt.Errorf("provisioner: route %d references missing entry node %d", route.ID, route.EntryNodeID)
		}
		settings, err := exitRouteSettings(node, entry, route, routesByID)
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

	seen := make(map[string]struct{}, len(relayFiles)+len(exitFiles))
	appendFile := func(f File) {
		if _, ok := seen[f.Name]; ok {
			return
		}
		seen[f.Name] = struct{}{}
		out = append(out, f)
	}
	for _, f := range relayFiles {
		if f.Name == "docker-compose.yml" {
			relayCompose = f.Data
			continue
		}
		appendFile(f)
	}
	for _, f := range exitFiles {
		if f.Name == "docker-compose.yml" {
			exitCompose = f.Data
			continue
		}
		appendFile(f)
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

func exitRouteSettings(
	node, entry panelstore.Node,
	route panelstore.Route,
	routesByID map[int64]panelstore.Route,
) (app.Settings, error) {
	descriptor, ok := app.DescriptorFor(route.TransportType)
	if ok && descriptor.Caps.Multi != nil {
		return multiExitSettings(node, entry, route, routesByID)
	}
	return singleExitSettings(node, entry, route, route)
}

func singleExitSettings(
	node, entry panelstore.Node,
	route panelstore.Route,
	namespace panelstore.Route,
) (app.Settings, error) {
	tr, ok := entry.EnabledTransport(route.TransportType)
	if !ok {
		return app.Settings{}, fmt.Errorf("provisioner: route %d entry node %q has no enabled %s transport", route.ID, entry.Name, route.TransportType)
	}
	descriptor, ok := app.DescriptorFor(route.TransportType)
	if !ok || descriptor.Caps.Server == nil {
		return app.Settings{}, fmt.Errorf("provisioner: unsupported route transport %q", route.TransportType)
	}
	ctx := app.RouteContext{
		Role:               app.RouteRoleExit,
		RouteID:            namespace.ID,
		RouteName:          namespace.Name,
		UserID:             "",
		ExitNodeID:         strconv.FormatInt(node.ID, 10),
		Domain:             tr.Domain,
		EncryptionPassword: entry.GeneratedSecrets["encryption_password"],
		Secrets:            entry.GeneratedSecrets,
	}
	var settings app.Settings
	var err error
	if route.ID != namespace.ID {
		ctx.Role = app.RouteRoleExitSubflow
		ctx.SubRouteID = route.ID
		ctx.SubRouteName = route.Name
		settings, err = descriptor.ExitSubflowConfig(ctx)
	} else {
		settings, err = descriptor.ExitConfig(ctx)
	}
	if err != nil {
		return app.Settings{}, fmt.Errorf("provisioner: route %d: %w", route.ID, err)
	}
	if strings.TrimSpace(settings.Name) == "" {
		settings.Name = route.Name
	}
	return settings, nil
}

func multiExitSettings(
	node, entry panelstore.Node,
	route panelstore.Route,
	routesByID map[int64]panelstore.Route,
) (app.Settings, error) {
	descriptor, ok := app.DescriptorFor(route.TransportType)
	if !ok || descriptor.Caps.Multi == nil {
		return app.Settings{}, fmt.Errorf("provisioner: route %d: transport %q is not a multi composer",
			route.ID, route.TransportType)
	}
	req := app.MultiRequest{
		Strategy:           route.MultiStrategy,
		RouteID:            route.ID,
		RouteName:          route.Name,
		ExitNodeID:         strconv.FormatInt(node.ID, 10),
		EncryptionPassword: entry.GeneratedSecrets["encryption_password"],
	}
	for _, id := range route.SubRouteIDs {
		child, ok := routesByID[id]
		if !ok {
			return app.Settings{}, fmt.Errorf("provisioner: multi route %d: sub-route %d not found", route.ID, id)
		}
		childDescriptor, ok := app.DescriptorFor(child.TransportType)
		if ok && childDescriptor.Caps.Multi != nil {
			return app.Settings{}, fmt.Errorf("provisioner: multi route %d: nested multi route %d", route.ID, id)
		}
		childSettings, err := singleExitSettings(node, entry, child, route)
		if err != nil {
			return app.Settings{}, err
		}
		req.Subflows = append(req.Subflows, app.SubflowSpec{
			Type:      childSettings.Type,
			Name:      child.Name,
			Transport: childSettings.Transport,
		})
	}
	if err := descriptor.Caps.Multi.Validate(req); err != nil {
		return app.Settings{}, fmt.Errorf("provisioner: route %d: %w", route.ID, err)
	}
	return descriptor.Caps.Multi.BuildExit(req)
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
