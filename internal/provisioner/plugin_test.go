package provisioner

import (
	"context"
	"strings"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/capability"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

const pluginRelayKind = "provisioner_plugin_relay"

type pluginRelayCaps struct{}

func (pluginRelayCaps) RelayUpstream() string  { return "plugin:1234" }
func (pluginRelayCaps) RelaySecrets() []string { return []string{"plugin_secret"} }
func (pluginRelayCaps) SupportsDecoy() bool    { return true }

func (pluginRelayCaps) RelayAsset(ctx capability.RelayContext) (capability.RelayAsset, error) {
	return capability.RelayAsset{
		Name:     "plugin",
		Upstream: "plugin:1234",
		Compose:  []byte("  plugin:\n    image: busybox\n    command: [\"true\"]\n"),
		Files: []capability.Asset{{
			Name: "plugin.conf",
			Mode: 0o600,
			Data: []byte("domain=" + ctx.Domain + "\n"),
		}},
		Steps: []capability.NodeStep{{Name: "plugin step", Command: "true"}},
	}, nil
}

func registerPluginRelay() {
	if _, ok := app.DescriptorFor(pluginRelayKind); ok {
		return
	}
	_ = app.RegisterDescriptor(app.Descriptor{
		Kind: pluginRelayKind,
		Build: func(context.Context, *app.Settings, string, logging.ContextLogger, app.Values) (*app.BuildResult, error) {
			return &app.BuildResult{}, nil
		},
		Caps: app.Capabilities{Relay: pluginRelayCaps{}},
	})
}

func TestRenderRelayPlanUsesTransportCapability(t *testing.T) {
	registerPluginRelay()
	node := panelstore.Node{
		ID:      1,
		Name:    "plugin-relay",
		IsRelay: true,
		GeneratedSecrets: map[string]string{
			"plugin_secret": "secret",
		},
		Transports: []panelstore.Transport{{
			Type:    pluginRelayKind,
			Domain:  "plugin.example.com",
			Enabled: true,
		}},
	}
	result, err := RenderRelayPlan(node, RenderOptions{})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	files := fileMap(result.Files)
	compose := string(files["docker-compose.yml"].Data)
	if !strings.Contains(compose, "plugin:") {
		t.Fatalf("compose missing plugin service:\n%s", compose)
	}
	caddy := string(files["Caddyfile"].Data)
	if !strings.Contains(caddy, "reverse_proxy plugin:1234") {
		t.Fatalf("Caddyfile missing plugin upstream:\n%s", caddy)
	}
	if string(files["plugin.conf"].Data) != "domain=plugin.example.com\n" {
		t.Fatalf("plugin file = %q", files["plugin.conf"].Data)
	}
	if len(result.Steps) != 1 || result.Steps[0].Name != "plugin step" {
		t.Fatalf("steps = %+v", result.Steps)
	}
	if got := decoyRepoURL(node); got != "" {
		t.Fatalf("decoy without repo = %q", got)
	}
	node.Transports[0].DecoyRepoURL = "https://github.com/owner/repo"
	if got := decoyRepoURL(node); got == "" {
		t.Fatal("decoy repo was not picked from the capability")
	}
}
