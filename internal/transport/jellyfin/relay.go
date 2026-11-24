package jellyfin

import (
	"bytes"
	_ "embed"
	"fmt"
	"strings"
	"text/template"

	"github.com/Esteviredzu/shampoore/internal/capability"
)

//go:embed relay_compose.yml.tmpl
var relayComposeTemplate string

type relayProvider struct{}

func (relayProvider) RelayUpstream() string { return "jellyfin-broker:8097" }

func (relayProvider) RelaySecrets() []string { return []string{"jellyfin_secret"} }

func (relayProvider) SupportsDecoy() bool { return false }

func (relayProvider) RelayAsset(ctx capability.RelayContext) (capability.RelayAsset, error) {
	secret := strings.TrimSpace(ctx.Secrets["jellyfin_secret"])
	if secret == "" {
		return capability.RelayAsset{}, fmt.Errorf("jellyfin: missing jellyfin_secret")
	}
	tmpl, err := template.New("jellyfin-relay").Parse(relayComposeTemplate)
	if err != nil {
		return capability.RelayAsset{}, err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, struct {
		Domain     string
		Secret     string
		LocalRelay bool
	}{
		Domain:     ctx.Domain,
		Secret:     secret,
		LocalRelay: ctx.LocalRelay,
	}); err != nil {
		return capability.RelayAsset{}, err
	}
	return capability.RelayAsset{
		Name:     "jellyfin-broker",
		Upstream: "jellyfin-broker:8097",
		Compose:  buf.Bytes(),
	}, nil
}
