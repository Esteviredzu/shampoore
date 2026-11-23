package raw_ws

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

func (relayProvider) RelayUpstream() string { return "relay:8787" }

func (relayProvider) RelaySecrets() []string { return []string{"raw_ws_password"} }

func (relayProvider) SupportsDecoy() bool { return true }

func (relayProvider) RelayAsset(ctx capability.RelayContext) (capability.RelayAsset, error) {
	password := strings.TrimSpace(ctx.Secrets["raw_ws_password"])
	if password == "" {
		return capability.RelayAsset{}, fmt.Errorf("raw_ws: missing raw_ws_password")
	}
	tmpl, err := template.New("raw_ws-relay").Parse(relayComposeTemplate)
	if err != nil {
		return capability.RelayAsset{}, err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, struct {
		Password   string
		LocalRelay bool
	}{Password: password, LocalRelay: ctx.LocalRelay}); err != nil {
		return capability.RelayAsset{}, err
	}
	return capability.RelayAsset{
		Name:     "relay",
		Upstream: "relay:8787",
		Compose:  buf.Bytes(),
	}, nil
}
