package livekit_datachannel

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

//go:embed livekit.yaml.tmpl
var livekitTemplate string

type relayProvider struct{}

func (relayProvider) RelayUpstream() string { return "livekit:7880" }

func (relayProvider) RelaySecrets() []string {
	return []string{"livekit_api_key", "livekit_api_secret"}
}

func (relayProvider) SupportsDecoy() bool { return false }

func (relayProvider) RelayAsset(ctx capability.RelayContext) (capability.RelayAsset, error) {
	apiKey := strings.TrimSpace(ctx.Secrets["livekit_api_key"])
	apiSecret := strings.TrimSpace(ctx.Secrets["livekit_api_secret"])
	if apiKey == "" || apiSecret == "" {
		return capability.RelayAsset{}, fmt.Errorf("livekit: livekit_api_key and livekit_api_secret are required")
	}
	compose, err := renderTemplate("livekit-relay", relayComposeTemplate, struct {
		LocalRelay bool
	}{LocalRelay: ctx.LocalRelay})
	if err != nil {
		return capability.RelayAsset{}, err
	}
	config, err := renderTemplate("livekit-config", livekitTemplate, struct {
		APIKey    string
		APISecret string
	}{APIKey: apiKey, APISecret: apiSecret})
	if err != nil {
		return capability.RelayAsset{}, err
	}
	return capability.RelayAsset{
		Name:     "livekit",
		Upstream: "livekit:7880",
		Compose:  compose,
		Files: []capability.Asset{{
			Name: "livekit.yaml",
			Mode: 0o600,
			Data: config,
		}},
	}, nil
}

func renderTemplate(name, source string, data any) ([]byte, error) {
	tmpl, err := template.New(name).Parse(source)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
