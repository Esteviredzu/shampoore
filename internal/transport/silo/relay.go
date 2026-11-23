package silo

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

func (relayProvider) RelayUpstream() string { return "silo:9000" }

func (relayProvider) RelaySecrets() []string {
	return []string{"minio_root_user", "minio_root_password", "s3_bucket"}
}

func (relayProvider) SupportsDecoy() bool { return false }

func (relayProvider) RelayAsset(ctx capability.RelayContext) (capability.RelayAsset, error) {
	bucket := strings.TrimSpace(ctx.Secrets["s3_bucket"])
	if bucket == "" {
		return capability.RelayAsset{}, fmt.Errorf("silo transport is enabled but s3_bucket is empty")
	}
	tmpl, err := template.New("silo-relay").Parse(relayComposeTemplate)
	if err != nil {
		return capability.RelayAsset{}, err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, struct {
		RootUser     string
		RootPassword string
		Domain       string
		LocalRelay   bool
	}{
		RootUser:     ctx.Secrets["minio_root_user"],
		RootPassword: ctx.Secrets["minio_root_password"],
		Domain:       ctx.Domain,
		LocalRelay:   ctx.LocalRelay,
	}); err != nil {
		return capability.RelayAsset{}, err
	}
	return capability.RelayAsset{
		Name:     "silo",
		Upstream: "silo:9000",
		Compose:  buf.Bytes(),
		Steps: []capability.NodeStep{{
			Name:    "create silo bucket",
			Command: "docker exec shampoore-silo mkdir -p " + capability.ShellQuote("/data/"+bucket),
		}},
	}, nil
}
