package app

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/logging"
)

func TestLegacyCapabilitiesAreAdapted(t *testing.T) {
	const kind = "test_capability_legacy"
	err := RegisterDescriptor(Descriptor{
		Kind: kind,
		Build: func(context.Context, *Settings, string, logging.ContextLogger, Values) (*BuildResult, error) {
			return &BuildResult{}, nil
		},
		DecodeURL: func(url.Values) (json.RawMessage, error) {
			return json.RawMessage(`{"decoded":true}`), nil
		},
		EncodeURL: func(json.RawMessage, url.Values) error { return nil },
		BuildSubscriptionConfig: func(p RouteParams) (Settings, error) {
			return Settings{Type: "client", Name: p.Namespace()}, nil
		},
		BuildServerConfig: func(p RouteParams) (Settings, error) {
			return Settings{Type: "server", Name: p.Namespace()}, nil
		},
		RelayUpstream: "relay:8787",
		RelaySecrets:  []string{"pw"},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	d, ok := DescriptorFor(kind)
	if !ok {
		t.Fatal("descriptor not found")
	}
	if d.Caps.Client == nil || d.Caps.Server == nil || d.Caps.Link == nil || d.Caps.Relay == nil {
		t.Fatalf("legacy capabilities were not adapted: %+v", d.Caps)
	}
	got, err := d.ClientConfig(RouteContext{RouteID: 7})
	if err != nil || got.Type != "client" || got.Name != "route-7" {
		t.Fatalf("client config = %+v, %v", got, err)
	}
	got, err = d.ExitConfig(RouteContext{RouteID: 8})
	if err != nil || got.Type != "server" || got.Name != "route-8" {
		t.Fatalf("exit config = %+v, %v", got, err)
	}
	if d.RelayUpstreamAddr() != "relay:8787" {
		t.Fatalf("relay upstream = %q", d.RelayUpstreamAddr())
	}
	if _, err := d.DecodeLink(url.Values{}); err != nil {
		t.Fatalf("decode link: %v", err)
	}
}

func TestRouteContextNamespace(t *testing.T) {
	cases := []struct {
		ctx  RouteContext
		want string
	}{
		{RouteContext{RouteID: 42}, "route-42"},
		{RouteContext{RouteName: "alpha"}, "alpha"},
		{RouteContext{}, "route"},
	}
	for _, tc := range cases {
		if got := tc.ctx.Namespace(); got != tc.want {
			t.Fatalf("Namespace() = %q, want %q", got, tc.want)
		}
	}
}
