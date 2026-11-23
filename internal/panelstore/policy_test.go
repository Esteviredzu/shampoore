package panelstore

import (
	"context"
	"path/filepath"
	"testing"
)

type testPolicy struct {
	LegacyPolicy
}

func (testPolicy) Known(kind string) bool {
	return kind == "plugin_transport" || LegacyPolicy{}.Known(kind)
}

func TestLegacyPolicyMatchesHistoricalBehaviour(t *testing.T) {
	policy := LegacyPolicy{}
	for _, kind := range []string{TransportRawWS, TransportSilo, TransportLiveKit, TransportMulti} {
		if !policy.Known(kind) {
			t.Fatalf("LegacyPolicy does not know %q", kind)
		}
	}
	if policy.Known("plugin_transport") {
		t.Fatal("LegacyPolicy accepted an unknown transport")
	}
	if !policy.IsMulti(TransportMulti) || policy.IsMulti(TransportRawWS) {
		t.Fatal("LegacyPolicy multi detection is wrong")
	}
	if len(policy.MultiStrategies(TransportMulti)) != 2 {
		t.Fatalf("LegacyPolicy strategies = %+v", policy.MultiStrategies(TransportMulti))
	}
	if !policy.AllowsDecoy(TransportRawWS) || policy.AllowsDecoy(TransportSilo) {
		t.Fatal("LegacyPolicy decoy policy is wrong")
	}
	if !policy.EditableSecret("raw_ws_password") {
		t.Fatal("raw_ws_password should be editable")
	}
	if policy.EditableSecret("tunnel_id") {
		t.Fatal("tunnel_id should not be editable")
	}
}

func TestStoreAcceptsInjectedTransportPolicy(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(context.Background(), filepath.Join(dir, "panel.db"),
		WithTransportPolicy(testPolicy{}))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	node, err := store.CreateNode(context.Background(), Node{
		Name:    "plugin-relay",
		SSHHost: "203.0.113.1",
		IsRelay: true,
		Transports: []Transport{
			{Type: "plugin_transport", Domain: "plugin.example.com", Enabled: true},
		},
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	if node.GeneratedSecrets["raw_ws_password"] == "" {
		t.Fatal("legacy secrets were not generated through the policy")
	}
}
