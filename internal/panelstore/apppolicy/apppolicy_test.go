package apppolicy_test

import (
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore/apppolicy"

	_ "github.com/Esteviredzu/shampoore/internal/transport/all"
)

func TestPolicyExposesJellyfinPairBroker(t *testing.T) {
	policy := apppolicy.Policy{}
	if !policy.Known("jellyfin") {
		t.Fatal("policy does not know jellyfin")
	}
	if policy.IsMulti("jellyfin") {
		t.Fatal("jellyfin must not be a multi composer")
	}
	if policy.AllowsDecoy("jellyfin") {
		t.Fatal("jellyfin is its own decoy")
	}

	found := false
	for _, spec := range policy.Secrets() {
		if spec.Key == "jellyfin_secret" {
			found = true
			if !spec.Editable {
				t.Fatal("jellyfin_secret must be editable")
			}
			if spec.Generator.Kind != "hex" || spec.Generator.Bytes != 32 {
				t.Fatalf("jellyfin_secret generator = %+v", spec.Generator)
			}
		}
	}
	if !found {
		t.Fatal("policy secrets do not include jellyfin_secret")
	}

	descriptor, ok := app.DescriptorFor("jellyfin")
	if !ok {
		t.Fatal("jellyfin descriptor is not registered")
	}
	if got := descriptor.RelayUpstreamAddr(); got != "jellyfin-broker:8097" {
		t.Fatalf("relay upstream = %q", got)
	}
	if descriptor.Caps.Relay == nil {
		t.Fatal("jellyfin relay capability is missing")
	}
	for _, key := range descriptor.Caps.Relay.RelaySecrets() {
		if key == "encryption_password" {
			t.Fatal("relay must not require the encryption password")
		}
	}
}
