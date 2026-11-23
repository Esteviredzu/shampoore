package panel

import "testing"

func TestLoadTemplatesExposesCapabilities(t *testing.T) {
	templates := LoadTemplates()
	byKind := make(map[string]Template, len(templates))
	for _, tpl := range templates {
		byKind[tpl.Kind] = tpl
	}
	rawWS, ok := byKind["raw_ws"]
	if !ok {
		t.Fatal("raw_ws template is missing")
	}
	if !rawWS.Capabilities.Deployable || !rawWS.Capabilities.DefaultRelay ||
		!rawWS.Capabilities.ClientConfig || !rawWS.Capabilities.ServerConfig ||
		!rawWS.Capabilities.Multiuser || !rawWS.Capabilities.Decoy {
		t.Fatalf("raw_ws capabilities = %+v", rawWS.Capabilities)
	}
	if len(rawWS.Capabilities.Secrets) == 0 {
		t.Fatal("raw_ws secrets are not exposed")
	}
	multi, ok := byKind["multi"]
	if !ok {
		t.Fatal("multi template is missing")
	}
	if !multi.Capabilities.Multi || len(multi.Capabilities.Strategies) != 2 {
		t.Fatalf("multi capabilities = %+v", multi.Capabilities)
	}
}
