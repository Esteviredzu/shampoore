package multipipe

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"

	_ "github.com/Esteviredzu/shampoore/internal/transport/livekit_datachannel"
)

const composerMockKind = "composer_mock_transport"

func registerComposerMock() {
	if _, ok := app.DescriptorFor(composerMockKind); ok {
		return
	}
	_ = app.RegisterDescriptor(app.Descriptor{
		Kind: composerMockKind,
		Build: func(context.Context, *app.Settings, string, logging.ContextLogger, app.Values) (*app.BuildResult, error) {
			return &app.BuildResult{}, nil
		},
	})
}

func TestComposerBuildsAnyRegisteredTransport(t *testing.T) {
	registerComposerMock()
	req := app.MultiRequest{
		Strategy:           "flowmux",
		RouteID:            9,
		RouteName:          "plugin-multi",
		UserID:             "alice",
		ExitNodeID:         "42",
		EncryptionPassword: "enc",
		Subflows: []app.SubflowSpec{
			{Type: composerMockKind, Transport: json.RawMessage(`{"x":1}`)},
		},
	}
	client, err := (composer{}).BuildSubscription(req)
	if err != nil {
		t.Fatalf("subscription: %v", err)
	}
	if client.Type != "multi" || client.UserID != "alice" || client.ExitNodeID != "42" ||
		client.EncryptionPassword != "enc" {
		t.Fatalf("subscription settings = %+v", client)
	}
	var cfg config
	if err := json.Unmarshal(client.Transport, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Strategy != "flowmux" || len(cfg.Transports) != 1 || cfg.Transports[0].Type != composerMockKind {
		t.Fatalf("multi config = %+v", cfg)
	}

	server, err := (composer{}).BuildExit(req)
	if err != nil {
		t.Fatalf("exit: %v", err)
	}
	if server.UserID != "" {
		t.Fatalf("exit settings must not carry UserID: %+v", server)
	}
}

func TestLiveKitChildConfigMatchesSingleRoute(t *testing.T) {
	d, ok := app.DescriptorFor("livekit_datachannel")
	if !ok || d.Caps.Client == nil {
		t.Skip("livekit_datachannel descriptor is not linked into this build")
	}
	secrets := map[string]string{
		"livekit_api_key":     "key",
		"livekit_api_secret":  "secret",
		"encryption_password": "pw",
	}
	params := app.RouteContext{
		Role:               app.RouteRoleSubscription,
		RouteID:            7,
		RouteName:          "route-7",
		UserID:             "alice",
		ExitNodeID:         "2",
		Domain:             "lk.example.com",
		EncryptionPassword: "pw",
		Secrets:            secrets,
	}
	single, err := d.ClientConfig(params)
	if err != nil {
		t.Fatalf("single client config: %v", err)
	}

	multi, err := (composer{}).BuildSubscription(app.MultiRequest{
		Strategy:           "flowmux",
		RouteID:            params.RouteID,
		RouteName:          params.RouteName,
		UserID:             params.UserID,
		ExitNodeID:         params.ExitNodeID,
		EncryptionPassword: params.EncryptionPassword,
		Subflows: []app.SubflowSpec{{
			Type:      single.Type,
			Transport: single.Transport,
		}},
	})
	if err != nil {
		t.Fatalf("multi subscription: %v", err)
	}
	if single.UserID != multi.UserID || single.ExitNodeID != multi.ExitNodeID ||
		single.EncryptionPassword != multi.EncryptionPassword {
		t.Fatalf("identity mismatch: single=%+v multi=%+v", single, multi)
	}

	var child transportConfig
	var cfg config
	if err := json.Unmarshal(multi.Transport, &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Transports) != 1 {
		t.Fatalf("multi transports = %d", len(cfg.Transports))
	}
	if err := json.Unmarshal(cfg.Transports[0].Transport, &child); err != nil {
		t.Fatal(err)
	}
	var direct transportConfig
	if err := json.Unmarshal(single.Transport, &direct); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"room", "room_prefix", "user_id", "exit_node_id", "signal_url", "data_topic", "api_key", "api_secret"} {
		if direct[key] != child[key] {
			t.Fatalf("%s differs: single=%v child=%v", key, direct[key], child[key])
		}
	}
	if direct["room"] != "route-7-alice" {
		t.Fatalf("livekit room = %v", direct["room"])
	}
}

type transportConfig map[string]any

func TestComposerRejectsNestedMulti(t *testing.T) {
	registerComposerMock()
	err := (composer{}).Validate(app.MultiRequest{
		Strategy: "bond",
		Subflows: []app.SubflowSpec{{Type: "multi"}},
	})
	if err == nil {
		t.Fatal("nested multi was accepted")
	}
}
