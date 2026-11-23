package multipipe

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"
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
