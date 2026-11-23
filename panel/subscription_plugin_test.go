package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

const panelPluginKind = "panel_plugin_transport"

type panelPluginClient struct{}

func (panelPluginClient) SubscriptionConfig(ctx app.RouteContext) (app.Settings, error) {
	return app.Settings{
		Type:               panelPluginKind,
		Name:               ctx.Namespace(),
		UserID:             ctx.UserID,
		ExitNodeID:         ctx.ExitNodeID,
		EncryptionPassword: ctx.EncryptionPassword,
		Transport:          json.RawMessage(`{"plugin":"ok"}`),
	}, nil
}

func registerPanelPlugin() {
	if _, ok := app.DescriptorFor(panelPluginKind); ok {
		return
	}
	_ = app.RegisterDescriptor(app.Descriptor{
		Kind: panelPluginKind,
		Build: func(context.Context, *app.Settings, string, logging.ContextLogger, app.Values) (*app.BuildResult, error) {
			return &app.BuildResult{}, nil
		},
		Caps: app.Capabilities{Client: panelPluginClient{}},
	})
}

func TestSubscriptionUsesRegisteredPluginTransport(t *testing.T) {
	registerPanelPlugin()
	api, store, _, _ := newControlTestAPI(t)
	ctx := context.Background()

	relay, err := store.CreateNode(ctx, panelstore.Node{
		Name:    "plugin-relay",
		SSHHost: "203.0.113.11",
		IsRelay: true,
		Transports: []panelstore.Transport{{
			Type:    panelPluginKind,
			Domain:  "plugin.example.com",
			Enabled: true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	exit, err := store.CreateNode(ctx, panelstore.Node{
		Name:    "plugin-exit",
		SSHHost: "198.51.100.11",
		IsExit:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRoute(ctx, panelstore.Route{
		Name:          "plugin-route",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: panelPluginKind,
		Enabled:       true,
	}); err != nil {
		t.Fatal(err)
	}
	user, err := store.CreateUser(ctx, panelstore.User{Name: "Plugin User", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateSettings(ctx, panelstore.Settings{PublicDomain: "sub.example.com"}); err != nil {
		t.Fatalf("enable subscriptions: %v", err)
	}

	rec := perform(api.PublicHandler(), http.MethodGet, "/sub/"+user.SubToken, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("subscription = %d: %s", rec.Code, rec.Body.String())
	}
	var settings []app.Settings
	if err := json.Unmarshal(rec.Body.Bytes(), &settings); err != nil {
		t.Fatal(err)
	}
	if len(settings) != 1 || settings[0].Type != panelPluginKind {
		t.Fatalf("settings = %+v", settings)
	}
	if settings[0].UserID != user.ID || settings[0].ExitNodeID == "" {
		t.Fatalf("identity = %q/%q", settings[0].UserID, settings[0].ExitNodeID)
	}
}
