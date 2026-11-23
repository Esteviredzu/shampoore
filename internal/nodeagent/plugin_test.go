package nodeagent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/logging"
)

const pluginPatchKind = "nodeagent_plugin_patch"

type pluginPatchCaps struct{}

func (pluginPatchCaps) SupportsMultiuser() bool { return true }

func (pluginPatchCaps) PatchUsers(raw json.RawMessage, users []auth.UserSpec) (json.RawMessage, bool, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw, false, err
	}
	encoded, err := json.Marshal(users)
	if err != nil {
		return raw, false, err
	}
	obj["members"] = encoded
	out, err := json.Marshal(obj)
	return out, true, err
}

func TestPatchUsersUsesTransportCapability(t *testing.T) {
	if _, ok := app.DescriptorFor(pluginPatchKind); !ok {
		_ = app.RegisterDescriptor(app.Descriptor{
			Kind: pluginPatchKind,
			Build: func(ctx context.Context, s *app.Settings, role string, logger logging.ContextLogger, eff app.Values) (*app.BuildResult, error) {
				return &app.BuildResult{}, nil
			},
			Caps: app.Capabilities{Multiuser: pluginPatchCaps{}},
		})
	}
	settings := &app.Settings{
		Type:      pluginPatchKind,
		Transport: json.RawMessage(`{"members":[]}`),
	}
	changed, err := patchUsers(settings, []auth.UserSpec{{UserID: "alice", Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("patchUsers reported no change")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(settings.Transport, &obj); err != nil {
		t.Fatal(err)
	}
	if _, ok := obj["members"]; !ok {
		t.Fatalf("capability patch was not applied: %s", settings.Transport)
	}
}
