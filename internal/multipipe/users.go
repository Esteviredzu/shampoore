package multipipe

import (
	"encoding/json"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
)

type multiuserCaps struct{}

func (multiuserCaps) SupportsMultiuser() bool { return true }

func (multiuserCaps) PatchUsers(raw json.RawMessage, users []auth.UserSpec) (json.RawMessage, bool, error) {
	var cfg config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return raw, false, err
	}
	changed := false
	for i := range cfg.Transports {
		child := &cfg.Transports[i]
		if child.Type == "" || len(child.Transport) == 0 {
			continue
		}
		var (
			updated      json.RawMessage
			childChanged bool
			err          error
		)
		if d, ok := app.DescriptorFor(child.Type); ok && d.Caps.Multiuser != nil {
			updated, childChanged, err = d.Caps.Multiuser.PatchUsers(child.Transport, users)
		} else {
			updated, childChanged, err = app.PatchMultiuserUsers(child.Transport, users)
		}
		if err != nil {
			return raw, false, err
		}
		if !childChanged {
			continue
		}
		child.Transport = updated
		changed = true
	}
	if !changed {
		return raw, false, nil
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return raw, false, err
	}
	return out, true, nil
}
