package app

import (
	"bytes"
	"encoding/json"

	"github.com/Esteviredzu/shampoore/internal/auth"
)

type MultiuserConfig struct{}

func (MultiuserConfig) SupportsMultiuser() bool { return true }

func (m MultiuserConfig) PatchUsers(raw json.RawMessage, users []auth.UserSpec) (json.RawMessage, bool, error) {
	return PatchMultiuserUsers(raw, users)
}

func PatchMultiuserUsers(raw json.RawMessage, users []auth.UserSpec) (json.RawMessage, bool, error) {
	if len(raw) == 0 {
		return raw, false, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw, false, err
	}
	var multiuser bool
	if value, ok := obj["multiuser"]; ok {
		if err := json.Unmarshal(value, &multiuser); err != nil {
			return raw, false, err
		}
	}
	if !multiuser {
		return raw, false, nil
	}
	newUsers, err := json.Marshal(users)
	if err != nil {
		return raw, false, err
	}
	old, hadUsers := obj["users"]
	if hadUsers && bytes.Equal(bytes.TrimSpace(old), newUsers) {
		return raw, false, nil
	}
	if !hadUsers && len(users) == 0 {
		return raw, false, nil
	}
	obj["users"] = newUsers
	updated, err := json.Marshal(obj)
	if err != nil {
		return raw, false, err
	}
	return updated, true, nil
}
