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

func UsersFromTransport(raw json.RawMessage) []auth.UserSpec {
	if len(raw) == 0 {
		return nil
	}
	out := make([]auth.UserSpec, 0, 4)
	index := make(map[string]int)
	walkTransportUsers(raw, &out, index)
	return out
}

func walkTransportUsers(raw json.RawMessage, out *[]auth.UserSpec, index map[string]int) {
	if len(raw) == 0 {
		return
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return
	}
	if usersRaw, ok := obj["users"]; ok {
		var specs []auth.UserSpec
		if err := json.Unmarshal(usersRaw, &specs); err == nil {
			for _, spec := range specs {
				id, err := auth.NormalizeUserID(spec.UserID)
				if err != nil {
					continue
				}
				spec.UserID = id
				if pos, ok := index[id]; ok {
					(*out)[pos] = spec
					continue
				}
				index[id] = len(*out)
				*out = append(*out, spec)
			}
		}
	}
	childrenRaw, ok := obj["transports"]
	if !ok {
		return
	}
	var children []json.RawMessage
	if err := json.Unmarshal(childrenRaw, &children); err != nil {
		return
	}
	for _, child := range children {
		var childObj map[string]json.RawMessage
		if err := json.Unmarshal(child, &childObj); err != nil {
			continue
		}
		if tr, ok := childObj["transport"]; ok {
			walkTransportUsers(tr, out, index)
		}
	}
}

func TransportWithoutUsers(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage("{}"), nil
	}
	cleaned, err := stripUsers(raw)
	if err != nil {
		return nil, err
	}
	return json.Marshal(cleaned)
}

func stripUsers(raw json.RawMessage) (any, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return stripUsersValue(value), nil
}

func stripUsersValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			if key == "users" {
				delete(v, key)
				continue
			}
			v[key] = stripUsersValue(item)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = stripUsersValue(item)
		}
		return v
	default:
		return value
	}
}
