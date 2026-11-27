package app

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestUsersFromTransportIncludesNestedChildren(t *testing.T) {
	raw := json.RawMessage(`{
		"multiuser": true,
		"users": [{"user_id":"alice","enabled":true,"max_speed_kbps":512}],
		"transports": [
			{"transport": {"multiuser": true, "users": [
				{"user_id":"bob","enabled":true},
				{"user_id":"alice","enabled":true,"max_speed_kbps":1024}
			]}}
		]
	}`)
	specs := UsersFromTransport(raw)
	if len(specs) != 2 {
		t.Fatalf("specs = %+v, want 2 users", specs)
	}
	byID := make(map[string]int64, len(specs))
	for _, spec := range specs {
		byID[spec.UserID] = spec.MaxSpeedKBps
	}
	if got := byID["bob"]; got != 0 {
		t.Fatalf("bob max speed = %d, want 0", got)
	}
	if got := byID["alice"]; got != 1024 {
		t.Fatalf("alice max speed = %d, want the nested (later) value 1024", got)
	}
}

func TestTransportWithoutUsersStripsNestedUsers(t *testing.T) {
	raw := json.RawMessage(`{
		"multiuser": true,
		"room": "route-1",
		"users": [{"user_id":"alice","enabled":true}],
		"transports": [
			{"name":"raw_ws","transport": {"multiuser": true, "users": [{"user_id":"bob"}], "room":"r"}}
		]
	}`)
	cleaned, err := TransportWithoutUsers(raw)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(cleaned, []byte(`"users"`)) {
		t.Fatalf("users were not stripped: %s", cleaned)
	}
	var obj map[string]any
	if err := json.Unmarshal(cleaned, &obj); err != nil {
		t.Fatal(err)
	}
	if obj["room"] != "route-1" {
		t.Fatalf("unrelated fields lost: %s", cleaned)
	}

	withOtherChange := json.RawMessage(`{
		"multiuser": true,
		"room": "route-2",
		"users": [{"user_id":"bob"}],
		"transports": [
			{"name":"raw_ws","transport": {"multiuser": true, "users": [{"user_id":"bob"}], "room":"r"}}
		]
	}`)
	other, err := TransportWithoutUsers(withOtherChange)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(cleaned, other) {
		t.Fatal("a room change must not look like a users-only change")
	}

	usersOnly := json.RawMessage(`{
		"multiuser": true,
		"room": "route-1",
		"users": [{"user_id":"carol"}],
		"transports": [
			{"name":"raw_ws","transport": {"multiuser": true, "users": [{"user_id":"carol"}], "room":"r"}}
		]
	}`)
	same, err := TransportWithoutUsers(usersOnly)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cleaned, same) {
		t.Fatalf("users-only change not detected:\n%s\n%s", cleaned, same)
	}
}
