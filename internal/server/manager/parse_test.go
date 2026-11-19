package manager

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"

	_ "github.com/Esteviredzu/shampoore/internal/multipipe"
	_ "github.com/Esteviredzu/shampoore/internal/transport/livekit_datachannel"
	_ "github.com/Esteviredzu/shampoore/internal/transport/raw_ws"
	_ "github.com/Esteviredzu/shampoore/internal/transport/silo"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_raw"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_squares"
)

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind: "reqtest",
		Fields: []app.Field{
			{Key: "token", Type: app.FieldTypeText, Required: true},
		},
		Build: func(context.Context, *app.Settings, string, logging.ContextLogger, app.Values) (*app.BuildResult, error) {
			return &app.BuildResult{}, nil
		},
	})
	if err != nil {
		panic(err)
	}
}

func TestParseConfigContentRejectsTypo(t *testing.T) {
	cases := []string{
		`{"type":"yandex","encryption_pasword":"typo","transport":{"hash":"cc39e6052ac81e6f43be184baaf33fcb"}}`,
		`{"settings":{"type":"yandex","encryption_pasword":"typo"}}`,
	}
	for _, raw := range cases {
		if _, err := ParseConfigContent(raw); err == nil {
			t.Fatalf("expected error for %s", raw)
		}
	}
}

func TestParseConfigContentWrapper(t *testing.T) {
	s, err := ParseConfigContent(`{"name":"outer","settings":{"type":"yandex","transport":{"hash":"cc39e6052ac81e6f43be184baaf33fcb"}}}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != "outer" {
		t.Fatalf("name = %q, want outer", s.Name)
	}

	if _, err := ParseConfigContent(`{"name":123,"settings":{"type":"yandex"}}`); err == nil {
		t.Fatal("expected error for non-string top-level name")
	}
	if _, err := ParseConfigContent(`{"settings":null}`); err == nil {
		t.Fatal("expected error for null settings")
	}
}

func TestValidateSettings(t *testing.T) {
	const yandexHash = "cc39e6052ac81e6f43be184baaf33fcb"
	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"yandex valid", `{"type":"yandex","transport":{"hash":"` + yandexHash + `"}}`, false},
		{"yandex missing hash", `{"type":"yandex","transport":{}}`, true},
		{"yandex bad hash", `{"type":"yandex","transport":{"hash":"short"}}`, true},
		{"silo valid", `{"type":"silo","transport":{"endpoint":"s3.example","bucket":"b","access_key":"a","secret_key":"s"}}`, false},
		{"silo missing secret", `{"type":"silo","transport":{"endpoint":"s3.example","bucket":"b","access_key":"a"}}`, true},
		{"raw_ws valid", `{"type":"raw_ws","transport":{"server_url":"wss://example","room":"r","password":"p"}}`, false},
		{"raw_ws missing password", `{"type":"raw_ws","transport":{"server_url":"wss://example","room":"r"}}`, true},
		{"livekit valid", `{"type":"livekit_datachannel","transport":{"signal_url":"wss://example","api_key":"k","api_secret":"s","room":"r"}}`, false},
		{"livekit missing room", `{"type":"livekit_datachannel","transport":{"signal_url":"wss://example","api_key":"k","api_secret":"s"}}`, true},
		{"livekit multiuser valid", `{"type":"livekit_datachannel","transport":{"signal_url":"wss://example","api_key":"k","api_secret":"s","multiuser":true,"token_listen":"127.0.0.1:8099","room_prefix":"shp"}}`, false},
		{"livekit multiuser missing listen", `{"type":"livekit_datachannel","transport":{"signal_url":"wss://example","api_key":"k","api_secret":"s","multiuser":true}}`, true},
		{"livekit on-demand client valid", `{"type":"livekit_datachannel","transport":{"signal_url":"wss://example","user_id":"alice","token_url":"https://control.example/session"}}`, false},
		{"telemost raw valid", `{"type":"yandex_telemost_raw","transport":{"conference_id":"41514947350946"}}`, false},
		{"telemost raw empty", `{"type":"yandex_telemost_raw","transport":{}}`, true},
		{"telemost squares valid", `{"type":"yandex_telemost_squares","transport":{"conference_link":"https://telemost.yandex.ru/j/41514947350946"}}`, false},
		{"telemost squares empty", `{"type":"yandex_telemost_squares"}`, true},
		{"multi valid", `{"type":"multi","transport":{"strategy":"bond","transports":[{"type":"yandex","transport":{"hash":"` + yandexHash + `"}}]}}`, false},
		{"multi empty", `{"type":"multi","transport":{"strategy":"flowmux","transports":[]}}`, true},
		{"multi unknown", `{"type":"multi","transport":{"transports":[{"type":"nope","transport":{}}]}}`, true},
		{"multi bad strategy", `{"type":"multi","transport":{"strategy":"roundrobin","transports":[{"type":"yandex","transport":{"hash":"` + yandexHash + `"}}]}}`, true},
		{"required fields missing", `{"type":"reqtest","transport":{"other":"x"}}`, true},
		{"required fields valid", `{"type":"reqtest","transport":{"token":"x"}}`, false},
		{"unknown type", `{"type":"does-not-exist"}`, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := ParseConfigContent(tc.raw)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			err = ValidateSettings(s)
			if tc.wantErr && err == nil {
				t.Fatal("expected validation error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestWriteSettingsFilePermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}

	s := &app.Settings{Type: "yandex", Advanced: app.NewValues()}
	if err := writeSettingsFile(path, s); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("permissions = %o, want 600", perm)
	}
}

func TestStatePermissionsAndCorruption(t *testing.T) {
	dir := t.TempDir()
	st, err := LoadState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Set("cfg", true); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, StateFileName))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("state permissions = %o, want 600", perm)
	}

	st, err = LoadState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !st.IsEnabled("cfg") {
		t.Fatal("state did not persist enabled flag")
	}

	if err := os.WriteFile(filepath.Join(dir, StateFileName), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadState(dir); err == nil || !strings.Contains(err.Error(), "parse") {
		t.Fatalf("expected state parse error, got %v", err)
	}
}

func TestAddRejectsStrictConfigErrors(t *testing.T) {
	m := New(t.TempDir(), logging.New("test", false), nil, false)
	err := m.Add("cfg", "", `{"type":"yandex","transport":{}}`)
	if err == nil {
		t.Fatal("expected validation error")
	}
	err = m.Add("cfg", "", `{"type":"yandex","encryption_pasword":"typo","transport":{"hash":"cc39e6052ac81e6f43be184baaf33fcb"}}`)
	if err == nil {
		t.Fatal("expected strict parse error")
	}
}
