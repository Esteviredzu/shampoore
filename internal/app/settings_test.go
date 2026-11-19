package app

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/logging"
)

func testBuild(context.Context, *Settings, string, logging.ContextLogger, Values) (*BuildResult, error) {
	return &BuildResult{}, nil
}

func init() {
	if err := RegisterDescriptor(Descriptor{
		Kind: "unittest",
		Defaults: Values{
			"num":  1,
			"flag": true,
			"text": "default",
		},
		Build: testBuild,
	}); err != nil {
		panic(err)
	}
	if err := RegisterDescriptor(Descriptor{
		Kind:     "urltest",
		Defaults: Values{"num": 1},
		DecodeURL: func(url.Values) (json.RawMessage, error) {
			return json.RawMessage(`{"key":"value"}`), nil
		},
		Build: testBuild,
	}); err != nil {
		panic(err)
	}
}

func decodeSettings(t *testing.T, raw string) (*Settings, error) {
	t.Helper()
	var s Settings
	err := json.Unmarshal([]byte(raw), &s)
	return &s, err
}

func TestSettingsRejectsUnknownField(t *testing.T) {
	_, err := decodeSettings(t, `{"type":"unittest","encryption_pasword":"typo"}`)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestSettingsRejectsWrongTypes(t *testing.T) {
	cases := []string{
		`{"type":"unittest","name":123}`,
		`{"type":"unittest","encryption_password":123}`,
		`{"type":"unittest","advanced":[]}`,
		`{"type":"unittest","transport":"nope"}`,
		`{"type":"unittest","enabled":"yes"}`,
		`{"type":123}`,
		`{"type":"unittest","name":null}`,
		`{"type":"unittest","encryption_password":null}`,
	}
	for _, raw := range cases {
		if _, err := decodeSettings(t, raw); err == nil {
			t.Fatalf("expected error for %s", raw)
		}
	}
}

func TestSettingsRejectsDuplicateField(t *testing.T) {
	_, err := decodeSettings(t, `{"type":"unittest","encryption_password":"a","encryption_password":"b"}`)
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate field error, got %v", err)
	}
}

func TestSettingsRejectsNonObject(t *testing.T) {
	if _, err := decodeSettings(t, `[]`); err == nil {
		t.Fatal("expected error for JSON array")
	}
	if _, err := decodeSettings(t, `null`); err == nil {
		t.Fatal("expected error for JSON null")
	}
}

func TestSettingsRejectsConflictingTypes(t *testing.T) {
	_, err := decodeSettings(t, `{"type":"unittest","transport_kind":"other"}`)
	if err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("expected conflicting types error, got %v", err)
	}
}

func TestSettingsLegacyTransportBlock(t *testing.T) {
	s, err := decodeSettings(t, `{"type":"unittest","unittest":{"key":"value"}}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Type != "unittest" || !strings.Contains(string(s.Transport), "value") {
		t.Fatalf("unexpected settings: %+v transport=%s", s, s.Transport)
	}
	if _, err := decodeSettings(t, `{"type":"unittest","other":{"key":"value"}}`); err == nil {
		t.Fatal("expected error for mismatched transport block")
	}
}

func TestSettingsValidConfig(t *testing.T) {
	s, err := decodeSettings(t, `{
		"name":"cfg",
		"type":"unittest",
		"transport":{"key":"value"},
		"encryption_password":"secret",
		"advanced":{"num":5,"flag":false,"text":"override"}
	}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != "cfg" || s.Type != "unittest" || s.EncryptionPassword != "secret" {
		t.Fatalf("unexpected settings: %+v", s)
	}
	if s.Advanced.Int("num", 0) != 5 {
		t.Fatalf("advanced.num = %v", s.Advanced["num"])
	}
}

func TestSettingsAdvancedTypeValidation(t *testing.T) {
	if _, err := decodeSettings(t, `{"type":"unittest","advanced":{"num":"abc"}}`); err == nil {
		t.Fatal("expected error for non-numeric advanced value")
	}
	if _, err := decodeSettings(t, `{"type":"unittest","advanced":{"flag":5}}`); err == nil {
		t.Fatal("expected error for non-boolean advanced value")
	}
	if _, err := decodeSettings(t, `{"type":"unittest","advanced":{"text":5}}`); err == nil {
		t.Fatal("expected error for non-string advanced value")
	}
}

func TestApplyDefaultsRequiresType(t *testing.T) {
	if err := (&Settings{}).ApplyDefaults(); err == nil {
		t.Fatal("expected error for missing type")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"transport":{"key":"value"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected Load to reject config without type")
	}
}

func TestParseURLValidatesAdvanced(t *testing.T) {
	s, err := ParseURL("shampoore://?type=urltest&a.num=5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Type != "urltest" || s.Advanced.Int("num", 0) != 5 {
		t.Fatalf("unexpected settings: %+v", s)
	}
	if _, err := ParseURL("shampoore://?type=urltest&a.num=abc"); err == nil {
		t.Fatal("expected error for malformed advanced value")
	}
}
