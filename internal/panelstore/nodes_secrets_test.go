package panelstore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestNodeCustomSecretsPersistAndPartialUpdate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	custom := map[string]string{
		"encryption_password": "manual-encryption",
		"raw_ws_password":     "manual-raw-ws",
		"livekit_api_key":     "manual-key",
		"livekit_api_secret":  "manual-secret",
		"minio_root_user":     "manual-minio",
		"minio_root_password": "manual-minio-pass",
		"s3_bucket":           "manual-bucket",
	}
	relay, err := store.CreateNode(ctx, Node{
		Name:    "relay-custom-secrets",
		SSHHost: "203.0.113.90",
		IsRelay: true,
		Transports: []Transport{
			{Type: TransportRawWS, Domain: "ws.example.com", Enabled: true},
		},
		Secrets: custom,
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	if relay.EncryptionCipher != EncryptionCipherName {
		t.Fatalf("encryption cipher = %q, want %q", relay.EncryptionCipher, EncryptionCipherName)
	}
	for key, want := range custom {
		if got := relay.GeneratedSecrets[key]; got != want {
			t.Fatalf("generated secret %q = %q, want %q", key, got, want)
		}
		if got := relay.Secrets[key]; got != want {
			t.Fatalf("view secret %q = %q, want %q", key, got, want)
		}
	}
	if got := relay.GeneratedSecrets["s3_access_key"]; got != custom["minio_root_user"] {
		t.Fatalf("s3_access_key = %q, want %q", got, custom["minio_root_user"])
	}
	if got := relay.GeneratedSecrets["s3_secret_key"]; got != custom["minio_root_password"] {
		t.Fatalf("s3_secret_key = %q, want %q", got, custom["minio_root_password"])
	}

	got, err := store.GetNode(ctx, relay.ID)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range custom {
		if got.Secrets[key] != want {
			t.Fatalf("persisted secret %q = %q, want %q", key, got.Secrets[key], want)
		}
	}

	update := got
	update.Secrets = map[string]string{"raw_ws_password": "manual-raw-ws-2"}
	updated, err := store.UpdateNode(ctx, update)
	if err != nil {
		t.Fatalf("update node: %v", err)
	}
	if updated.Secrets["raw_ws_password"] != "manual-raw-ws-2" {
		t.Fatalf("raw_ws_password = %q", updated.Secrets["raw_ws_password"])
	}
	if updated.Secrets["encryption_password"] != custom["encryption_password"] {
		t.Fatalf("encryption_password was rotated: %q", updated.Secrets["encryption_password"])
	}
	if updated.GeneratedSecrets["s3_access_key"] != custom["minio_root_user"] {
		t.Fatalf("s3_access_key changed on partial update: %q", updated.GeneratedSecrets["s3_access_key"])
	}

	data, err := json.Marshal(updated)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "generated_secrets") {
		t.Fatalf("node JSON leaked generated_secrets: %s", data)
	}
	if !strings.Contains(string(data), "\"secrets\"") || !strings.Contains(string(data), "\"encryption_cipher\"") {
		t.Fatalf("node JSON missing secrets/cipher: %s", data)
	}
}

func TestNodeRejectsInvalidSecrets(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	cases := []map[string]string{
		{"raw_ws_password": ""},
		{"raw_ws_password": "line\nbreak"},
		{"unknown_secret": "value"},
	}
	for i, secrets := range cases {
		_, err := store.CreateNode(ctx, Node{
			Name:    "relay-bad-secret-" + string(rune('a'+i)),
			SSHHost: "203.0.113.91",
			IsRelay: true,
			Transports: []Transport{
				{Type: TransportRawWS, Domain: "ws.example.com", Enabled: true},
			},
			Secrets: secrets,
		})
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("case %d: error = %v, want invalid", i, err)
		}
	}
}
