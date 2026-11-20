package panelstore

import (
	"context"
	"errors"
	"testing"
)

func TestNormalizeDecoyRepoURL(t *testing.T) {
	valid := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"   ", ""},
		{"https://github.com/Esteviredzu/example_website", "https://github.com/Esteviredzu/example_website"},
		{"https://github.com/owner/repo/", "https://github.com/owner/repo"},
		{"https://www.github.com/owner/repo", "https://github.com/owner/repo"},
		{"https://codeload.github.com/owner/repo/zip/refs/heads/main.zip", "https://codeload.github.com/owner/repo/zip/refs/heads/main.zip"},
	}
	for _, tc := range valid {
		got, err := normalizeDecoyRepoURL(TransportRawWS, tc.in)
		if err != nil {
			t.Fatalf("normalizeDecoyRepoURL(%q) error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("normalizeDecoyRepoURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	invalid := []string{
		"http://github.com/owner/repo",
		"https://gitlab.com/owner/repo",
		"https://github.com/owner",
		"https://github.com/owner/repo/extra",
		"https://github.com/owner/repo?ref=dev",
		"https://github.com/owner/repo#readme",
		"https://user:pass@github.com/owner/repo",
		"https://github.com/ow ner/repo",
		"ftp://github.com/owner/repo",
	}
	for _, raw := range invalid {
		if got, err := normalizeDecoyRepoURL(TransportRawWS, raw); err == nil {
			t.Fatalf("normalizeDecoyRepoURL(%q) = %q, want error", raw, got)
		}
	}

	if _, err := normalizeDecoyRepoURL(TransportSilo, "https://github.com/owner/repo"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("silo decoy url error = %v, want ErrInvalid", err)
	}
	if _, err := normalizeDecoyRepoURL(TransportLiveKit, "https://github.com/owner/repo"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("livekit decoy url error = %v, want ErrInvalid", err)
	}
}

func TestNodeDecoyRepoURLRoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	node, err := store.CreateNode(ctx, Node{
		Name:    "relay-decoy",
		SSHHost: "203.0.113.10",
		IsRelay: true,
		Transports: []Transport{
			{Type: TransportRawWS, Domain: "ws.example.com", DecoyRepoURL: "https://github.com/Esteviredzu/example_website", Enabled: true},
		},
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	if len(node.Transports) != 1 || node.Transports[0].DecoyRepoURL != "https://github.com/Esteviredzu/example_website" {
		t.Fatalf("created transports = %+v", node.Transports)
	}
	got, err := store.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got.Transports[0].DecoyRepoURL != "https://github.com/Esteviredzu/example_website" {
		t.Fatalf("persisted decoy url = %q", got.Transports[0].DecoyRepoURL)
	}

	got.Transports[0].DecoyRepoURL = ""
	updated, err := store.UpdateNode(ctx, got)
	if err != nil {
		t.Fatalf("update node: %v", err)
	}
	if updated.Transports[0].DecoyRepoURL != "" {
		t.Fatalf("updated decoy url = %q, want empty", updated.Transports[0].DecoyRepoURL)
	}
}

func TestNodeRejectsInvalidDecoyRepoURL(t *testing.T) {
	store := newTestStore(t)
	_, err := store.CreateNode(context.Background(), Node{
		Name:    "relay-bad-decoy",
		SSHHost: "203.0.113.10",
		IsRelay: true,
		Transports: []Transport{
			{Type: TransportRawWS, Domain: "ws.example.com", DecoyRepoURL: "http://github.com/owner/repo", Enabled: true},
		},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("create error = %v, want ErrInvalid", err)
	}
}
