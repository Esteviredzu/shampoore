package geofilter

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestMatchAction(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ru.txt"), []byte("# test rules\n5.5.0.0/16\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "rules.state.json"), []byte(`{"ru": true}`), 0o644); err != nil {
		t.Fatal(err)
	}

	ip := net.ParseIP("5.5.1.1")

	bypass := NewFilter(ActionBypass)
	if err := bypass.LoadDir(dir); err != nil {
		t.Fatal(err)
	}
	action, ok := bypass.MatchAction(ip)
	if !ok || action != ActionBypass {
		t.Fatalf("bypass filter: got action=%q matched=%v, want %q true", action, ok, ActionBypass)
	}
	if !bypass.Match(ip) {
		t.Fatal("bypass filter: Match must report the matched rule")
	}

	block := NewFilter(ActionBlock)
	if err := block.LoadDir(dir); err != nil {
		t.Fatal(err)
	}
	action, ok = block.MatchAction(ip)
	if !ok || action != ActionBlock {
		t.Fatalf("block filter: got action=%q matched=%v, want %q true", action, ok, ActionBlock)
	}

	if _, ok := block.MatchAction(net.ParseIP("9.9.9.9")); ok {
		t.Fatal("unmatched destination reported as matched")
	}
	if _, ok := block.MatchAction(nil); ok {
		t.Fatal("nil destination reported as matched")
	}

	if err := block.SetEnabled("ru", false); err != nil {
		t.Fatal(err)
	}
	if _, ok := block.MatchAction(ip); ok {
		t.Fatal("disabled rule reported as matched")
	}
}
