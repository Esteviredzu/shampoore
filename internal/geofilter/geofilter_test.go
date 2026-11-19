package geofilter

import (
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

func TestDisabledRuleStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "lan.txt"), []byte("10.0.0.0/8\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "rules.state.json"),
		[]byte("{\n  \"lan\": false\n}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ip := net.ParseIP("10.1.2.3")

	for attempt := 0; attempt < 2; attempt++ {
		f := NewFilter(ActionBypass)
		if err := f.LoadDir(dir); err != nil {
			t.Fatalf("load attempt %d: %v", attempt, err)
		}
		rules := f.Rules()
		if len(rules) != 1 {
			t.Fatalf("attempt %d: got %d rules, want 1", attempt, len(rules))
		}
		if rules[0].Enabled {
			t.Fatalf("attempt %d: disabled rule was auto-enabled on startup", attempt)
		}
		if _, ok := f.MatchAction(ip); ok {
			t.Fatalf("attempt %d: disabled rule matched traffic", attempt)
		}
	}

	first := NewFilter(ActionBlock)
	if err := first.LoadDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := first.SetEnabled("lan", true); err != nil {
		t.Fatalf("enable rule: %v", err)
	}
	if action, ok := first.MatchAction(ip); !ok || action != ActionBlock {
		t.Fatalf("enabled rule did not match: action=%q ok=%v", action, ok)
	}

	afterEnable := NewFilter(ActionBlock)
	if err := afterEnable.LoadDir(dir); err != nil {
		t.Fatal(err)
	}
	if !afterEnable.Match(ip) {
		t.Fatal("enabled state was not persisted to rules.state.json")
	}

	if err := first.SetEnabled("lan", false); err != nil {
		t.Fatalf("disable rule: %v", err)
	}
	afterDisable := NewFilter(ActionBlock)
	if err := afterDisable.LoadDir(dir); err != nil {
		t.Fatal(err)
	}
	if afterDisable.Match(ip) {
		t.Fatal("disabled state was not persisted to rules.state.json")
	}
}

func TestConcurrentMatchWhileReload(t *testing.T) {
	dir := t.TempDir()
	rulePath := filepath.Join(dir, "ru.txt")
	if err := os.WriteFile(rulePath, []byte("5.5.0.0/16\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "rules.state.json"), []byte(`{"ru": true}`), 0o644); err != nil {
		t.Fatal(err)
	}

	f := NewFilter(ActionBypass)
	if err := f.LoadDir(dir); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ips := []net.IP{net.ParseIP("5.5.1.1"), net.ParseIP("6.6.1.1"), nil}
			for !stop.Load() {
				for _, ip := range ips {
					if action, ok := f.MatchAction(ip); ok && action != ActionBypass {
						t.Errorf("unexpected action %q for %v", action, ip)
						return
					}
				}
				_ = f.Match(net.ParseIP("5.5.1.1"))
				_ = f.Rules()
				_ = f.EnabledCIDRs()
			}
		}()
	}

	toggleDone := make(chan struct{})
	go func() {
		defer close(toggleDone)
		for i := 0; i < 20; i++ {
			if err := f.SetEnabled("ru", i%2 == 0); err != nil {
				t.Errorf("set enabled: %v", err)
				return
			}
		}
	}()

	for i := 0; i < 30; i++ {
		cidr := "5.5.0.0/16"
		if i%2 == 0 {
			cidr = "6.6.0.0/16"
		}
		if err := os.WriteFile(rulePath, []byte(cidr+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := f.LoadDir(dir); err != nil {
			t.Fatalf("reload %d: %v", i, err)
		}
	}

	<-toggleDone
	stop.Store(true)
	wg.Wait()
}
