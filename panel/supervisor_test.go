package panel

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSupervisorStartIsNonBlocking(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-server")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	sup := NewSupervisor(filepath.Join(dir, "control.sock"), dir, dir)
	sup.binary = script

	begin := time.Now()
	if err := sup.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Fatalf("Start blocked for %v", elapsed)
	}

	second := time.Now()
	if err := sup.Start(); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if elapsed := time.Since(second); elapsed > time.Second {
		t.Fatalf("second Start blocked for %v", elapsed)
	}

	if err := sup.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "control.sock")); !os.IsNotExist(err) {
		t.Fatalf("socket still present after Stop: %v", err)
	}
}
