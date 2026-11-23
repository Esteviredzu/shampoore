package architecture

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

const modulePath = "github.com/Esteviredzu/shampoore"

func TestCorePackagesDoNotImportConcreteTransports(t *testing.T) {
	packages := []string{
		modulePath + "/panel",
		modulePath + "/internal/provisioner",
		modulePath + "/internal/multipipe",
		modulePath + "/internal/server/manager",
		modulePath + "/internal/server/logbuf",
		modulePath + "/internal/core/...",
		modulePath + "/internal/app",
	}
	transportPrefix := modulePath + "/internal/transport/"
	for _, pkg := range packages {
		out, err := exec.Command("go", "list", "-deps", pkg).Output()
		if err != nil {
			t.Fatalf("go list -deps %s: %v", pkg, err)
		}
		for _, dep := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if !strings.HasPrefix(dep, transportPrefix) {
				continue
			}
			if dep == modulePath+"/internal/transport" {
				continue
			}
			t.Errorf("%s must not import concrete transport %s", pkg, dep)
		}
	}
}

func TestCompositionRootCoversEveryTransport(t *testing.T) {
	entries, err := os.ReadDir("../transport")
	if err != nil {
		t.Fatal(err)
	}
	allSource, err := os.ReadFile("../transport/all/all.go")
	if err != nil {
		t.Fatal(err)
	}
	all := string(allSource)
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || name == "all" {
			continue
		}
		if _, err := os.Stat("../transport/" + name + "/register.go"); err != nil {
			continue
		}
		importPath := modulePath + "/internal/transport/" + name
		if !strings.Contains(all, importPath) {
			t.Errorf("internal/transport/all does not import %s", importPath)
		}
	}
}
