package provisioner

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/geofilter"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

func TestExitRuleFilesShipEnabledBlockRules(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"active.txt":   "1.1.1.0/24\n",
		"disabled.txt": "4.4.4.0/24\n",
		"rules.state.json": `{
  "active": true,
  "disabled": false
}
`,
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	p := New(&fakeStore{}, Options{RulesDir: dir})
	out, err := p.exitRuleFiles(context.Background(), panelstore.Node{ID: 2, Name: "exit", IsExit: true})
	if err != nil {
		t.Fatalf("exit rule files: %v", err)
	}
	archive := fileMap(out)
	if _, ok := archive["rules/disabled.txt"]; ok {
		t.Fatal("disabled rule was shipped to the exit node")
	}
	stateFile, ok := archive["rules/rules.state.json"]
	if !ok {
		t.Fatal("rules.state.json is missing from the archive")
	}
	var state map[string]bool
	if err := json.Unmarshal(stateFile.Data, &state); err != nil {
		t.Fatalf("parse shipped rules state: %v", err)
	}
	if len(state) != 1 || !state["active"] {
		t.Fatalf("shipped rules state = %+v, want only active=true", state)
	}

	rulesDir := t.TempDir()
	for name, file := range archive {
		path := filepath.Join(rulesDir, filepath.Base(name))
		if err := os.WriteFile(path, file.Data, 0o600); err != nil {
			t.Fatalf("write shipped file %s: %v", name, err)
		}
	}
	filter := geofilter.NewFilter(geofilter.ActionBlock)
	if err := filter.LoadDir(rulesDir); err != nil {
		t.Fatalf("load shipped rules: %v", err)
	}
	action, matched := filter.MatchAction(net.ParseIP("1.1.1.1"))
	if !matched || action != geofilter.ActionBlock {
		t.Fatalf("shipped rule match = action %q matched %v, want block true", action, matched)
	}
	rules := filter.Rules()
	if len(rules) != 1 || !rules[0].Enabled || rules[0].Action != geofilter.ActionBlock {
		t.Fatalf("loaded rules = %+v, want one enabled block rule", rules)
	}
}
