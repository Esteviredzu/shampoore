package geofilter

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Action string

const (
	ActionBypass Action = "bypass"
	ActionBlock  Action = "block"
)

type RuleSet struct {
	Name    string   `json:"name"`
	CIDRs   []string `json:"cidrs"`
	Enabled bool     `json:"enabled"`
	Action  Action   `json:"action"`
}

type Filter struct {
	mu        sync.RWMutex
	rules     []RuleSet
	enabled   map[string]bool
	action    Action
	nets      map[string][]*net.IPNet
	statePath string
}

func NewFilter(action Action) *Filter {
	return &Filter{
		action:  action,
		enabled: make(map[string]bool),
		nets:    make(map[string][]*net.IPNet),
	}
}

func (f *Filter) Action() Action {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.action
}

func (f *Filter) LoadDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.rules = nil
	f.nets = make(map[string][]*net.IPNet)

	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if e.Name() == "rules.state.json" {
			continue
		}
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext != ".txt" && ext != ".json" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		name := strings.TrimSuffix(e.Name(), ext)

		cidrs, ok := loadCIDRs(path, ext)
		if !ok {
			continue
		}

		f.rules = append(f.rules, RuleSet{
			Name:    name,
			CIDRs:   cidrs,
			Enabled: false,
			Action:  f.action,
		})
	}

	f.loadState(dir)
	for i := range f.rules {
		if f.enabled[f.rules[i].Name] {
			f.parseNets(&f.rules[i])
		}
	}
	return nil
}

func loadCIDRs(path, ext string) ([]string, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	if ext == ".json" {
		var js struct {
			CIDRs []string `json:"cidrs"`
		}
		if err := json.Unmarshal(data, &js); err != nil {
			return nil, false
		}
		return cleanCIDRs(js.CIDRs), true
	}
	var out []string
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line != "" {
			out = append(out, line)
		}
	}
	return cleanCIDRs(out), true
}

func cleanCIDRs(in []string) []string {
	out := make([]string, 0, len(in))
	for _, c := range in {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		out = append(out, c)
	}
	return out
}

func (f *Filter) parseNets(rs *RuleSet) {
	var nets []*net.IPNet
	for _, cidr := range rs.CIDRs {
		_, n, err := net.ParseCIDR(cidr)
		if err == nil {
			nets = append(nets, n)
		}
	}
	f.nets[rs.Name] = nets
}

func (f *Filter) Match(ip net.IP) bool {
	_, ok := f.MatchAction(ip)
	return ok
}

func (f *Filter) MatchAction(ip net.IP) (Action, bool) {
	if ip == nil {
		return "", false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, rs := range f.rules {
		if !f.enabled[rs.Name] {
			continue
		}
		for _, n := range f.nets[rs.Name] {
			if n.Contains(ip) {
				action := rs.Action
				if action == "" {
					action = f.action
				}
				return action, true
			}
		}
	}
	return "", false
}

func (f *Filter) Rules() []RuleSet {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]RuleSet, len(f.rules))
	copy(out, f.rules)
	for i := range out {
		out[i].Enabled = f.enabled[out[i].Name]
	}
	return out
}

func (f *Filter) SetEnabled(name string, on bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	found := false
	for i := range f.rules {
		if f.rules[i].Name == name {
			found = true
			f.enabled[name] = on
			if on {
				f.parseNets(&f.rules[i])
			} else {
				delete(f.nets, name)
			}
			break
		}
	}
	if !found {
		return fmt.Errorf("rule %q not found", name)
	}
	f.saveState()
	return nil
}

func (f *Filter) EnabledCIDRs() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []string
	for _, rs := range f.rules {
		if f.enabled[rs.Name] {
			out = append(out, rs.CIDRs...)
		}
	}
	return out
}

func (f *Filter) EnableAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.rules {
		name := f.rules[i].Name
		f.enabled[name] = true
		f.parseNets(&f.rules[i])
	}
}

func (f *Filter) loadState(dir string) {
	path := filepath.Join(dir, "rules.state.json")
	f.statePath = path
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var state map[string]bool
	if json.Unmarshal(data, &state) == nil {
		for k, v := range state {
			f.enabled[k] = v
		}
	}
}

func (f *Filter) saveState() {
	if f.statePath == "" {
		return
	}
	state := make(map[string]bool, len(f.enabled))
	for k, v := range f.enabled {
		state[k] = v
	}
	data, _ := json.MarshalIndent(state, "", "  ")
	_ = os.WriteFile(f.statePath, data, 0644)
}
