package geofilter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

const stateFileName = "rules.state.json"

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

type ruleSnapshot struct {
	rules     []RuleSet
	enabled   map[string]bool
	nets      map[string][]*net.IPNet
	statePath string
	version   uint64
}

type Filter struct {
	action Action
	snap   atomic.Pointer[ruleSnapshot]

	mu      sync.Mutex
	loadMu  sync.Mutex
	saveMu  sync.Mutex
	version uint64
	written uint64
}

func NewFilter(action Action) *Filter {
	f := &Filter{action: action}
	f.snap.Store(&ruleSnapshot{
		enabled: make(map[string]bool),
		nets:    make(map[string][]*net.IPNet),
	})
	return f
}

func (f *Filter) Action() Action {
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

	f.loadMu.Lock()
	defer f.loadMu.Unlock()

	var firstErr error
	rules := make([]RuleSet, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") || e.Name() == stateFileName {
			continue
		}
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext != ".txt" && ext != ".json" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		name := strings.TrimSuffix(e.Name(), ext)

		cidrs, err := loadCIDRs(path, ext)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("load rule %q: %w", e.Name(), err)
			}
			continue
		}

		rules = append(rules, RuleSet{
			Name:    name,
			CIDRs:   cidrs,
			Enabled: false,
			Action:  f.action,
		})
	}

	statePath := filepath.Join(dir, stateFileName)
	diskState, err := readStateFile(statePath)
	if err != nil && firstErr == nil {
		firstErr = err
	}

	known := make(map[string]struct{}, len(rules))
	for _, rs := range rules {
		known[rs.Name] = struct{}{}
	}

	for {
		base := f.snap.Load()
		enabled := mergeEnabled(base, diskState)
		for name := range enabled {
			if _, ok := known[name]; !ok {
				delete(enabled, name)
			}
		}

		nets := buildNets(rules, enabled)
		for i := range rules {
			rules[i].Enabled = enabled[rules[i].Name]
			rules[i].Action = f.action
		}

		snap := &ruleSnapshot{
			rules:     rules,
			enabled:   enabled,
			nets:      nets,
			statePath: statePath,
		}

		f.mu.Lock()
		if f.snap.Load() == base {
			f.version++
			snap.version = f.version
			f.snap.Store(snap)
			f.mu.Unlock()
			return firstErr
		}
		f.mu.Unlock()

	}
}

func loadCIDRs(path, ext string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if ext == ".json" {
		var js struct {
			CIDRs []string `json:"cidrs"`
		}
		if err := json.Unmarshal(data, &js); err != nil {
			return nil, err
		}
		return cleanCIDRs(js.CIDRs), nil
	}
	var out []string
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
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
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return cleanCIDRs(out), nil
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

func readStateFile(path string) (map[string]bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read rules state: %w", err)
	}
	var state map[string]bool
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parse rules state: %w", err)
	}
	return state, nil
}

func mergeEnabled(base *ruleSnapshot, diskState map[string]bool) map[string]bool {
	enabled := make(map[string]bool, len(diskState))
	if base != nil {
		for name, on := range base.enabled {
			enabled[name] = on
		}
	}
	for name, on := range diskState {
		if _, ok := enabled[name]; !ok {
			enabled[name] = on
		}
	}
	return enabled
}

func buildNets(rules []RuleSet, enabled map[string]bool) map[string][]*net.IPNet {
	nets := make(map[string][]*net.IPNet, len(rules))
	for _, rs := range rules {
		if !enabled[rs.Name] {
			continue
		}
		nets[rs.Name] = parseCIDRs(rs.CIDRs)
	}
	return nets
}

func parseCIDRs(cidrs []string) []*net.IPNet {
	var nets []*net.IPNet
	for _, cidr := range cidrs {
		_, n, err := net.ParseCIDR(cidr)
		if err == nil {
			nets = append(nets, n)
		}
	}
	return nets
}

func (f *Filter) Match(ip net.IP) bool {
	_, ok := f.MatchAction(ip)
	return ok
}

func (f *Filter) MatchAction(ip net.IP) (Action, bool) {
	if ip == nil {
		return "", false
	}
	snap := f.snap.Load()
	if snap == nil {
		return "", false
	}
	for _, rs := range snap.rules {
		if !snap.enabled[rs.Name] {
			continue
		}
		for _, n := range snap.nets[rs.Name] {
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
	snap := f.snap.Load()
	if snap == nil {
		return []RuleSet{}
	}
	out := make([]RuleSet, len(snap.rules))
	for i, rs := range snap.rules {
		out[i] = rs
		out[i].CIDRs = append([]string(nil), rs.CIDRs...)
		out[i].Enabled = snap.enabled[rs.Name]
	}
	return out
}

func (f *Filter) SetEnabled(name string, on bool) error {
	f.mu.Lock()
	cur := f.snap.Load()
	if cur == nil {
		f.mu.Unlock()
		return fmt.Errorf("rule %q not found", name)
	}
	idx := -1
	for i := range cur.rules {
		if cur.rules[i].Name == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		f.mu.Unlock()
		return fmt.Errorf("rule %q not found", name)
	}

	enabled := make(map[string]bool, len(cur.enabled)+1)
	for ruleName, ruleOn := range cur.enabled {
		enabled[ruleName] = ruleOn
	}
	enabled[name] = on

	nets := make(map[string][]*net.IPNet, len(cur.nets)+1)
	for ruleName, ruleNets := range cur.nets {
		nets[ruleName] = ruleNets
	}
	if on {
		nets[name] = parseCIDRs(cur.rules[idx].CIDRs)
	} else {
		delete(nets, name)
	}

	rules := make([]RuleSet, len(cur.rules))
	copy(rules, cur.rules)
	rules[idx].Enabled = on

	f.version++
	snap := &ruleSnapshot{
		rules:     rules,
		enabled:   enabled,
		nets:      nets,
		statePath: cur.statePath,
		version:   f.version,
	}
	f.snap.Store(snap)
	f.mu.Unlock()

	return f.saveState(snap)
}

func (f *Filter) EnabledCIDRs() []string {
	snap := f.snap.Load()
	if snap == nil {
		return nil
	}
	var out []string
	for _, rs := range snap.rules {
		if snap.enabled[rs.Name] {
			out = append(out, rs.CIDRs...)
		}
	}
	return out
}

func (f *Filter) EnableAll() {
	f.mu.Lock()
	cur := f.snap.Load()
	if cur == nil {
		f.mu.Unlock()
		return
	}

	enabled := make(map[string]bool, len(cur.rules))
	rules := make([]RuleSet, len(cur.rules))
	nets := make(map[string][]*net.IPNet, len(cur.rules))
	for i, rs := range cur.rules {
		enabled[rs.Name] = true
		rules[i] = rs
		rules[i].Enabled = true
		nets[rs.Name] = parseCIDRs(rs.CIDRs)
	}

	f.version++
	snap := &ruleSnapshot{
		rules:     rules,
		enabled:   enabled,
		nets:      nets,
		statePath: cur.statePath,
		version:   f.version,
	}
	f.snap.Store(snap)
	f.mu.Unlock()
}

func (f *Filter) saveState(snap *ruleSnapshot) error {
	if snap == nil || snap.statePath == "" {
		return nil
	}
	state := make(map[string]bool, len(snap.rules))
	for _, rs := range snap.rules {
		state[rs.Name] = snap.enabled[rs.Name]
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal rules state: %w", err)
	}

	f.saveMu.Lock()
	defer f.saveMu.Unlock()
	if snap.version < f.written {
		return nil
	}
	tmp, err := os.CreateTemp(filepath.Dir(snap.statePath), ".rules-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create rules state temp: %w", err)
	}
	tmpName := tmp.Name()
	if err := tmp.Chmod(0644); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("chmod rules state temp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write rules state temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close rules state temp: %w", err)
	}
	if err := os.Rename(tmpName, snap.statePath); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("replace rules state: %w", err)
	}
	f.written = snap.version
	return nil
}
