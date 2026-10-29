package manager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

const StateFileName = ".state.json"

type State struct {
	mu   sync.Mutex
	path string
	data map[string]bool
}

func LoadState(dir string) (*State, error) {
	path := filepath.Join(dir, StateFileName)
	s := &State{path: path, data: map[string]bool{}}
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &s.data)
	}
	if s.data == nil {
		s.data = map[string]bool{}
	}
	return s, nil
}

func (s *State) IsEnabled(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[name]
}

func (s *State) Set(name string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if enabled {
		s.data[name] = true
	} else {
		delete(s.data, name)
	}
	return s.saveLocked()
}

func (s *State) Remove(name string) error {
	return s.Set(name, false)
}

func (s *State) saveLocked() error {
	if len(s.data) == 0 {
		_ = os.Remove(s.path)
		return nil
	}
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0644)
}