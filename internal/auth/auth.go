package auth

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/google/uuid"
)

var (
	ErrUserIDRequired       = errors.New("auth: user id is required")
	ErrUserNotFound         = errors.New("auth: user not found")
	ErrUserDisabled         = errors.New("auth: user is disabled")
	ErrUserExpired          = errors.New("auth: user has expired")
	ErrTrafficLimitExceeded = errors.New("auth: traffic limit exceeded")
	ErrExitNotAllowed       = errors.New("auth: exit node is not allowed")
)

type UserSpec struct {
	UserID            string    `json:"user_id"`
	Enabled           bool      `json:"enabled"`
	ExpiresAt         time.Time `json:"expires_at,omitempty"`
	TrafficLimitBytes int64     `json:"traffic_limit_bytes,omitempty"`

	MaxSpeedKBps int64    `json:"max_speed_kbps,omitempty"`
	AllowedExits []string `json:"allowed_exits,omitempty"`
}

type Usage struct {
	UploadBytes   int64 `json:"upload_bytes"`
	DownloadBytes int64 `json:"download_bytes"`
}

type UserInfo struct {
	UserSpec
	Usage Usage `json:"usage"`
}

type userState struct {
	spec     UserSpec
	upload   atomic.Int64
	download atomic.Int64
}

func (u *userState) usage() Usage {
	return Usage{UploadBytes: u.upload.Load(), DownloadBytes: u.download.Load()}
}

func (u *userState) used() int64 {
	return u.upload.Load() + u.download.Load()
}

func (u *userState) overLimit() bool {
	return u.spec.TrafficLimitBytes > 0 && u.used() >= u.spec.TrafficLimitBytes
}

type UserRegistry struct {
	mu        sync.RWMutex
	users     map[string]*userState
	listeners map[uint64]func()
	nextSub   uint64
}

func NewRegistry() *UserRegistry {
	return &UserRegistry{
		users:     make(map[string]*userState),
		listeners: make(map[uint64]func()),
	}
}

func NormalizeUserID(userID string) (string, error) {
	id := strings.TrimSpace(userID)
	if id == "" {
		return "", ErrUserIDRequired
	}
	if len(id) > 128 {
		return "", fmt.Errorf("auth: user id is too long (%d > 128)", len(id))
	}
	for _, r := range id {
		if unicode.IsControl(r) {
			return "", errors.New("auth: user id contains control characters")
		}
	}
	if parsed, err := uuid.Parse(id); err == nil {
		return parsed.String(), nil
	}
	return id, nil
}

func (r *UserRegistry) Upsert(spec UserSpec) error {
	if r == nil {
		return nil
	}
	id, err := NormalizeUserID(spec.UserID)
	if err != nil {
		return err
	}
	spec.UserID = id
	spec.AllowedExits = append([]string(nil), spec.AllowedExits...)

	r.mu.Lock()
	state, ok := r.users[id]
	if ok {
		state.spec = spec
	} else {
		r.users[id] = &userState{spec: spec}
	}
	r.mu.Unlock()

	r.notify()
	return nil
}

func (r *UserRegistry) Replace(specs []UserSpec) error {
	if r == nil {
		return nil
	}
	next := make(map[string]UserSpec, len(specs))
	for _, spec := range specs {
		id, err := NormalizeUserID(spec.UserID)
		if err != nil {
			return err
		}
		spec.UserID = id
		spec.AllowedExits = append([]string(nil), spec.AllowedExits...)
		next[id] = spec
	}

	r.mu.Lock()
	for id, spec := range next {
		if state, ok := r.users[id]; ok {
			state.spec = spec
			continue
		}
		r.users[id] = &userState{spec: spec}
	}
	for id := range r.users {
		if _, ok := next[id]; !ok {
			delete(r.users, id)
		}
	}
	r.mu.Unlock()

	r.notify()
	return nil
}

func (r *UserRegistry) Remove(userID string) {
	if r == nil {
		return
	}
	id, err := NormalizeUserID(userID)
	if err != nil {
		return
	}
	r.mu.Lock()
	_, ok := r.users[id]
	delete(r.users, id)
	r.mu.Unlock()
	if ok {
		r.notify()
	}
}

func (r *UserRegistry) Get(userID string) (UserInfo, bool) {
	if r == nil {
		return UserInfo{}, false
	}
	id, err := NormalizeUserID(userID)
	if err != nil {
		return UserInfo{}, false
	}
	r.mu.RLock()
	state, ok := r.users[id]
	if !ok {
		r.mu.RUnlock()
		return UserInfo{}, false
	}
	info := UserInfo{UserSpec: cloneSpec(state.spec), Usage: state.usage()}
	r.mu.RUnlock()
	return info, true
}

func (r *UserRegistry) Snapshot() []UserInfo {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	out := make([]UserInfo, 0, len(r.users))
	for _, state := range r.users {
		out = append(out, UserInfo{UserSpec: cloneSpec(state.spec), Usage: state.usage()})
	}
	r.mu.RUnlock()
	return out
}

func (r *UserRegistry) Active() bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	active := len(r.users) > 0
	r.mu.RUnlock()
	return active
}

func (r *UserRegistry) ActiveUsers() []UserSpec {
	if r == nil {
		return nil
	}
	now := time.Now()
	r.mu.RLock()
	out := make([]UserSpec, 0, len(r.users))
	for _, state := range r.users {
		if !state.spec.Enabled {
			continue
		}
		if !state.spec.ExpiresAt.IsZero() && !now.Before(state.spec.ExpiresAt) {
			continue
		}
		if state.overLimit() {
			continue
		}
		out = append(out, cloneSpec(state.spec))
	}
	r.mu.RUnlock()
	return out
}

func (r *UserRegistry) Subscribe(fn func()) func() {
	if r == nil || fn == nil {
		return func() {}
	}
	r.mu.Lock()
	r.nextSub++
	id := r.nextSub
	r.listeners[id] = fn
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.listeners, id)
		r.mu.Unlock()
	}
}

func (r *UserRegistry) notify() {
	if r == nil {
		return
	}
	r.mu.RLock()
	listeners := make([]func(), 0, len(r.listeners))
	for _, fn := range r.listeners {
		listeners = append(listeners, fn)
	}
	r.mu.RUnlock()
	for _, fn := range listeners {
		fn()
	}
}

func (r *UserRegistry) Authorize(userID, exitNodeID string) error {
	if r == nil {
		return nil
	}
	id, err := NormalizeUserID(userID)
	if err != nil {
		return err
	}
	exit := strings.TrimSpace(exitNodeID)

	r.mu.RLock()
	state, ok := r.users[id]
	if !ok && len(r.users) == 0 {
		r.mu.RUnlock()
		return nil
	}
	if !ok {
		r.mu.RUnlock()
		return fmt.Errorf("%w: %s", ErrUserNotFound, id)
	}
	spec := cloneSpec(state.spec)
	used := state.used()
	r.mu.RUnlock()

	if !spec.Enabled {
		return fmt.Errorf("%w: %s", ErrUserDisabled, id)
	}
	if !spec.ExpiresAt.IsZero() && !time.Now().Before(spec.ExpiresAt) {
		return fmt.Errorf("%w: %s", ErrUserExpired, id)
	}
	if spec.TrafficLimitBytes > 0 && used >= spec.TrafficLimitBytes {
		return fmt.Errorf("%w: %s", ErrTrafficLimitExceeded, id)
	}
	if !exitAllowed(spec.AllowedExits, exit) {
		return fmt.Errorf("%w: %s -> %q", ErrExitNotAllowed, id, exit)
	}
	return nil
}

func exitAllowed(allowed []string, exit string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, item := range allowed {
		if item == "*" {
			return true
		}
		if item == exit && exit != "" {
			return true
		}
	}
	return false
}

func (r *UserRegistry) AddTraffic(userID string, rx, tx int64) error {
	if r == nil {
		return nil
	}
	if rx < 0 {
		rx = 0
	}
	if tx < 0 {
		tx = 0
	}
	id, err := NormalizeUserID(userID)
	if err != nil {
		return err
	}

	r.mu.RLock()
	state, ok := r.users[id]
	limit := int64(0)
	if ok {
		limit = state.spec.TrafficLimitBytes
	}
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrUserNotFound, id)
	}

	if rx > 0 {
		state.upload.Add(rx)
	}
	if tx > 0 {
		state.download.Add(tx)
	}
	if limit > 0 && state.used() >= limit {
		r.notify()
		return fmt.Errorf("%w: %s", ErrTrafficLimitExceeded, id)
	}
	return nil
}

func cloneSpec(spec UserSpec) UserSpec {
	spec.AllowedExits = append([]string(nil), spec.AllowedExits...)
	return spec
}
