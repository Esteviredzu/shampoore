package auth

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func mustRegistry(t *testing.T, specs ...UserSpec) *UserRegistry {
	t.Helper()
	r := NewRegistry()
	for _, spec := range specs {
		if err := r.Upsert(spec); err != nil {
			t.Fatalf("upsert %q: %v", spec.UserID, err)
		}
	}
	return r
}

func TestAuthorizeSuccessAndLegacyModes(t *testing.T) {
	var nilRegistry *UserRegistry
	if err := nilRegistry.Authorize("alice", "exit-1"); err != nil {
		t.Fatalf("nil registry should authorize: %v", err)
	}

	empty := NewRegistry()
	if empty.Active() {
		t.Fatal("empty registry reported itself active")
	}
	if err := empty.Authorize("alice", "exit-1"); err != nil {
		t.Fatalf("empty registry should authorize: %v", err)
	}

	r := mustRegistry(t, UserSpec{
		UserID:       "alice",
		Enabled:      true,
		ExpiresAt:    time.Now().Add(time.Hour),
		AllowedExits: []string{"exit-1"},
	})
	if err := r.Authorize("alice", "exit-1"); err != nil {
		t.Fatalf("authorize: %v", err)
	}
	if err := r.Authorize("alice", "exit-2"); !errors.Is(err, ErrExitNotAllowed) {
		t.Fatalf("expected exit error, got %v", err)
	}
}

func TestAuthorizeDisabledExpiredAndLimit(t *testing.T) {
	disabled := mustRegistry(t, UserSpec{UserID: "bob", Enabled: false})
	if err := disabled.Authorize("bob", ""); !errors.Is(err, ErrUserDisabled) {
		t.Fatalf("expected disabled error, got %v", err)
	}

	expired := mustRegistry(t, UserSpec{UserID: "carol", Enabled: true, ExpiresAt: time.Now().Add(-time.Second)})
	if err := expired.Authorize("carol", ""); !errors.Is(err, ErrUserExpired) {
		t.Fatalf("expected expired error, got %v", err)
	}

	limited := mustRegistry(t, UserSpec{UserID: "dave", Enabled: true, TrafficLimitBytes: 10})
	if err := limited.AddTraffic("dave", 4, 0); err != nil {
		t.Fatalf("add traffic: %v", err)
	}
	if err := limited.Authorize("dave", ""); err != nil {
		t.Fatalf("below limit should authorize: %v", err)
	}
	if err := limited.AddTraffic("dave", 3, 3); !errors.Is(err, ErrTrafficLimitExceeded) {
		t.Fatalf("expected limit error, got %v", err)
	}
	if err := limited.Authorize("dave", ""); !errors.Is(err, ErrTrafficLimitExceeded) {
		t.Fatalf("expected limit error from authorize, got %v", err)
	}
}

func TestAuthorizeWildcardAndEmptyExit(t *testing.T) {
	r := mustRegistry(t, UserSpec{UserID: "eve", Enabled: true, AllowedExits: []string{"*"}})
	if err := r.Authorize("eve", "anything"); err != nil {
		t.Fatalf("wildcard exit denied: %v", err)
	}

	restricted := mustRegistry(t, UserSpec{UserID: "frank", Enabled: true, AllowedExits: []string{"exit-1"}})
	if err := restricted.Authorize("frank", ""); !errors.Is(err, ErrExitNotAllowed) {
		t.Fatalf("empty exit should be denied for restricted user, got %v", err)
	}
}

func TestRegistryNotificationsAndActiveUsers(t *testing.T) {
	r := NewRegistry()
	var changes int
	cancel := r.Subscribe(func() { changes++ })

	if err := r.Upsert(UserSpec{UserID: "a", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(UserSpec{UserID: "b", Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(UserSpec{UserID: "c", Enabled: true, ExpiresAt: time.Now().Add(-time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if changes != 3 {
		t.Fatalf("expected 3 change notifications, got %d", changes)
	}
	active := r.ActiveUsers()
	if len(active) != 1 || active[0].UserID != "a" {
		t.Fatalf("unexpected active users: %+v", active)
	}

	r.Remove("a")
	if changes != 4 {
		t.Fatalf("expected remove notification, got %d", changes)
	}
	cancel()
	r.Remove("b")
	if changes != 4 {
		t.Fatalf("cancelled subscriber was still notified: %d", changes)
	}
}

func TestAddTrafficConcurrent(t *testing.T) {
	r := mustRegistry(t, UserSpec{UserID: "concurrent", Enabled: true})
	const workers = 32
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if err := r.AddTraffic("concurrent", 1, 1); err != nil {
					t.Errorf("add traffic: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	info, ok := r.Get("concurrent")
	if !ok {
		t.Fatal("user missing")
	}
	want := int64(workers * iterations)
	if info.Usage.UploadBytes != want || info.Usage.DownloadBytes != want {
		t.Fatalf("unexpected usage: %+v, want %d/%d", info.Usage, want, want)
	}
}

func TestRegistryReplaceAppliesFullUserList(t *testing.T) {
	r := mustRegistry(t,
		UserSpec{UserID: "alice", Enabled: true, MaxSpeedKBps: 128},
		UserSpec{UserID: "bob", Enabled: true},
	)

	notifications := make(chan struct{}, 8)
	unsub := r.Subscribe(func() { notifications <- struct{}{} })
	defer unsub()

	if err := r.Replace([]UserSpec{
		{UserID: "alice", Enabled: true, MaxSpeedKBps: 2048, AllowedExits: []string{"exit-1"}},
		{UserID: "carol", Enabled: true},
	}); err != nil {
		t.Fatalf("replace: %v", err)
	}

	alice, ok := r.Get("alice")
	if !ok {
		t.Fatal("alice missing after replace")
	}
	if alice.MaxSpeedKBps != 2048 {
		t.Fatalf("alice max speed = %d, want 2048", alice.MaxSpeedKBps)
	}
	if len(alice.AllowedExits) != 1 || alice.AllowedExits[0] != "exit-1" {
		t.Fatalf("alice allowed exits = %v", alice.AllowedExits)
	}
	if _, ok := r.Get("bob"); ok {
		t.Fatal("bob was not removed by replace")
	}
	if _, ok := r.Get("carol"); !ok {
		t.Fatal("carol was not added by replace")
	}

	select {
	case <-notifications:
	case <-time.After(time.Second):
		t.Fatal("replace did not notify subscribers")
	}

	if err := r.Replace([]UserSpec{{UserID: "bad\x00id", Enabled: true}}); err == nil {
		t.Fatal("invalid user id was accepted")
	}
}

func TestNormalizeUserID(t *testing.T) {
	if _, err := NormalizeUserID("   "); !errors.Is(err, ErrUserIDRequired) {
		t.Fatalf("expected empty id error, got %v", err)
	}
	if _, err := NormalizeUserID("bad\x00id"); err == nil {
		t.Fatal("control character was accepted")
	}
	got, err := NormalizeUserID("  550e8400-e29b-41d4-a716-446655440000 ")
	if err != nil {
		t.Fatal(err)
	}
	if got != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("unexpected normalized id: %q", got)
	}
}
