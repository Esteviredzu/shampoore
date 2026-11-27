package panelstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUserMaxSpeedRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	user, err := s.CreateUser(ctx, User{Name: "Speed", Enabled: true, MaxSpeedKBps: 512})
	if err != nil {
		t.Fatal(err)
	}
	if user.MaxSpeedKBps != 512 {
		t.Fatalf("created max_speed_kbps = %d, want 512", user.MaxSpeedKBps)
	}
	got, err := s.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxSpeedKBps != 512 {
		t.Fatalf("stored max_speed_kbps = %d, want 512", got.MaxSpeedKBps)
	}

	got.MaxSpeedKBps = 0
	updated, err := s.UpdateUser(ctx, got)
	if err != nil {
		t.Fatal(err)
	}
	if updated.MaxSpeedKBps != 0 {
		t.Fatalf("update to unlimited = %d, want 0", updated.MaxSpeedKBps)
	}
	reloaded, _ := s.GetUser(ctx, user.ID)
	if reloaded.MaxSpeedKBps != 0 {
		t.Fatalf("reloaded max_speed_kbps = %d, want 0", reloaded.MaxSpeedKBps)
	}

	if _, err := s.CreateUser(ctx, User{Name: "Bad", Enabled: true, MaxSpeedKBps: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative speed error = %v, want invalid", err)
	}
}

func TestMarkUsersSeenUpdatesOnlineWindow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	user, err := s.CreateUser(ctx, User{Name: "Online", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if user.Online(time.Now()) {
		t.Fatal("fresh user must not be online")
	}

	seen := time.Now().UTC()
	if err := s.MarkUsersSeen(ctx, 42, []string{user.ID, user.ID, "missing-user", "   "}, seen); err != nil {
		t.Fatalf("mark users seen: %v", err)
	}
	got, err := s.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Online(seen) {
		t.Fatalf("user must be online right after activity: %+v", got)
	}
	if got.LastSeenNode != 42 {
		t.Fatalf("last_seen_node = %d, want 42", got.LastSeenNode)
	}
	if got.Online(seen.Add(UserOnlineWindow + time.Second)) {
		t.Fatal("user stayed online beyond the window")
	}

	got.Name = "Online Renamed"
	if _, err := s.UpdateUser(ctx, got); err != nil {
		t.Fatal(err)
	}
	reloaded, _ := s.GetUser(ctx, user.ID)
	if reloaded.LastSeenAt.IsZero() || !reloaded.Online(time.Now()) {
		t.Fatalf("update cleared last_seen_at: %+v", reloaded)
	}

	if err := s.MarkUsersSeen(ctx, 42, nil, time.Now()); err != nil {
		t.Fatalf("empty mark: %v", err)
	}
}
