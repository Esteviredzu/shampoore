package panelstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mkRelay(t *testing.T, s *Store, name, domain string) Node {
	t.Helper()
	n, err := s.CreateNode(context.Background(), Node{
		Name:    name,
		SSHHost: "203.0.113.10",
		IsRelay: true,
		Transports: []Transport{
			{Type: TransportRawWS, Domain: domain, Enabled: true},
			{Type: TransportSilo, Domain: "s3." + domain, Enabled: true},
			{Type: TransportLiveKit, Domain: "lk." + domain, Enabled: true},
		},
	})
	if err != nil {
		t.Fatalf("create relay: %v", err)
	}
	return n
}

func mkExit(t *testing.T, s *Store, name string) Node {
	t.Helper()
	n, err := s.CreateNode(context.Background(), Node{
		Name:    name,
		SSHHost: "198.51.100.20",
		IsExit:  true,
	})
	if err != nil {
		t.Fatalf("create exit: %v", err)
	}
	return n
}

func TestStorePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/panel.db"
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	node := mkRelay(t, s, "persisted", "ws.example.com")
	user, err := s.CreateUser(ctx, User{Name: "persisted-user", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.GetNode(ctx, node.ID)
	if err != nil || got.Name != node.Name || got.NodeSecret != node.NodeSecret {
		t.Fatalf("persisted node = %+v, %v", got, err)
	}
	gotUser, err := s2.FindUserBySubToken(ctx, user.SubToken)
	if err != nil || gotUser.ID != user.ID {
		t.Fatalf("persisted user = %+v, %v", gotUser, err)
	}
}

func TestCreateNodeRejectsDuplicateTransportTypes(t *testing.T) {
	store := newTestStore(t)
	_, err := store.CreateNode(context.Background(), Node{
		Name:    "duplicate",
		SSHHost: "203.0.113.55",
		IsRelay: true,
		Transports: []Transport{
			{Type: TransportRawWS, Domain: "one.example.com", Enabled: true},
			{Type: TransportRawWS, Domain: "two.example.com", Enabled: true},
		},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate transport error = %v, want invalid", err)
	}
}

func TestNodeCRUDAndSecrets(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, s, "relay-de", "ws.example.com")
	if relay.ID == 0 || relay.NodeSecret == "" {
		t.Fatalf("node not initialized: %+v", relay)
	}
	for _, key := range []string{"encryption_password", "raw_ws_password", "livekit_api_key", "livekit_api_secret", "minio_root_password", "s3_access_key", "s3_secret_key", "s3_bucket"} {
		if relay.GeneratedSecrets[key] == "" {
			t.Fatalf("missing generated secret %q", key)
		}
	}
	got, err := s.GetNode(ctx, relay.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != relay.Name || len(got.Transports) != 3 {
		t.Fatalf("unexpected node: %+v", got)
	}
	found, err := s.FindNodeBySecret(ctx, relay.NodeSecret)
	if err != nil || found.ID != relay.ID {
		t.Fatalf("find by secret: %+v %v", found, err)
	}

	got.Name = "relay-de-2"
	got.Transports[0].Domain = "ws2.example.com"
	updated, err := s.UpdateNode(ctx, got)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Name != "relay-de-2" || updated.GeneratedSecrets["raw_ws_password"] != relay.GeneratedSecrets["raw_ws_password"] {
		t.Fatalf("update changed secrets or name: %+v", updated)
	}

	if _, err := s.CreateNode(ctx, relay); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate node error = %v, want conflict", err)
	}
	if err := s.DeleteNode(ctx, relay.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetNode(ctx, relay.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted node error = %v, want not found", err)
	}
}

func TestRouteCRUDAndCascade(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, s, "relay", "ws.example.com")
	exit := mkExit(t, s, "exit")
	relay.IsExit = true
	if _, err := s.UpdateNode(ctx, relay); err != nil {
		t.Fatal(err)
	}
	_, err := s.CreateRoute(ctx, Route{
		Name:          "direct",
		EntryNodeID:   relay.ID,
		ExitNodeID:    relay.ID,
		TransportType: TransportRawWS,
		Enabled:       true,
	})
	if err != nil {
		t.Fatalf("direct route: %v", err)
	}
	route, err := s.CreateRoute(ctx, Route{
		Name:          "relay-exit",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: TransportSilo,
		Enabled:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateRoute(ctx, Route{
		Name:          "bad",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: "wireguard",
		Enabled:       true,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unsupported type error = %v, want invalid", err)
	}
	route.Enabled = false
	if _, err := s.UpdateRoute(ctx, route); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteNode(ctx, relay.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRoute(ctx, route.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("route after cascade = %v, want not found", err)
	}
}

func TestUserCRUDAndSubscriptionToken(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	user, err := s.CreateUser(ctx, User{Name: "Alice", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if user.ID == "" || len(user.SubToken) != 64 {
		t.Fatalf("user not initialized: %+v", user)
	}
	found, err := s.FindUserBySubToken(ctx, user.SubToken)
	if err != nil || found.ID != user.ID {
		t.Fatalf("find user: %+v %v", found, err)
	}
	if _, err := s.CreateUser(ctx, User{ID: user.ID, SubToken: user.SubToken, Enabled: true}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate user error = %v, want conflict", err)
	}

	user.Name = "Alice Updated"
	user.TrafficLimitBytes = 100
	if _, err := s.UpdateUser(ctx, user); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTraffic(ctx, []TrafficDelta{{UserID: user.ID, UploadBytes: 10, DownloadBytes: 20}}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.UsedUploadBytes != 10 || got.UsedDownloadBytes != 20 || got.Name != "Alice Updated" {
		t.Fatalf("unexpected user after update: %+v", got)
	}
	if err := s.ResetUserTraffic(ctx, user.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetUser(ctx, user.ID)
	if got.UsedBytes() != 0 {
		t.Fatalf("traffic not reset: %+v", got)
	}
	if err := s.DeleteUser(ctx, user.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetUser(ctx, user.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted user error = %v, want not found", err)
	}
}

func TestAddTrafficAtomicConcurrent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	user, err := s.CreateUser(ctx, User{Name: "Concurrent", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 32
	const iterations = 50
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if err := s.AddTraffic(ctx, []TrafficDelta{{
					UserID:        user.ID,
					UploadBytes:   100,
					DownloadBytes: 200,
				}}); err != nil {
					t.Errorf("add traffic: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	got, err := s.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(goroutines * iterations * 100)
	if got.UsedUploadBytes != want {
		t.Fatalf("upload = %d, want %d", got.UsedUploadBytes, want)
	}
	if got.UsedDownloadBytes != want*2 {
		t.Fatalf("download = %d, want %d", got.UsedDownloadBytes, want*2)
	}
}

func TestActiveUsersForExitAndLimits(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, s, "relay", "ws.example.com")
	exit := mkExit(t, s, "exit")
	route, err := s.CreateRoute(ctx, Route{
		Name:          "route",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: TransportRawWS,
		Enabled:       true,
	})
	if err != nil {
		t.Fatal(err)
	}

	active, err := s.CreateUser(ctx, User{Name: "active", Enabled: true, AllowedRouteIDs: []int64{route.ID}})
	if err != nil {
		t.Fatal(err)
	}
	expired, err := s.CreateUser(ctx, User{
		Name:      "expired",
		Enabled:   true,
		ExpiresAt: time.Now().Add(-time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	limited, err := s.CreateUser(ctx, User{
		Name:              "limited",
		Enabled:           true,
		TrafficLimitBytes: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddTraffic(ctx, []TrafficDelta{{UserID: limited.ID, UploadBytes: 60, DownloadBytes: 50}}); err != nil {
		t.Fatal(err)
	}
	limited, err = s.GetUser(ctx, limited.ID)
	if err != nil {
		t.Fatal(err)
	}
	disallowed, err := s.CreateUser(ctx, User{Name: "disallowed", Enabled: true, AllowedRouteIDs: []int64{route.ID + 100}})
	if err != nil {
		t.Fatal(err)
	}

	users, err := s.ActiveUsersForExit(ctx, exit.ID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 1 || users[0].ID != active.ID {
		t.Fatalf("active users = %+v, want only %s", users, active.ID)
	}
	if expired.Active(time.Now()) || limited.Active(time.Now()) {
		t.Fatal("inactive users reported active")
	}
	if disallowed.RouteAllowed(route.ID) {
		t.Fatal("disallowed user may use the route")
	}
	all, err := s.ActiveUsers(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("active users = %d, want 2 (active + disallowed)", len(all))
	}
}
