package panelstore

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSettingsDefaultsAndUpdate(t *testing.T) {
	store, err := Open(context.Background(), filepath.Join(t.TempDir(), "panel.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	settings, err := store.GetSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if settings.PublicListen != DefaultPublicListen {
		t.Fatalf("default public listen = %q", settings.PublicListen)
	}
	exists, err := store.SettingsExists(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("fresh database must not report existing settings")
	}

	saved, err := store.UpdateSettings(context.Background(), Settings{
		PublicDomain: "Sub.Example.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	if saved.PublicDomain != "sub.example.com" || saved.PublicURL != "https://sub.example.com" {
		t.Fatalf("normalized settings = %+v", saved)
	}
	if !saved.SubscriptionsEnabled {
		t.Fatalf("subscriptions must default to enabled with a domain: %+v", saved)
	}
	if saved.PublicListen != DefaultPublicListen {
		t.Fatalf("public listen = %q", saved.PublicListen)
	}

	reloaded, err := store.GetSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.PublicDomain != "sub.example.com" || reloaded.PublicURL != "https://sub.example.com" {
		t.Fatalf("reloaded settings = %+v", reloaded)
	}
	if !reloaded.SubscriptionsEnabled {
		t.Fatalf("reloaded subscriptions = %+v", reloaded)
	}
	if reloaded.UpdatedAt.IsZero() {
		t.Fatal("settings timestamp was not stored")
	}

	for _, domain := range []string{"bad domain", "nodot", "-bad.example.com", "sub..example.com"} {
		if _, err := store.UpdateSettings(context.Background(), Settings{PublicDomain: domain}); err == nil {
			t.Fatalf("accepted invalid domain %q", domain)
		}
	}
}

func TestSettingsMigrationFromVersionOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "panel.db")
	store, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.db.ExecContext(context.Background(), "DROP TABLE settings; PRAGMA user_version=1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen migrated database: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.GetSettings(context.Background()); err != nil {
		t.Fatalf("settings table was not migrated: %v", err)
	}
}

func TestSettingsSubscriptionsExplicitToggle(t *testing.T) {
	store, err := Open(context.Background(), filepath.Join(t.TempDir(), "panel.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	disabled := Settings{PublicDomain: "sub.example.com"}
	disabled.SetSubscriptionsEnabled(false)
	saved, err := store.UpdateSettings(ctx, disabled)
	if err != nil {
		t.Fatal(err)
	}
	if saved.SubscriptionsEnabled {
		t.Fatalf("explicit disable was overridden: %+v", saved)
	}
	reloaded, err := store.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.SubscriptionsEnabled || reloaded.PublicDomain != "sub.example.com" {
		t.Fatalf("reloaded disabled settings = %+v", reloaded)
	}

	reloaded.PublicListen = "127.0.0.1:9944"
	saved, err = store.UpdateSettings(ctx, reloaded)
	if err != nil {
		t.Fatal(err)
	}
	if saved.SubscriptionsEnabled {
		t.Fatalf("disable state was lost on settings update: %+v", saved)
	}

	cleared := saved
	cleared.PublicDomain = ""
	cleared.PublicURL = ""
	saved, err = store.UpdateSettings(ctx, cleared)
	if err != nil {
		t.Fatal(err)
	}
	if saved.SubscriptionsEnabled {
		t.Fatalf("subscriptions must be disabled without a domain: %+v", saved)
	}
}
