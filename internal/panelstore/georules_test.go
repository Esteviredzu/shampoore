package panelstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNormalizeGeoRuleURL(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{
			name: "github blob",
			in:   "https://github.com/user/repo/blob/main/rules/ru.txt",
			want: "https://raw.githubusercontent.com/user/repo/main/rules/ru.txt",
		},
		{
			name: "github blob with nested path",
			in:   "https://GitHub.com/user/repo/blob/master/lists/cn/geoip.txt",
			want: "https://raw.githubusercontent.com/user/repo/master/lists/cn/geoip.txt",
		},
		{
			name: "raw url unchanged",
			in:   "https://raw.githubusercontent.com/user/repo/main/ru.txt",
			want: "https://raw.githubusercontent.com/user/repo/main/ru.txt",
		},
		{
			name: "plain https url unchanged",
			in:   "https://example.com/rules/ru.txt",
			want: "https://example.com/rules/ru.txt",
		},
		{
			name:    "github non blob url rejected",
			in:      "https://github.com/user/repo/tree/main/rules",
			wantErr: true,
		},
		{
			name:    "non http scheme rejected",
			in:      "ftp://example.com/rules.txt",
			wantErr: true,
		},
		{
			name:    "missing path rejected",
			in:      "https://example.com",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeGeoRuleURL(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NormalizeGeoRuleURL(%q) = %q, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeGeoRuleURL(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("NormalizeGeoRuleURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestGeoRuleSourceCRUDAndDue(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	saved, err := store.UpsertGeoRuleSource(ctx, GeoRuleSource{
		Name:                  "ru",
		URL:                   "https://github.com/user/repo/blob/main/ru.txt",
		UpdateIntervalMinutes: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	if saved.URL != "https://raw.githubusercontent.com/user/repo/main/ru.txt" {
		t.Fatalf("stored url = %q", saved.URL)
	}

	due, err := store.DueGeoRuleSources(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Name != "ru" {
		t.Fatalf("fresh source must be due: %+v", due)
	}

	if err := store.MarkGeoRuleSourceUpdated(ctx, "ru", now); err != nil {
		t.Fatal(err)
	}
	due, err = store.DueGeoRuleSources(ctx, now.Add(30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("source must not be due yet: %+v", due)
	}
	due, err = store.DueGeoRuleSources(ctx, now.Add(61*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 {
		t.Fatalf("source must be due after the interval: %+v", due)
	}

	if err := store.SetGeoRuleSourceError(ctx, "ru", "timeout"); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetGeoRuleSource(ctx, "ru")
	if err != nil {
		t.Fatal(err)
	}
	if got.LastError != "timeout" || got.LastUpdatedAt.IsZero() {
		t.Fatalf("source after error = %+v", got)
	}

	list, err := store.ListGeoRuleSources(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %+v, %v", list, err)
	}
	if err := store.DeleteGeoRuleSource(ctx, "ru"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetGeoRuleSource(ctx, "ru"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get deleted source = %v, want not found", err)
	}
	if err := store.DeleteGeoRuleSource(ctx, "ru"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func TestGeoRuleSourceValidation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	for _, src := range []GeoRuleSource{
		{Name: "", URL: "https://example.com/rules.txt"},
		{Name: "a/b", URL: "https://example.com/rules.txt"},
		{Name: "ru", URL: "ftp://example.com/rules.txt"},
		{Name: "ru2", URL: "https://example.com/rules.txt", UpdateIntervalMinutes: -1},
	} {
		if _, err := store.UpsertGeoRuleSource(ctx, src); err == nil {
			t.Fatalf("accepted invalid source %+v", src)
		}
	}
}

func TestGeoRuleNodeBindings(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	exitA := mkExit(t, store, "exit-a")
	exitB := mkExit(t, store, "exit-b")
	relay := mkRelay(t, store, "relay", "ws.example.com")

	if _, err := store.UpsertGeoRuleSource(ctx, GeoRuleSource{
		Name: "ru", URL: "https://example.com/ru.txt",
	}); err != nil {
		t.Fatal(err)
	}

	ids, err := store.SetGeoRuleNodes(ctx, "ru", []int64{exitB.ID, exitA.ID, exitA.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != exitA.ID || ids[1] != exitB.ID {
		t.Fatalf("normalized bindings = %v", ids)
	}
	got, err := store.GeoRuleNodes(ctx, "ru")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != exitA.ID || got[1] != exitB.ID {
		t.Fatalf("bindings = %v", got)
	}
	source, err := store.GetGeoRuleSource(ctx, "ru")
	if err != nil {
		t.Fatal(err)
	}
	if len(source.ExitNodeIDs) != 2 || source.ExitNodeIDs[0] != exitA.ID {
		t.Fatalf("source bindings = %v", source.ExitNodeIDs)
	}
	list, err := store.ListGeoRuleSources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || len(list[0].ExitNodeIDs) != 2 {
		t.Fatalf("listed sources = %+v", list)
	}
	bindings, err := store.ListGeoRuleBindings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings["ru"]) != 2 {
		t.Fatalf("all bindings = %+v", bindings)
	}

	if _, err := store.SetGeoRuleNodes(ctx, "ru", []int64{relay.ID}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("relay binding error = %v, want invalid", err)
	}
	if _, err := store.SetGeoRuleNodes(ctx, "ru", []int64{999999}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing node binding error = %v, want invalid", err)
	}

	ids, err = store.SetGeoRuleNodes(ctx, "ru", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("cleared bindings = %v", ids)
	}

	if _, err := store.SetGeoRuleNodes(ctx, "ru", []int64{exitA.ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteNode(ctx, exitA.ID); err != nil {
		t.Fatal(err)
	}
	got, err = store.GeoRuleNodes(ctx, "ru")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("bindings after node delete = %v", got)
	}

	if _, err := store.SetGeoRuleNodes(ctx, "ru", []int64{exitB.ID}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteGeoRuleSource(ctx, "ru"); err != nil {
		t.Fatal(err)
	}
	got, err = store.GeoRuleNodes(ctx, "ru")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("bindings after source delete = %v", got)
	}
}
