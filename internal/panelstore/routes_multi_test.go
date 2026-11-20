package panelstore

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func mkSingleRoutes(t *testing.T, store *Store, relay, exit Node) []Route {
	t.Helper()
	ctx := context.Background()
	out := make([]Route, 0, 3)
	for _, kind := range []string{TransportRawWS, TransportSilo, TransportLiveKit} {
		route, err := store.CreateRoute(ctx, Route{
			Name:          "single-" + kind,
			EntryNodeID:   relay.ID,
			ExitNodeID:    exit.ID,
			TransportType: kind,
			Enabled:       true,
		})
		if err != nil {
			t.Fatalf("create single route %s: %v", kind, err)
		}
		out = append(out, route)
	}
	return out
}

func TestMultiRouteValidation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, store, "relay-multi", "ws.multi.example.com")
	exit := mkExit(t, store, "exit-multi")
	singles := mkSingleRoutes(t, store, relay, exit)

	cases := []struct {
		name  string
		route Route
	}{
		{
			name: "bad strategy",
			route: Route{Name: "bad-strategy", TransportType: TransportMulti, MultiStrategy: "roundrobin",
				SubRouteIDs: []int64{singles[0].ID, singles[1].ID}},
		},
		{
			name: "too few children",
			route: Route{Name: "too-few", TransportType: TransportMulti, MultiStrategy: MultiStrategyBond,
				SubRouteIDs: []int64{singles[0].ID}},
		},
		{
			name: "missing child",
			route: Route{Name: "missing-child", TransportType: TransportMulti, MultiStrategy: MultiStrategyBond,
				SubRouteIDs: []int64{singles[0].ID, 9999}},
		},
		{
			name: "duplicate children",
			route: Route{Name: "duplicate-children", TransportType: TransportMulti, MultiStrategy: MultiStrategyBond,
				SubRouteIDs: []int64{singles[0].ID, singles[0].ID}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.CreateRoute(ctx, tc.route); !errors.Is(err, ErrInvalid) {
				t.Fatalf("error = %v, want invalid", err)
			}
		})
	}
}

func TestMultiRoutePersistsAndCoexistsByStrategy(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, store, "relay-multi-ok", "ws.multi-ok.example.com")
	exit := mkExit(t, store, "exit-multi-ok")
	singles := mkSingleRoutes(t, store, relay, exit)

	bond, err := store.CreateRoute(ctx, Route{
		Name:          "bond-route",
		TransportType: TransportMulti,
		MultiStrategy: MultiStrategyBond,
		SubRouteIDs:   []int64{singles[0].ID, singles[1].ID},
		Enabled:       true,
	})
	if err != nil {
		t.Fatalf("create bond route: %v", err)
	}
	if bond.EntryNodeID != relay.ID || bond.ExitNodeID != exit.ID {
		t.Fatalf("derived endpoints = %d/%d, want %d/%d", bond.EntryNodeID, bond.ExitNodeID, relay.ID, exit.ID)
	}
	got, err := store.GetRoute(ctx, bond.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TransportType != TransportMulti || got.MultiStrategy != MultiStrategyBond {
		t.Fatalf("persisted multi route = %+v", got)
	}
	if len(got.SubRouteIDs) != 2 || got.SubRouteIDs[0] != singles[0].ID || got.SubRouteIDs[1] != singles[1].ID {
		t.Fatalf("persisted sub-route ids = %v", got.SubRouteIDs)
	}

	if _, err := store.CreateRoute(ctx, Route{
		Name:          "flowmux-route",
		TransportType: TransportMulti,
		MultiStrategy: MultiStrategyFlowMux,
		SubRouteIDs:   []int64{singles[1].ID, singles[2].ID},
		Enabled:       true,
	}); err != nil {
		t.Fatalf("flowmux route must coexist with bond: %v", err)
	}

	routes, err := store.ListRoutes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	multi := 0
	for _, route := range routes {
		if route.TransportType == TransportMulti {
			multi++
		}
	}
	if multi != 2 {
		t.Fatalf("multi routes = %d, want 2", multi)
	}
}

func TestMultiRouteRejectsRecursionAndMixedEndpoints(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, store, "relay-multi-guard", "ws.multi-guard.example.com")
	exit := mkExit(t, store, "exit-multi-guard")
	singles := mkSingleRoutes(t, store, relay, exit)

	multi, err := store.CreateRoute(ctx, Route{
		Name:          "guard-bond",
		TransportType: TransportMulti,
		MultiStrategy: MultiStrategyBond,
		SubRouteIDs:   []int64{singles[0].ID, singles[1].ID},
		Enabled:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRoute(ctx, Route{
		Name:          "nested-multi",
		TransportType: TransportMulti,
		MultiStrategy: MultiStrategyFlowMux,
		SubRouteIDs:   []int64{multi.ID, singles[2].ID},
		Enabled:       true,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nested multi error = %v, want invalid", err)
	}

	relay2 := mkRelay(t, store, "relay-multi-other", "ws.multi-other.example.com")
	exit2 := mkExit(t, store, "exit-multi-other")
	other, err := store.CreateRoute(ctx, Route{
		Name:          "other-path",
		EntryNodeID:   relay2.ID,
		ExitNodeID:    exit2.ID,
		TransportType: TransportRawWS,
		Enabled:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRoute(ctx, Route{
		Name:          "mixed-endpoints",
		TransportType: TransportMulti,
		MultiStrategy: MultiStrategyBond,
		SubRouteIDs:   []int64{singles[0].ID, other.ID},
		Enabled:       true,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mixed endpoints error = %v, want invalid", err)
	}
}

func TestDeleteChildRouteBlockedByMulti(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, store, "relay-multi-del", "ws.multi-del.example.com")
	exit := mkExit(t, store, "exit-multi-del")
	singles := mkSingleRoutes(t, store, relay, exit)

	multi, err := store.CreateRoute(ctx, Route{
		Name:          "delete-guard-bond",
		TransportType: TransportMulti,
		MultiStrategy: MultiStrategyBond,
		SubRouteIDs:   []int64{singles[0].ID, singles[1].ID},
		Enabled:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteRoute(ctx, singles[0].ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete child error = %v, want conflict", err)
	}
	if err := store.DeleteRoute(ctx, multi.ID); err != nil {
		t.Fatalf("delete multi: %v", err)
	}
	if err := store.DeleteRoute(ctx, singles[0].ID); err != nil {
		t.Fatalf("delete child after multi removal: %v", err)
	}
}

func TestDeleteNodePrunesDependentMultiRoutes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, store, "relay-multi-node", "ws.multi-node.example.com")
	exit := mkExit(t, store, "exit-multi-node")
	singles := mkSingleRoutes(t, store, relay, exit)

	multi, err := store.CreateRoute(ctx, Route{
		Name:          "node-guard-bond",
		TransportType: TransportMulti,
		MultiStrategy: MultiStrategyBond,
		SubRouteIDs:   []int64{singles[0].ID, singles[1].ID},
		Enabled:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteNode(ctx, relay.ID); err != nil {
		t.Fatalf("delete relay node: %v", err)
	}
	if _, err := store.GetRoute(ctx, multi.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale multi route error = %v, want not found", err)
	}
	if _, err := store.GetNode(ctx, exit.ID); err != nil {
		t.Fatalf("exit node must survive: %v", err)
	}
}

func TestMigrationUpgradesLegacyRoutesTable(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/legacy.db"
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := `
	CREATE TABLE nodes (
		id   INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE
	);
	CREATE TABLE routes (
		id             INTEGER PRIMARY KEY AUTOINCREMENT,
		name           TEXT    NOT NULL UNIQUE,
		entry_node_id  INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
		exit_node_id   INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
		transport_type TEXT    NOT NULL CHECK (transport_type IN ('raw_ws','silo','livekit')),
		enabled        INTEGER NOT NULL DEFAULT 1
	);
	INSERT INTO nodes (id, name) VALUES (1, 'relay'), (2, 'exit');
	INSERT INTO routes (name, entry_node_id, exit_node_id, transport_type, enabled)
		VALUES ('legacy-route', 1, 2, 'raw_ws', 1);
	PRAGMA user_version=5;`
	if _, err := db.ExecContext(ctx, legacy); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	defer store.Close()

	routes, err := store.ListRoutes(ctx)
	if err != nil {
		t.Fatalf("list migrated routes: %v", err)
	}
	if len(routes) != 1 || routes[0].Name != "legacy-route" {
		t.Fatalf("migrated routes = %+v", routes)
	}
	if routes[0].MultiStrategy != "" || len(routes[0].SubRouteIDs) != 0 {
		t.Fatalf("legacy route defaults = %+v", routes[0])
	}

	res, err := store.db.ExecContext(ctx, `INSERT INTO routes
		(name, entry_node_id, exit_node_id, transport_type, multi_strategy, sub_route_ids_json, enabled)
		VALUES ('legacy-multi', 1, 2, 'multi', 'bond', '[1]', 1)`)
	if err != nil {
		t.Fatalf("insert multi into migrated table: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	multi, err := store.GetRoute(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if multi.TransportType != TransportMulti || multi.MultiStrategy != MultiStrategyBond || len(multi.SubRouteIDs) != 1 {
		t.Fatalf("multi route after migration = %+v", multi)
	}
}
