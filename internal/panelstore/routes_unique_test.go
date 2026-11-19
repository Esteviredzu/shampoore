package panelstore

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestCreateRouteRejectsDuplicateEndpointsAndTransport(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, store, "relay-dup", "ws.dup.example.com")
	exit := mkExit(t, store, "exit-dup")

	route := Route{
		Name:          "dup-raw-ws",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: TransportRawWS,
		Enabled:       true,
	}
	if _, err := store.CreateRoute(ctx, route); err != nil {
		t.Fatalf("create first route: %v", err)
	}

	duplicate := route
	duplicate.Name = "dup-raw-ws-2"
	if _, err := store.CreateRoute(ctx, duplicate); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate route error = %v, want conflict", err)
	}

	otherTransport := route
	otherTransport.Name = "dup-silo"
	otherTransport.TransportType = TransportSilo
	if _, err := store.CreateRoute(ctx, otherTransport); err != nil {
		t.Fatalf("same endpoints with another transport must be allowed: %v", err)
	}
}

func TestUpdateRouteRejectsDuplicateEndpointsAndTransport(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, store, "relay-upd", "ws.upd.example.com")
	exit := mkExit(t, store, "exit-upd")

	rawWS, err := store.CreateRoute(ctx, Route{
		Name:          "raw-ws",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: TransportRawWS,
		Enabled:       true,
	})
	if err != nil {
		t.Fatalf("create raw_ws route: %v", err)
	}
	silo, err := store.CreateRoute(ctx, Route{
		Name:          "silo",
		EntryNodeID:   relay.ID,
		ExitNodeID:    exit.ID,
		TransportType: TransportSilo,
		Enabled:       true,
	})
	if err != nil {
		t.Fatalf("create silo route: %v", err)
	}

	rawWS.TransportType = TransportSilo
	if _, err := store.UpdateRoute(ctx, rawWS); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting update error = %v, want conflict", err)
	}

	silo.Enabled = false
	if _, err := store.UpdateRoute(ctx, silo); err != nil {
		t.Fatalf("non-conflicting update failed: %v", err)
	}
}

func TestConcurrentRouteCreationAllowsOnlyOneDuplicate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	relay := mkRelay(t, store, "relay-race", "ws.race.example.com")
	exit := mkExit(t, store, "exit-race")

	const attempts = 8
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.CreateRoute(ctx, Route{
				Name:          "race-" + string(rune('a'+i)),
				EntryNodeID:   relay.ID,
				ExitNodeID:    exit.ID,
				TransportType: TransportLiveKit,
				Enabled:       true,
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)

	created, conflicts := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			created++
		case errors.Is(err, ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected create error: %v", err)
		}
	}
	if created != 1 || conflicts != attempts-1 {
		t.Fatalf("created=%d conflicts=%d, want 1 and %d", created, conflicts, attempts-1)
	}
}
