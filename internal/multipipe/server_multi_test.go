package multipipe

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multipipe/bond"
)

const serverCarrierKind = "multipipe_server_carrier"

type fakeAggSession struct {
	mu    sync.Mutex
	sink  app.SessionTransportSink
	start bool
}

func (f *fakeAggSession) Start(context.Context) error {
	f.mu.Lock()
	f.start = true
	f.mu.Unlock()
	return nil
}
func (f *fakeAggSession) Stop(context.Context) error           { return nil }
func (f *fakeAggSession) SetSessionHandler(app.SessionHandler) {}
func (f *fakeAggSession) SetTransportSink(sink app.SessionTransportSink) {
	f.mu.Lock()
	f.sink = sink
	f.mu.Unlock()
}
func (f *fakeAggSession) hasSink() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sink != nil
}

var (
	serverCarrierOnce sync.Once
	sessionsMu        sync.Mutex
	sessions          []*fakeAggSession
)

func resetSessions() {
	sessionsMu.Lock()
	sessions = nil
	sessionsMu.Unlock()
}

func recordSession(s *fakeAggSession) {
	sessionsMu.Lock()
	sessions = append(sessions, s)
	sessionsMu.Unlock()
}

func recordedSessions() []*fakeAggSession {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	return append([]*fakeAggSession(nil), sessions...)
}

func registerServerCarrier() {
	serverCarrierOnce.Do(func() {
		err := app.RegisterDescriptor(app.Descriptor{
			Kind:     serverCarrierKind,
			Defaults: app.NewValues(),
			Build: func(
				_ context.Context,
				_ *app.Settings,
				role string,
				_ logging.ContextLogger,
				_ app.Values,
			) (*app.BuildResult, error) {
				if role != "server" {
					return &app.BuildResult{Transport: &recordingTransport{}}, nil
				}
				sess := &fakeAggSession{}
				recordSession(sess)
				return &app.BuildResult{
					SessionServer: sess,
					UserRegistry:  auth.NewRegistry(),
				}, nil
			},
		})
		if err != nil {
			panic(err)
		}
	})
}

func serverMultiRuntime(t *testing.T, strategy string) *app.Runtime {
	t.Helper()
	registerServerCarrier()
	resetSessions()

	profile := map[string]any{
		"type":         "multi",
		"exit_node_id": "42",
		"transport": map[string]any{
			"strategy": strategy,
			"transports": []map[string]any{
				{"type": serverCarrierKind},
				{"type": serverCarrierKind},
			},
		},
	}
	raw, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := app.ParseSource(string(raw))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt, err := app.BuildRuntime(ctx, settings, "server", logging.New("multipipe-server-test", false))
	if err != nil {
		t.Fatalf("build runtime: %v", err)
	}
	return rt
}

func TestServerMultiBondBuildsAggregator(t *testing.T) {
	rt := serverMultiRuntime(t, "bond")
	if rt.SessionServer == nil {
		t.Fatal("server bond runtime has no session server")
	}
	if _, ok := rt.SessionServer.(*bond.Server); !ok {
		t.Fatalf("session server = %T, want *bond.Server", rt.SessionServer)
	}
	if rt.StreamMux != nil {
		t.Fatal("server bond runtime must not expose a client stream multiplexer")
	}
	if rt.UserRegistry == nil {
		t.Fatal("aggregate registry was not propagated")
	}
	fakes := recordedSessions()
	if len(fakes) != 2 {
		t.Fatalf("child sessions = %d, want 2", len(fakes))
	}
	for i, fake := range fakes {
		if !fake.hasSink() {
			t.Fatalf("child %d did not receive the bond transport sink", i)
		}
	}
}

func TestServerMultiFlowmuxUsesCompositeSessionServer(t *testing.T) {
	rt := serverMultiRuntime(t, "flowmux")
	if rt.SessionServer == nil {
		t.Fatal("server flowmux runtime has no session server")
	}
	if _, ok := rt.SessionServer.(*compositeSessionServer); !ok {
		t.Fatalf("session server = %T, want *compositeSessionServer", rt.SessionServer)
	}
	for i, fake := range recordedSessions() {
		if fake.hasSink() {
			t.Fatalf("child %d was aggregated although flowmux keeps independent tunnels", i)
		}
	}
}

func TestServerMultiRequiresSessionChildren(t *testing.T) {
	registerServerCarrier()
	resetSessions()

	profile := map[string]any{
		"type": "multi",
		"transport": map[string]any{
			"strategy": "bond",
			"transports": []map[string]any{
				{"type": testCarrierKind},
				{"type": serverCarrierKind},
			},
		},
	}
	raw, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := app.ParseSource(string(raw))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := app.BuildRuntime(ctx, settings, "server", logging.New("multipipe-server-test", false)); err == nil {
		t.Fatal("bond on the server accepted a child without a session server")
	}
}
