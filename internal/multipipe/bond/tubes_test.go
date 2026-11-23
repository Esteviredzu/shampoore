package bond

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/relay"
	"github.com/Esteviredzu/shampoore/internal/testutil/s3test"
	"github.com/Esteviredzu/shampoore/internal/transport"
	"github.com/Esteviredzu/shampoore/internal/transport/raw_ws"
	"github.com/Esteviredzu/shampoore/internal/transport/silo"
)

const (
	tubesPassword  = "bond-three-tubes-secret"
	tubesTunnelID  = "route-9"
	tubesRelayPass = "relay-room-password"
	tubesBucket    = "shampoore-tubes"
)

func buildSubflow(
	t *testing.T,
	role, kind string,
	raw json.RawMessage,
	userID, exitNodeID, password string,
	logger *captureLogger,
) *app.BuildResult {
	t.Helper()
	d, ok := app.DescriptorFor(kind)
	if !ok {
		t.Fatalf("no descriptor for %q", kind)
	}
	settings := &app.Settings{
		Type:               kind,
		Transport:          raw,
		UserID:             userID,
		ExitNodeID:         exitNodeID,
		EncryptionPassword: password,
	}
	res, err := d.Build(context.Background(), settings, role, logger, d.Defaults.Merge(settings.Advanced))
	if err != nil {
		t.Fatalf("build %s %s: %v", role, kind, err)
	}
	return res
}

func bottomlessEcho(ctx context.Context, mux multiplexing.StreamMultiplexer) error {
	for {
		stream, err := mux.AcceptStream(ctx)
		if err != nil {
			return err
		}
		go func(st multiplexing.Stream) {
			defer st.Close()
			_ = st.SetDeadline(time.Now().Add(30 * time.Second))
			buf := make([]byte, 32*1024)
			for {
				n, err := st.Read(buf)
				if n > 0 {
					if _, werr := st.Write(buf[:n]); werr != nil {
						return
					}
				}
				if err != nil {
					return
				}
			}
		}(stream)
	}
}

func tubesJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func TestBondThreeTubesRoundTripAndIdle(t *testing.T) {
	s3Server := s3test.New(t)
	relayServer := relay.New(tubesRelayPass)
	relayHTTP := httptest.NewServer(relayServer)
	t.Cleanup(relayHTTP.Close)

	logger := &captureLogger{}
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{UserID: "alice", Enabled: true}); err != nil {
		t.Fatal(err)
	}

	siloServerJSON := tubesJSON(t, silo.Config{
		Endpoint:           s3Server.Endpoint(),
		AccessKey:          "minioadmin",
		SecretKey:          "minioadmin",
		Bucket:             tubesBucket,
		TunnelID:           tubesTunnelID,
		Multiuser:          true,
		Users:              []auth.UserSpec{{UserID: "alice", Enabled: true}},
		MinFileSizeKB:      256,
		MaxFileSizeKB:      512,
		FlushMs:            50,
		PollIntervalMs:     10,
		EncryptionPassword: tubesPassword,
	})
	siloClientJSON := tubesJSON(t, silo.Config{
		Endpoint:           s3Server.Endpoint(),
		AccessKey:          "minioadmin",
		SecretKey:          "minioadmin",
		Bucket:             tubesBucket,
		TunnelID:           tubesTunnelID,
		UserID:             "alice",
		MinFileSizeKB:      256,
		MaxFileSizeKB:      512,
		FlushMs:            50,
		PollIntervalMs:     10,
		EncryptionPassword: tubesPassword,
	})
	wsServerJSON := tubesJSON(t, raw_ws.Config{
		ServerURL:       relayHTTP.URL,
		Room:            tubesTunnelID,
		Password:        tubesRelayPass,
		AllowInsecureWS: true,
		Multiuser:       true,
		Users:           []auth.UserSpec{{UserID: "alice", Enabled: true}},
	})
	wsClientJSON := tubesJSON(t, raw_ws.Config{
		ServerURL:       relayHTTP.URL,
		Room:            tubesTunnelID,
		Password:        tubesRelayPass,
		Batch:           true,
		AllowInsecureWS: true,
		UserID:          "alice",
	})

	siloServerRes := buildSubflow(t, "server", "silo", siloServerJSON, "", "42", tubesPassword, logger)
	wsServerRes := buildSubflow(t, "server", "raw_ws", wsServerJSON, "", "42", tubesPassword, logger)
	siloClientRes := buildSubflow(t, "client", "silo", siloClientJSON, "alice", "42", tubesPassword, logger)
	wsClientRes := buildSubflow(t, "client", "raw_ws", wsClientJSON, "alice", "42", tubesPassword, logger)

	stubClient, stubServer := newStubPair()
	stubServerSession := newStubSessionServer("alice", stubServer)
	names := []string{"0-livekit", "1-silo", "2-raw_ws"}
	children := []app.SessionServer{stubServerSession, siloServerRes.SessionServer, wsServerRes.SessionServer}

	aggregator := NewServer(names, children, logger, tunnel.Config{
		FlushThreshold: 8192,
		FlushInterval:  5 * time.Millisecond,
	}, registry)
	aggregator.SetEncryptionPassword(tubesPassword)
	aggregator.SetExitNodeID("42")

	ready := make(chan struct{})
	var readyOnce sync.Once
	aggregator.SetSessionHandler(func(hctx context.Context, mux multiplexing.StreamMultiplexer) error {
		readyOnce.Do(func() { close(ready) })
		return bottomlessEcho(hctx, mux)
	})
	for i, child := range children {
		agg, ok := child.(app.AggregableSession)
		if !ok {
			t.Fatalf("child %d is not aggregable", i)
		}
		agg.SetTransportSink(aggregator.Sink(names[i]))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := aggregator.Start(ctx); err != nil {
		t.Fatalf("aggregator start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		_ = aggregator.Stop(stopCtx)
	})
	if !stubServerSession.register() {
		t.Fatal("livekit stub tube was not accepted by the aggregator")
	}

	select {
	case <-ready:
	case <-time.After(30 * time.Second):
		t.Fatalf("aggregated session did not start: %s", logger.dump())
	}

	clientMux, err := NewMux(
		[]transport.Transport{stubClient, siloClientRes.Transport, wsClientRes.Transport},
		names,
		logger,
	)
	if err != nil {
		t.Fatalf("client mux: %v", err)
	}
	clientMux.SetIdentity("alice", "42")
	clientMux.SetEncryptionPassword(tubesPassword)
	if err := clientMux.Start(ctx); err != nil {
		t.Fatalf("client start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		_ = clientMux.Stop(stopCtx)
	})

	stream, err := clientMux.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(60 * time.Second))

	payload := bytes.Repeat([]byte("bond-tube-"), 30000)
	writeErr := make(chan error, 1)
	go func() {
		_, err := stream.Write(payload)
		writeErr <- err
	}()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil {
		t.Fatalf("read echo (%d bytes): %v\n%s", len(payload), err, logger.dump())
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("echo mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	carried := func() map[string]int64 {
		out := map[string]int64{}
		for _, layer := range aggregator.WalkLayers() {
			if layer.Kind != "pipe" {
				continue
			}
			out[layer.Type] += layer.Snapshot.NetworkSent
		}
		return out
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		stats := carried()
		complete := true
		for _, kind := range []string{"livekit", "silo", "raw_ws"} {
			if stats[kind] <= 0 {
				complete = false
			}
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tube(s) never carried traffic: %+v\n%s", stats, logger.dump())
		}
		probe := bytes.Repeat([]byte("bond-probe-"), 6000)
		if _, err := stream.Write(probe); err != nil {
			t.Fatalf("fair-share probe write: %v\n%s", err, logger.dump())
		}
		reply := make([]byte, len(probe))
		if _, err := io.ReadFull(stream, reply); err != nil {
			t.Fatalf("fair-share probe read: %v\n%s", err, logger.dump())
		}
		if !bytes.Equal(reply, probe) {
			t.Fatal("fair-share probe echo mismatch")
		}
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(6 * time.Second)

	probe := []byte("after-idle")
	writeErr = make(chan error, 1)
	go func() {
		_, err := stream.Write(probe)
		writeErr <- err
	}()
	reply := make([]byte, len(probe))
	if _, err := io.ReadFull(stream, reply); err != nil {
		t.Fatalf("read after idle: %v\n%s", err, logger.dump())
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write after idle: %v", err)
	}
	if !bytes.Equal(reply, probe) {
		t.Fatalf("reply after idle = %q, want %q", reply, probe)
	}

	if msg, found := logger.contains("all subflows stopped receiving"); found {
		t.Fatalf("bond reset an idle session: %s\n%s", msg, logger.dump())
	}
	if msg, found := logger.contains("session reset"); found {
		t.Fatalf("bond reset an idle session: %s", msg)
	}
}
