package httpstream

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const bulkMessageSize = 32 << 10

func bulkProfile() MimicProfile {
	profile := testProfile(DownlinkStream)
	profile.MaxStreamBytes = 64 << 20
	profile.MaxStreamDuration = time.Minute
	profile.MaxUplinkBatchBytes = 64 << 10
	profile.UplinkBatchWindow = time.Millisecond
	profile.MaxUplinkInflight = 2
	profile.Padding = Padding{}
	return profile.Normalize()
}

func bulkServerConfig() ServerConfig {
	cfg := testServerConfig(nil)

	cfg.DownlinkReplayBytes = 4 << 20
	cfg.MaxMessageSize = bulkMessageSize
	cfg.MaxUplinkBodyBytes = 4 << 20
	cfg.ReorderTimeout = 2 * time.Second
	cfg.KeepAliveInterval = 5 * time.Second
	return cfg
}

func newBulkPeer(t *testing.T, srv *httptest.Server, hc *http.Client, profile MimicProfile, role, user, sessionID string) *Client {
	t.Helper()
	cfg := ClientConfig{
		BaseURL:          srv.URL,
		Secret:           testSecret,
		Namespace:        testNamespace,
		UserID:           user,
		Role:             role,
		SessionID:        sessionID,
		HTTPClient:       hc,
		Logger:           testLogger{},
		MaxMessageSize:   bulkMessageSize,
		ReadIdleTimeout:  15 * time.Second,
		ReconnectDelay:   10 * time.Millisecond,
		ReconnectMax:     100 * time.Millisecond,
		MaxUplinkRetries: 1,
	}
	client, err := NewClient(profile, cfg)
	if err != nil {
		t.Fatalf("new %s client: %v", role, err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start %s client: %v", role, err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Stop(stopCtx)
	})
	return client
}

func pumpDownlink(t *testing.T, src, dst *Client, total int) (time.Duration, time.Duration) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)

	sendErr := make(chan error, 1)
	go func() {
		payload := bytes.Repeat([]byte{0x5a}, bulkMessageSize)
		sent := 0
		for sent < total {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := src.Send(ctx, payload)
			cancel()
			if err != nil {
				sendErr <- err
				return
			}
			sent += len(payload)
		}
		sendErr <- nil
	}()

	start := time.Now()
	var lastAt time.Time
	var maxGap time.Duration
	received := 0
	for received < total {
		if time.Now().After(deadline) {
			t.Fatalf("received %d of %d bytes before the deadline", received, total)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msg, err := dst.Receive(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			t.Fatalf("receive after %d bytes: %v", received, err)
		}
		now := time.Now()
		if !lastAt.IsZero() {
			if gap := now.Sub(lastAt); gap > maxGap {
				maxGap = gap
			}
		}
		lastAt = now
		received += len(msg)
	}
	if err := <-sendErr; err != nil {
		t.Fatalf("send: %v", err)
	}
	return time.Since(start), maxGap
}

func TestPairBrokerBulkDownlinkAndReconnect(t *testing.T) {
	profile := bulkProfile()
	broker, err := NewPairBroker(profile, bulkServerConfig())
	if err != nil {
		t.Fatalf("new pair broker: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	srv, httpClient, _, _ := newH2Server(t, broker)

	upstream := newBulkPeer(t, srv, httpClient, profile, RoleExit, "alice", "")
	downstream := newBulkPeer(t, srv, httpClient, profile, RoleClient, "alice", "")
	waitPairReady(t, broker, 1, 2)

	total := bulkTransferBytes
	elapsed, maxGap := pumpDownlink(t, upstream, downstream, total)
	if bulkThroughputFloor > 0 {
		if mbps := float64(total) / elapsed.Seconds() / (1 << 20); mbps < bulkThroughputFloor {
			t.Fatalf("bulk downlink throughput dropped to %.2f MiB/s (elapsed %s)", mbps, elapsed)
		}
	}
	if maxGap > 700*time.Millisecond {
		t.Fatalf("downlink stalled for %s: the replay window was not released", maxGap)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = downstream.Stop(stopCtx)
	cancel()
	if err != nil {
		t.Fatalf("stop downstream: %v", err)
	}

	reconnected := newBulkPeer(t, srv, httpClient, profile, RoleClient, "alice", "")

	probe := bytes.Repeat([]byte{0x42}, 4<<10)
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 20*time.Second)
	err = upstream.Send(sendCtx, probe)
	sendCancel()
	if err != nil {
		t.Fatalf("send after reconnect: %v", err)
	}
	started := time.Now()
	var got []byte
	for time.Since(started) < 10*time.Second {
		recvCtx, recvCancel := context.WithTimeout(context.Background(), time.Second)
		msg, recvErr := reconnected.Receive(recvCtx)
		recvCancel()
		if recvErr == nil {
			got = msg
			break
		}
	}
	if got == nil {
		t.Fatal("the reconnected client received nothing: the pair room is stuck")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("first message after reconnect took %s", elapsed)
	}

	elapsed, maxGap = pumpDownlink(t, upstream, reconnected, bulkReconnectBytes)
	if bulkThroughputFloor > 0 {
		if mbps := float64(bulkReconnectBytes) / elapsed.Seconds() / (1 << 20); mbps < bulkThroughputFloor {
			t.Fatalf("throughput after reconnect dropped to %.2f MiB/s", mbps)
		}
	}
	if maxGap > 700*time.Millisecond {
		t.Fatalf("downlink stalled for %s after reconnect", maxGap)
	}
}

func TestSessionResyncsDownlinkAfterStaleCursor(t *testing.T) {
	handler := NewHandler(testProfile(DownlinkStream), testServerConfig(nil), nil)
	defer func() { _ = handler.Close() }()

	sidStr, sid, err := newSessionID()
	if err != nil {
		t.Fatalf("session id: %v", err)
	}
	info := SessionInfo{SessionID: sidStr, Namespace: testNamespace, UserID: "alice", Role: RoleClient}
	session := newSession(handler, "stale-cursor", info, sid)
	defer session.cancel()

	seq, err := session.enqueueDownlink(context.Background(), frameData, [][]byte{[]byte("first")})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if seq != 1 {
		t.Fatalf("first downlink seq = %d, want 1", seq)
	}
	if frames, needReset := session.snapshotFrom(0, 0); needReset || len(frames) != 1 {
		t.Fatalf("frame was not deliverable: frames=%d reset=%v", len(frames), needReset)
	}

	session.ackDownlink(seq)
	if _, needReset := session.snapshotFrom(0, 0); !needReset {
		t.Fatal("a cursor behind the acked frames must be detected as a reset")
	}

	session.resetDownlink()
	if frames, needReset := session.snapshotFrom(0, 0); needReset || len(frames) != 0 {
		t.Fatalf("reset left the buffer inconsistent: frames=%d reset=%v", len(frames), needReset)
	}

	seq, err = session.enqueueDownlink(context.Background(), frameData, [][]byte{[]byte("second")})
	if err != nil {
		t.Fatalf("send after reset: %v", err)
	}
	if seq != 1 {
		t.Fatalf("downlink seq after reset = %d, want 1", seq)
	}
	frames, needReset := session.snapshotFrom(0, 0)
	if needReset || len(frames) != 1 || frames[0].seq != 1 {
		t.Fatalf("post-reset frame is not deliverable: frames=%d reset=%v", len(frames), needReset)
	}
}
