package mobile

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
)

type pipeStream struct {
	conn   net.Conn
	id     string
	meta   map[string]any
	closed atomic.Bool
}

func (s *pipeStream) Read(p []byte) (int, error)  { return s.conn.Read(p) }
func (s *pipeStream) Write(p []byte) (int, error) { return s.conn.Write(p) }
func (s *pipeStream) Close() error {
	s.closed.Store(true)
	return s.conn.Close()
}
func (s *pipeStream) ID() string                         { return s.id }
func (s *pipeStream) Metadata() map[string]any           { return s.meta }
func (s *pipeStream) Closed() bool                       { return s.closed.Load() }
func (s *pipeStream) LocalAddr() net.Addr                { return s.conn.LocalAddr() }
func (s *pipeStream) RemoteAddr() net.Addr               { return s.conn.RemoteAddr() }
func (s *pipeStream) SetDeadline(t time.Time) error      { return s.conn.SetDeadline(t) }
func (s *pipeStream) SetReadDeadline(t time.Time) error  { return s.conn.SetReadDeadline(t) }
func (s *pipeStream) SetWriteDeadline(t time.Time) error { return s.conn.SetWriteDeadline(t) }

type probeMux struct {
	open func(context.Context, map[string]any) (multiplexing.Stream, error)
}

func (m *probeMux) Start(context.Context) error { return nil }
func (m *probeMux) Stop(context.Context) error  { return nil }
func (m *probeMux) OpenStream(ctx context.Context, meta map[string]any) (multiplexing.Stream, error) {
	return m.open(ctx, meta)
}
func (m *probeMux) AcceptStream(context.Context) (multiplexing.Stream, error) {
	return nil, errors.New("probeMux: AcceptStream is not implemented")
}

type recordingSink struct {
	inboundCalls atomic.Int32
	failureCalls atomic.Int32
}

func (s *recordingSink) inbound(int)         { s.inboundCalls.Add(1) }
func (s *recordingSink) streamFailure(error) { s.failureCalls.Add(1) }

func TestProbeExitThroughMux(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	var gotMetadata map[string]any
	mux := &probeMux{open: func(_ context.Context, meta map[string]any) (multiplexing.Stream, error) {
		gotMetadata = meta
		return &pipeStream{conn: client, id: "probe", meta: meta}, nil
	}}

	go func() {
		_, _ = server.Write([]byte(exitProbeReply))
		_ = server.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := probeExitThroughMux(ctx, mux); err != nil {
		t.Fatalf("probe failed: %v", err)
	}
	if got := gotMetadata[exitProbeMetadataKey]; got != exitProbeMetadataVal {
		t.Fatalf("probe metadata = %v, want %q", got, exitProbeMetadataVal)
	}
}

func TestProbeExitThroughMuxTimeout(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	mux := &probeMux{open: func(_ context.Context, meta map[string]any) (multiplexing.Stream, error) {
		return &pipeStream{conn: client, id: "probe", meta: meta}, nil
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := probeExitThroughMux(ctx, mux); err == nil {
		t.Fatal("probe without a reply must fail")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("probe timeout was not enforced, took %v", elapsed)
	}
}

func TestHealthMuxTracksInboundTraffic(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	inner := &probeMux{open: func(_ context.Context, meta map[string]any) (multiplexing.Stream, error) {
		return &pipeStream{conn: server, id: "user", meta: meta}, nil
	}}
	sink := &recordingSink{}
	mux := newHealthMux(inner, sink)

	stream, err := mux.OpenStream(context.Background(), nil)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()

	payload := []byte("hello")
	go func() { _, _ = client.Write(payload) }()

	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if sink.inboundCalls.Load() != 1 {
		t.Fatalf("inbound calls = %d, want 1", sink.inboundCalls.Load())
	}
	if sink.failureCalls.Load() != 0 {
		t.Fatalf("unexpected failure calls = %d", sink.failureCalls.Load())
	}
}

func TestHealthMuxReportsStreamFailureWithoutData(t *testing.T) {
	client, server := net.Pipe()

	inner := &probeMux{open: func(_ context.Context, meta map[string]any) (multiplexing.Stream, error) {
		return &pipeStream{conn: server, id: "user", meta: meta}, nil
	}}
	sink := &recordingSink{}
	mux := newHealthMux(inner, sink)

	stream, err := mux.OpenStream(context.Background(), nil)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()

	_ = client.Close()
	buf := make([]byte, 1)
	if _, err := stream.Read(buf); err == nil {
		t.Fatal("expected read error after peer close")
	}
	if sink.failureCalls.Load() != 1 {
		t.Fatalf("failure calls = %d, want 1", sink.failureCalls.Load())
	}
}

func startTUNForHealth(t *testing.T) *fakeBundle {
	t.Helper()
	bundle := &fakeBundle{
		transport: &fakeTransport{},
		provider:  &fakeProvider{},
		mux:       &fakeMux{},
	}
	installFakeBuilder(t, func() *fakeBundle { return bundle })
	startTUNFD = runFakeTUN
	if got := StartClientVPN(testSource, 0, 1500); got != "" {
		t.Fatalf("StartClientVPN returned %q", got)
	}
	t.Cleanup(func() {
		StopClient()
		waitForState(t, StateIdle, 3*time.Second)
	})
	return bundle
}

func waitForProbeCalls(t *testing.T, calls *atomic.Int32, want int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if calls.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("probe calls = %d, want at least %d", calls.Load(), want)
}

func waitForHealthCode(t *testing.T, code string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	needle := `"error_code":"` + code + `"`
	for time.Now().Before(deadline) {
		if strings.Contains(GetHealth(), needle) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("health did not report %q: %s", code, GetHealth())
}

func waitForResetCalls(t *testing.T, calls *atomic.Int32, want int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if calls.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("transport reset calls = %d, want at least %d", calls.Load(), want)
}

func TestHealthTUNStartsConnectingThenRunning(t *testing.T) {
	resetMobileState(t)

	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	exitProbe = func(ctx context.Context, _ multiplexing.StreamMultiplexer) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	startTUNForHealth(t)
	if GetState() != StateConnecting {
		t.Fatalf("state = %q, want %q", GetState(), StateConnecting)
	}
	if health := GetHealth(); strings.Contains(health, `"tunnel_up":true`) {
		t.Fatalf("tunnel_up must not be reported before the first successful probe: %s", health)
	}

	releaseOnce.Do(func() { close(release) })
	waitForState(t, StateRunning, 2*time.Second)
	if health := GetHealth(); !strings.Contains(health, `"tunnel_up":true`) {
		t.Fatalf("health does not report tunnel_up after a successful probe: %s", health)
	}
}

func TestHealthExitUnreachableKeepsTunnelAlive(t *testing.T) {
	resetMobileState(t)
	healthFailureLimit = 3
	healthRetryInitial = 10 * time.Millisecond
	healthReconnectMax = 100 * time.Millisecond

	var calls atomic.Int32
	var down atomic.Bool
	down.Store(true)
	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error {
		calls.Add(1)
		if down.Load() {
			return errors.New("exit down")
		}
		return nil
	}

	listener := newRecordingListener()
	SetStateListener(listener)
	t.Cleanup(func() { SetStateListener(nil) })

	bundle := startTUNForHealth(t)
	waitForHealthCode(t, HealthCodeExitUnreachable, 2*time.Second)

	if GetState() == StateError {
		t.Fatalf("transient probe failures must not tear the VPN down, state = %q", GetState())
	}
	if got := GetStateMessage(); got == HealthMessageExitUnreachable {
		t.Fatalf("fatal health message leaked into state: %q", got)
	}
	if listener.has(StateError) {
		t.Fatal("listener observed a fatal error state during transient probe failures")
	}

	waitForResetCalls(t, &bundle.transport.resetCalls, 1, 2*time.Second)

	down.Store(false)
	waitForState(t, StateRunning, 2*time.Second)
	if health := GetHealth(); !strings.Contains(health, `"exit_reachable":true`) {
		t.Fatalf("health does not report exit_reachable after recovery: %s", health)
	}
}

func TestHealthProbesPeriodicallyWhileRunning(t *testing.T) {
	resetMobileState(t)
	healthProbeInterval = 20 * time.Millisecond

	var calls atomic.Int32
	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error {
		calls.Add(1)
		return nil
	}

	startTUNForHealth(t)
	waitForState(t, StateRunning, 2*time.Second)
	waitForProbeCalls(t, &calls, 3, 2*time.Second)
}

func TestHealthDoesNotProbeWhenHealthy(t *testing.T) {
	resetMobileState(t)
	healthProbeInterval = time.Hour

	var calls atomic.Int32
	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error {
		calls.Add(1)
		return nil
	}

	startTUNForHealth(t)
	waitForState(t, StateRunning, 2*time.Second)
	waitForProbeCalls(t, &calls, 1, time.Second)

	time.Sleep(150 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("healthy tunnel issued %d probes, want only the startup probe", got)
	}
}

func TestHealthStreamFailureMeansNoInternet(t *testing.T) {
	resetMobileState(t)
	healthFailureLimit = 3
	healthFailureCooldown = time.Millisecond
	healthRetryInitial = 10 * time.Millisecond

	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error { return nil }

	startTUNForHealth(t)
	waitForState(t, StateRunning, 2*time.Second)

	stateMu.Lock()
	ctl := healthCtl
	stateMu.Unlock()
	if ctl == nil {
		t.Fatal("health controller is not active")
	}

	ctl.trigger(healthTrigger{streamFailure: true})
	waitForHealthCode(t, HealthCodeNoInternet, 2*time.Second)
	if GetState() != StateRunning {
		t.Fatalf("transient stream failure must keep the tunnel up, state = %q", GetState())
	}
	if got := GetStateMessage(); got == HealthMessageNoInternet {
		t.Fatalf("fatal no-internet message leaked into state: %q", got)
	}

	ctl.recordInboundActivity()
	waitForState(t, StateRunning, 2*time.Second)
	if health := GetHealth(); !strings.Contains(health, `"internet_verified":true`) {
		t.Fatalf("health does not report internet_verified: %s", health)
	}
	if health := GetHealth(); strings.Contains(health, `"error_code":"`+HealthCodeNoInternet+`"`) {
		t.Fatalf("health kept the stale outage code: %s", health)
	}
}

func TestHealthDoesNotFlagOutageAfterVerifiedTraffic(t *testing.T) {
	resetMobileState(t)
	healthFailureLimit = 3
	healthRetryInitial = 10 * time.Millisecond

	var calls atomic.Int32
	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error {
		calls.Add(1)
		return nil
	}

	startTUNForHealth(t)
	waitForState(t, StateRunning, 2*time.Second)

	stateMu.Lock()
	ctl := healthCtl
	stateMu.Unlock()
	if ctl == nil {
		t.Fatal("health controller is not active")
	}
	ctl.recordInboundActivity()

	before := calls.Load()
	ctl.trigger(healthTrigger{streamFailure: true})
	waitForProbeCalls(t, &calls, before+1, time.Second)
	time.Sleep(20 * time.Millisecond)

	if GetState() != StateRunning {
		t.Fatalf("recent verified traffic must prevent a false outage, state = %q", GetState())
	}
}

func TestNotifyNetworkChangedTriggersProbe(t *testing.T) {
	resetMobileState(t)

	var calls atomic.Int32
	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error {
		calls.Add(1)
		return nil
	}

	startTUNForHealth(t)
	waitForState(t, StateRunning, 2*time.Second)
	waitForProbeCalls(t, &calls, 1, time.Second)
	before := calls.Load()

	NotifyNetworkChanged()
	waitForProbeCalls(t, &calls, before+1, time.Second)
}

func TestStopClientWhileExitUnreachableStopsRuntime(t *testing.T) {
	resetMobileState(t)
	healthFailureLimit = 2
	healthRetryInitial = 10 * time.Millisecond
	healthReconnectMax = 50 * time.Millisecond

	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error {
		return errors.New("exit down")
	}

	bundle := startTUNForHealth(t)
	waitForHealthCode(t, HealthCodeExitUnreachable, 2*time.Second)
	if GetState() == StateError {
		t.Fatalf("transient outage must not produce a fatal state, got %q", GetState())
	}

	StopClient()
	waitForState(t, StateIdle, 3*time.Second)

	if bundle.provider.stopCalls.Load() == 0 || bundle.mux.stopCall.Load() == 0 {
		t.Fatal("health path leaked runtime resources")
	}
	if IsRunning() {
		t.Fatal("client is still running after StopClient")
	}
}

func TestProbeInternetThroughMuxReportsEgressOK(t *testing.T) {
	client, server := net.Pipe()
	inner := &probeMux{open: func(_ context.Context, meta map[string]any) (multiplexing.Stream, error) {
		return &pipeStream{conn: server, id: "net", meta: meta}, nil
	}}
	go func() {
		_, _ = client.Write([]byte(netProbeReply))
		_ = client.Close()
	}()

	supported, err := probeInternetThroughMux(context.Background(), inner)
	if !supported || err != nil {
		t.Fatalf("supported=%v err=%v, want supported and no error", supported, err)
	}
}

func TestProbeInternetThroughMuxReportsEgressFailure(t *testing.T) {
	client, server := net.Pipe()
	inner := &probeMux{open: func(_ context.Context, meta map[string]any) (multiplexing.Stream, error) {
		return &pipeStream{conn: server, id: "net", meta: meta}, nil
	}}
	go func() {
		_, _ = client.Write([]byte(netProbeErrPrefix + "dial failed\n"))
		_ = client.Close()
	}()

	supported, err := probeInternetThroughMux(context.Background(), inner)
	if !supported {
		t.Fatal("egress failure was treated as an unsupported exit node")
	}
	if err == nil || !strings.Contains(err.Error(), "dial failed") {
		t.Fatalf("err = %v, want the exit reason", err)
	}
}

func TestProbeInternetThroughMuxFallsBackOnOldExit(t *testing.T) {
	client, server := net.Pipe()
	inner := &probeMux{open: func(_ context.Context, meta map[string]any) (multiplexing.Stream, error) {
		return &pipeStream{conn: server, id: "net", meta: meta}, nil
	}}
	_ = client.Close()

	supported, err := probeInternetThroughMux(context.Background(), inner)
	if supported || err != nil {
		t.Fatalf("supported=%v err=%v, want an unsupported exit node without an error", supported, err)
	}
}

func TestHealthRequiresInternetProbeWhenSupported(t *testing.T) {
	resetMobileState(t)
	healthFailureLimit = 2
	healthRetryInitial = 10 * time.Millisecond
	healthReconnectMax = 50 * time.Millisecond
	healthProbeInterval = time.Hour

	var egress atomic.Bool
	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error { return nil }
	internetProbe = func(context.Context, multiplexing.StreamMultiplexer) (bool, error) {
		if egress.Load() {
			return true, nil
		}
		return true, errors.New("no egress")
	}

	startTUNForHealth(t)
	time.Sleep(150 * time.Millisecond)
	if GetState() == StateRunning {
		t.Fatal("connected was reported while the exit had no internet")
	}

	egress.Store(true)
	stateMu.Lock()
	ctl := healthCtl
	stateMu.Unlock()
	if ctl == nil {
		t.Fatal("health controller is not active")
	}
	ctl.trigger(healthTrigger{})
	waitForState(t, StateRunning, 2*time.Second)
}

func TestSingleProbeFailureDoesNotResetTransport(t *testing.T) {
	resetMobileState(t)
	healthFailureLimit = 3
	healthRetryInitial = 5 * time.Millisecond

	var calls atomic.Int32
	exitProbe = func(ctx context.Context, _ multiplexing.StreamMultiplexer) error {
		if calls.Add(1) == 1 {
			return errors.New("blip")
		}
		<-ctx.Done()
		return ctx.Err()
	}

	bundle := startTUNForHealth(t)
	time.Sleep(80 * time.Millisecond)
	if got := bundle.transport.resetCalls.Load(); got != 0 {
		t.Fatalf("a single failed probe reset the transport %d times", got)
	}
}

func TestRunningDowngradesToConnectingWhenExitUnreachable(t *testing.T) {
	resetMobileState(t)
	healthFailureLimit = 2
	healthRetryInitial = 10 * time.Millisecond
	healthReconnectMax = 50 * time.Millisecond
	healthProbeInterval = time.Hour

	var down atomic.Bool
	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error {
		if down.Load() {
			return errors.New("exit down")
		}
		return nil
	}

	startTUNForHealth(t)
	waitForState(t, StateRunning, 2*time.Second)

	down.Store(true)
	stateMu.Lock()
	ctl := healthCtl
	stateMu.Unlock()
	if ctl == nil {
		t.Fatal("health controller is not active")
	}
	ctl.trigger(healthTrigger{})
	waitForState(t, StateConnecting, 2*time.Second)
}

func TestTUNNeverReportsRunningWhenExitIsDown(t *testing.T) {
	resetMobileState(t)
	healthFailureLimit = 3
	healthRetryInitial = 10 * time.Millisecond
	healthReconnectMax = 50 * time.Millisecond

	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error {
		return errors.New("exit down")
	}

	listener := newRecordingListener()
	SetStateListener(listener)
	t.Cleanup(func() { SetStateListener(nil) })

	startTUNForHealth(t)
	waitForHealthCode(t, HealthCodeExitUnreachable, 2*time.Second)

	if listener.has(StateRunning) {
		t.Fatal("core reported running while the exit node was unreachable")
	}
	if listener.has(StateError) {
		t.Fatal("core reported a fatal error instead of retrying in the background")
	}
	if got := GetState(); got != StateConnecting {
		t.Fatalf("state = %q, want %q while the exit is still unreachable", got, StateConnecting)
	}
}
