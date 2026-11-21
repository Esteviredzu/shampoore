package mobile

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/core/channel/vp8"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/runtime"
)

const testSource = `{"type":"mobiletest"}`

type fakeTransport struct {
	startCalls atomic.Int32
	stopCalls  atomic.Int32
	resetCalls atomic.Int32
}

func (t *fakeTransport) Start(context.Context) error { t.startCalls.Add(1); return nil }
func (t *fakeTransport) Stop(context.Context) error  { t.stopCalls.Add(1); return nil }
func (t *fakeTransport) Send(context.Context, []byte) error {
	return nil
}
func (t *fakeTransport) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (t *fakeTransport) MaxMessageSize() int   { return 1200 }
func (t *fakeTransport) BytesDelivered() int64 { return 0 }
func (t *fakeTransport) BytesAcked() int64     { return 0 }
func (t *fakeTransport) NotifyNetworkChanged() { t.resetCalls.Add(1) }

type fakeProvider struct {
	stopCalls atomic.Int32
}

func (p *fakeProvider) Start(context.Context) error { return nil }
func (p *fakeProvider) Stop(context.Context) error  { p.stopCalls.Add(1); return nil }
func (p *fakeProvider) Tunnel() *vp8.Tunnel         { return nil }

type fakeMux struct {
	startErr  error
	startCall atomic.Int32
	stopCall  atomic.Int32
}

func (m *fakeMux) Start(context.Context) error {
	m.startCall.Add(1)
	return m.startErr
}
func (m *fakeMux) Stop(context.Context) error { m.stopCall.Add(1); return nil }
func (m *fakeMux) OpenStream(context.Context, map[string]any) (multiplexing.Stream, error) {
	return nil, errors.New("fakeMux: OpenStream is not implemented")
}
func (m *fakeMux) AcceptStream(context.Context) (multiplexing.Stream, error) {
	return nil, errors.New("fakeMux: AcceptStream is not implemented")
}

type fakeBundle struct {
	transport *fakeTransport
	provider  *fakeProvider
	mux       *fakeMux
}

var (
	fakeBundleMu   sync.Mutex
	fakeBundleNext func() *fakeBundle
)

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "mobiletest",
		DisplayName: "Mobile test",
		Build: func(
			_ context.Context,
			_ *app.Settings,
			_ string,
			_ logging.ContextLogger,
			_ app.Values,
		) (*app.BuildResult, error) {
			fakeBundleMu.Lock()
			fn := fakeBundleNext
			fakeBundleMu.Unlock()
			if fn == nil {
				return nil, errors.New("mobiletest: no bundle configured")
			}
			b := fn()
			if b == nil || b.transport == nil || b.provider == nil || b.mux == nil {
				return nil, errors.New("mobiletest: incomplete bundle")
			}
			return &app.BuildResult{
				Transport: b.transport,
				Provider:  b.provider,
				StreamMux: b.mux,
			}, nil
		},
	})
	if err != nil {
		panic(err)
	}
}

func installFakeBuilder(t *testing.T, fn func() *fakeBundle) {
	t.Helper()
	fakeBundleMu.Lock()
	old := fakeBundleNext
	fakeBundleNext = fn
	fakeBundleMu.Unlock()
	t.Cleanup(func() {
		fakeBundleMu.Lock()
		fakeBundleNext = old
		fakeBundleMu.Unlock()
	})
}

func runFakeTUN(ctx context.Context, _ int, _ int) (<-chan error, error) {
	done := make(chan error, 1)
	go func() {
		<-ctx.Done()
		done <- ctx.Err()
		close(done)
	}()
	return done, nil
}

func resetMobileState(t *testing.T) {
	t.Helper()
	StopClient()

	stateMu.Lock()
	state = StateIdle
	stateMessage = ""
	cancel = nil
	done = nil
	currentRuntime = nil
	stateListener = nil
	tunnelHealth = HealthStatus{}
	healthError = false
	healthCtl = nil
	stateMu.Unlock()

	statsMu.Lock()
	currentMux = nil
	statsAgg = nil
	statsJSONCache.Invalidate()
	statsMu.Unlock()

	filterMu.Lock()
	globalFilter = nil
	rulesDirPath = ""
	filterMu.Unlock()

	healthProbeTimeout = 3 * time.Second
	healthFailureLimit = 3
	healthRetryInitial = time.Second
	healthFailureCooldown = 5 * time.Second
	healthNoInternetGrace = 30 * time.Second
	exitProbe = func(context.Context, multiplexing.StreamMultiplexer) error { return nil }

	startTUNFD = runtime.StartTUNFD
}

func waitForState(t *testing.T, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if GetState() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("state did not become %q, current %q", want, GetState())
}

type recordingListener struct {
	mu     sync.Mutex
	states []string
	last   string
	ch     chan string
}

func newRecordingListener() *recordingListener {
	return &recordingListener{ch: make(chan string, 64)}
}

func (l *recordingListener) OnStateChanged(state, message string) {
	l.mu.Lock()
	l.states = append(l.states, state)
	l.last = state + ":" + message
	l.mu.Unlock()
	select {
	case l.ch <- state:
	default:
	}
}

func (l *recordingListener) has(state string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.states {
		if s == state {
			return true
		}
	}
	return false
}

func waitForStateEvent(t *testing.T, l *recordingListener, want string, timeout time.Duration) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case got := <-l.ch:
			if got == want {
				return
			}
		case <-timer.C:
			t.Fatalf("listener did not receive state %q", want)
		}
	}
}

func TestStateMachineStartStopNotifiesListener(t *testing.T) {
	resetMobileState(t)
	installFakeBuilder(t, func() *fakeBundle {
		return &fakeBundle{
			transport: &fakeTransport{},
			provider:  &fakeProvider{},
			mux:       &fakeMux{},
		}
	})
	startTUNFD = runFakeTUN

	release := make(chan struct{})
	exitProbe = func(ctx context.Context, _ multiplexing.StreamMultiplexer) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	listener := newRecordingListener()
	SetStateListener(listener)
	t.Cleanup(func() { SetStateListener(nil) })

	if got := StartClientVPN(testSource, 0, 1500); got != "" {
		t.Fatalf("StartClientVPN returned %q", got)
	}
	if GetState() != StateConnecting {
		t.Fatalf("expected connecting, got %q", GetState())
	}
	if !listener.has(StateConnecting) {
		t.Fatal("listener did not observe connecting")
	}

	close(release)
	waitForState(t, StateRunning, 2*time.Second)

	StopClient()
	waitForState(t, StateIdle, 3*time.Second)

	for _, want := range []string{StateStarting, StateConnecting, StateRunning, StateStopping, StateIdle} {
		if !listener.has(want) {
			t.Fatalf("listener did not observe %q", want)
		}
	}
	if IsRunning() {
		t.Fatal("client must not be running after StopClient")
	}
}

func TestMuxStartErrorRollsBackRuntime(t *testing.T) {
	resetMobileState(t)
	bundle := &fakeBundle{
		transport: &fakeTransport{},
		provider:  &fakeProvider{},
		mux:       &fakeMux{startErr: errors.New("mux boom")},
	}
	installFakeBuilder(t, func() *fakeBundle { return bundle })
	startTUNFD = runFakeTUN

	got := StartClientVPN(testSource, 0, 1500)
	if !strings.Contains(got, "mux boom") {
		t.Fatalf("expected mux error, got %q", got)
	}
	if GetState() != StateError {
		t.Fatalf("expected error state, got %q", GetState())
	}
	if bundle.provider.stopCalls.Load() == 0 {
		t.Fatal("provider was not stopped after mux.Start failure")
	}
	if bundle.mux.stopCall.Load() == 0 {
		t.Fatal("mux was not stopped after its own Start failure")
	}
}

func TestTUNStartErrorReturnedSynchronously(t *testing.T) {
	resetMobileState(t)
	bundle := &fakeBundle{
		transport: &fakeTransport{},
		provider:  &fakeProvider{},
		mux:       &fakeMux{},
	}
	installFakeBuilder(t, func() *fakeBundle { return bundle })
	startTUNFD = func(context.Context, int, int) (<-chan error, error) {
		return nil, errors.New("tun boom")
	}

	got := StartClientVPN(testSource, 4, 1500)
	if !strings.Contains(got, "tun boom") {
		t.Fatalf("expected synchronous TUN error, got %q", got)
	}
	if GetState() != StateError {
		t.Fatalf("expected error state, got %q", GetState())
	}
	if bundle.provider.stopCalls.Load() == 0 || bundle.mux.stopCall.Load() == 0 {
		t.Fatal("TUN startup failure leaked runtime resources")
	}
	if IsRunning() {
		t.Fatal("client must not be running after a failed TUN start")
	}
}

func TestTUNFDZeroIsValid(t *testing.T) {
	resetMobileState(t)
	installFakeBuilder(t, func() *fakeBundle {
		return &fakeBundle{
			transport: &fakeTransport{},
			provider:  &fakeProvider{},
			mux:       &fakeMux{},
		}
	})
	var gotFD atomic.Int32
	startTUNFD = func(ctx context.Context, fd int, mtu int) (<-chan error, error) {
		gotFD.Store(int32(fd))
		return runFakeTUN(ctx, fd, mtu)
	}

	if got := StartClientVPN(testSource, 0, 1500); got != "" {
		t.Fatalf("fd=0 must be accepted, got %q", got)
	}
	if gotFD.Load() != 0 {
		t.Fatalf("startTUNFD received fd=%d", gotFD.Load())
	}
	StopClient()
	waitForState(t, StateIdle, 3*time.Second)

	if got := StartClientVPN(testSource, -1, 1500); !strings.Contains(got, "invalid TUN fd") {
		t.Fatalf("expected invalid fd error, got %q", got)
	}
}

func TestAsyncTUNFailureNotifiesListener(t *testing.T) {
	resetMobileState(t)
	bundle := &fakeBundle{
		transport: &fakeTransport{},
		provider:  &fakeProvider{},
		mux:       &fakeMux{},
	}
	installFakeBuilder(t, func() *fakeBundle { return bundle })

	tunDone := make(chan error, 1)
	startTUNFD = func(context.Context, int, int) (<-chan error, error) {
		return tunDone, nil
	}

	listener := newRecordingListener()
	SetStateListener(listener)
	t.Cleanup(func() { SetStateListener(nil) })

	if got := StartClientVPN(testSource, 0, 1500); got != "" {
		t.Fatalf("StartClientVPN returned %q", got)
	}

	tunDone <- errors.New("tun died")
	close(tunDone)

	waitForState(t, StateError, 5*time.Second)
	if !listener.has(StateError) {
		t.Fatal("listener did not observe the asynchronous TUN failure")
	}
	if IsRunning() {
		t.Fatal("client must stop after an asynchronous TUN failure")
	}
	if bundle.provider.stopCalls.Load() == 0 || bundle.mux.stopCall.Load() == 0 {
		t.Fatal("asynchronous TUN failure leaked runtime resources")
	}
}

func TestGlobalFilterPointerStableAcrossReloads(t *testing.T) {
	resetMobileState(t)
	dir := t.TempDir()
	writeRule := func(name, cidr string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(cidr+"\n"), 0o644); err != nil {
			t.Fatalf("write rule: %v", err)
		}
	}

	writeRule("a.txt", "10.0.0.0/8")
	InitRules(dir)

	filterMu.Lock()
	first := globalFilter
	filterMu.Unlock()
	if first == nil {
		t.Fatal("filter was not created")
	}

	writeRule("b.txt", "192.168.0.0/16")
	if stale := GetRules(); strings.Contains(stale, `"b"`) {
		t.Fatalf("GetRules reloaded the rules directory behind the caller's back: %s", stale)
	}
	rulesJSON := ReloadRules()
	if !strings.Contains(rulesJSON, `"a"`) || !strings.Contains(rulesJSON, `"b"`) {
		t.Fatalf("ReloadRules did not observe the new rule: %s", rulesJSON)
	}

	filterMu.Lock()
	second := globalFilter
	filterMu.Unlock()
	if first != second {
		t.Fatal("globalFilter pointer was replaced by ReloadRules")
	}

	InitRules(dir)
	filterMu.Lock()
	third := globalFilter
	filterMu.Unlock()
	if first != third {
		t.Fatal("globalFilter pointer was replaced by InitRules")
	}

	if err := first.SetEnabled("a", true); err != nil {
		t.Fatalf("enable rule: %v", err)
	}
	if !first.Match(net.ParseIP("10.1.2.3")) {
		t.Fatal("enabled rule is not visible through the original filter instance")
	}
	if !strings.Contains(GetRules(), `"enabled":true`) {
		t.Fatal("GetRules does not reflect toggled rule state")
	}
}

func TestNotifyNetworkChangedReachesActiveTransport(t *testing.T) {
	resetMobileState(t)
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
	NotifyNetworkChanged()
	if bundle.transport.resetCalls.Load() != 1 {
		t.Fatalf("transport received %d network-change notifications", bundle.transport.resetCalls.Load())
	}

	StopClient()
	waitForState(t, StateIdle, 3*time.Second)

	NotifyNetworkChanged()
	if bundle.transport.resetCalls.Load() != 1 {
		t.Fatal("NotifyNetworkChanged must be a no-op while idle")
	}
}

func TestConcurrentStartStop(t *testing.T) {
	resetMobileState(t)
	installFakeBuilder(t, func() *fakeBundle {
		return &fakeBundle{
			transport: &fakeTransport{},
			provider:  &fakeProvider{},
			mux:       &fakeMux{},
		}
	})
	startTUNFD = runFakeTUN

	const workers = 8
	const rounds = 25
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				if (worker+j)%2 == 0 {
					_ = StartClientVPN(testSource, 0, 1500)
				} else {
					StopClient()
				}
			}
		}(i)
	}
	wg.Wait()
	StopClient()
	waitForState(t, StateIdle, 5*time.Second)

	stateMu.Lock()
	rt := currentRuntime
	cancelFn := cancel
	ch := done
	stateMu.Unlock()
	if rt != nil || cancelFn != nil || ch != nil {
		t.Fatal("lifecycle state was not cleared after concurrent Start/Stop")
	}
}
