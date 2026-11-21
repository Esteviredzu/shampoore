package mobile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	HealthCodeExitUnreachable = "exit_unreachable"

	HealthCodeNoInternet = "no_internet_through_exit"

	HealthMessageExitUnreachable = "Exit node is unavailable"
	HealthMessageNoInternet      = "Tunnel is up but no internet through exit node"

	exitProbeMetadataKey = "probe"
	exitProbeMetadataVal = "exit"
	exitProbeReply       = "ok\n"
)

var (
	healthProbeTimeout    = 3 * time.Second
	healthFailureLimit    = 3
	healthRetryInitial    = time.Second
	healthFailureCooldown = 5 * time.Second
	healthNoInternetGrace = 30 * time.Second

	exitProbe = probeExitThroughMux
)

type HealthStatus struct {
	State               string `json:"state"`
	Message             string `json:"message"`
	TunnelUp            bool   `json:"tunnel_up"`
	ExitReachable       bool   `json:"exit_reachable"`
	InternetVerified    bool   `json:"internet_verified"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	ErrorCode           string `json:"error_code,omitempty"`
	LastError           string `json:"last_error,omitempty"`
	LastProbeUnix       int64  `json:"last_probe_unix,omitempty"`
	LastSuccessUnix     int64  `json:"last_success_unix,omitempty"`
	LastActivityUnix    int64  `json:"last_activity_unix,omitempty"`
}

func probeExitThroughMux(ctx context.Context, mux multiplexing.StreamMultiplexer) error {
	if mux == nil {
		return errors.New("health: multiplexer is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	stream, err := mux.OpenStream(ctx, map[string]any{
		exitProbeMetadataKey: exitProbeMetadataVal,
	})
	if err != nil {
		return fmt.Errorf("health: open exit probe: %w", err)
	}
	defer stream.Close()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = stream.Close()
		case <-done:
		}
	}()
	defer close(done)

	buf := make([]byte, len(exitProbeReply))
	if _, err := io.ReadFull(stream, buf); err != nil {
		return fmt.Errorf("health: read exit probe: %w", err)
	}
	if string(buf) != exitProbeReply {
		return fmt.Errorf("health: unexpected exit probe reply %q", string(buf))
	}
	return nil
}

func GetHealth() string {
	stateMu.Lock()
	status := tunnelHealth
	status.State = state
	status.Message = stateMessage
	stateMu.Unlock()

	data, err := json.Marshal(status)
	if err != nil {
		log.Printf("mobile: marshal health: %v", err)
		return `{"state":"error","message":"health status unavailable"}`
	}
	return string(data)
}

func GetStateMessage() string {
	stateMu.Lock()
	defer stateMu.Unlock()
	return stateMessage
}

func IsConnected() bool {
	return GetState() == StateRunning
}

func VerifyConnection() {
	stateMu.Lock()
	ctl := healthCtl
	stateMu.Unlock()
	if ctl != nil {
		ctl.trigger(healthTrigger{})
	}
}

func resetHealthLocked() {
	tunnelHealth = HealthStatus{}
	healthError = false
	healthCtl = nil
}

func healthStateActiveLocked() bool {
	switch state {
	case StateConnecting, StateRunning, StateError:
		return true
	}
	return false
}

type healthTrigger struct {
	streamFailure bool
}

type healthController struct {
	ctx          context.Context
	rt           *app.Runtime
	mux          multiplexing.StreamMultiplexer
	probe        func(context.Context, multiplexing.StreamMultiplexer) error
	timeout      time.Duration
	failureLimit int
	retryInitial time.Duration

	triggerCh chan healthTrigger
	done      chan struct{}

	mu       sync.Mutex
	failures int
}

func newHealthController(ctx context.Context, rt *app.Runtime, mux multiplexing.StreamMultiplexer) *healthController {
	timeout := healthProbeTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	limit := healthFailureLimit
	if limit <= 0 {
		limit = 3
	}
	retryInitial := healthRetryInitial
	if retryInitial <= 0 {
		retryInitial = time.Second
	}
	return &healthController{
		ctx:          ctx,
		rt:           rt,
		mux:          mux,
		probe:        exitProbe,
		timeout:      timeout,
		failureLimit: limit,
		retryInitial: retryInitial,
		triggerCh:    make(chan healthTrigger, 4),
		done:         make(chan struct{}),
	}
}

func (c *healthController) start() {
	stateMu.Lock()
	tunnelHealth.TunnelUp = true
	healthCtl = c
	stateMu.Unlock()
	go c.run()
}

func (c *healthController) trigger(t healthTrigger) {
	select {
	case c.triggerCh <- t:
	default:
	}
}

func (c *healthController) run() {
	defer close(c.done)

	var timer *time.Timer
	var timerC <-chan time.Time
	disarm := func() {
		if timer == nil {
			timerC = nil
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
	}
	arm := func(d time.Duration) {
		disarm()
		if timer == nil {
			timer = time.NewTimer(d)
		} else {
			timer.Reset(d)
		}
		timerC = timer.C
	}
	defer func() {
		disarm()
		if timer != nil {
			timer.Stop()
		}
	}()

	arm(0)
	for {
		var trigger healthTrigger
		select {
		case <-c.ctx.Done():
			return
		case trigger = <-c.triggerCh:
			disarm()
		case <-timerC:
			timerC = nil
		}

		err, canceled := c.probeOnce()
		if canceled {
			return
		}

		if err == nil {
			c.mu.Lock()
			c.failures = 0
			c.mu.Unlock()
			if trigger.streamFailure {
				c.recordNoInternet()
			} else {
				c.recordProbeSuccess()
			}
			continue
		}

		c.mu.Lock()
		c.failures++
		failures := c.failures
		c.mu.Unlock()
		c.recordProbeFailure(failures, err)

		if failures < c.failureLimit {
			arm(c.retryInitial)
		}
	}
}

func (c *healthController) probeOnce() (error, bool) {
	ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
	err := c.probe(ctx, c.mux)
	cancel()
	if c.ctx.Err() != nil {
		return nil, true
	}
	if err != nil {
		log.Printf("mobile: exit health probe failed: %v", err)
	}
	return err, false
}

func (c *healthController) recordProbeSuccess() {
	now := time.Now().Unix()

	stateMu.Lock()
	if currentRuntime != c.rt || !healthStateActiveLocked() {
		stateMu.Unlock()
		return
	}
	tunnelHealth.TunnelUp = true
	tunnelHealth.ExitReachable = true
	tunnelHealth.LastProbeUnix = now
	tunnelHealth.LastSuccessUnix = now
	tunnelHealth.ConsecutiveFailures = 0
	tunnelHealth.LastError = ""

	recover := state == StateConnecting ||
		(state == StateError && healthError && tunnelHealth.ErrorCode == HealthCodeExitUnreachable)
	if tunnelHealth.ErrorCode != HealthCodeNoInternet {
		tunnelHealth.ErrorCode = ""
	}
	if recover {
		state = StateRunning
		stateMessage = ""
		healthError = false
	}
	listener := stateListener
	stateMu.Unlock()

	if recover && listener != nil {
		listener.OnStateChanged(StateRunning, "")
	}
}

func (c *healthController) recordProbeFailure(failures int, probeErr error) {
	now := time.Now().Unix()

	stateMu.Lock()
	if currentRuntime != c.rt || !healthStateActiveLocked() {
		stateMu.Unlock()
		return
	}
	tunnelHealth.TunnelUp = true
	tunnelHealth.LastProbeUnix = now
	tunnelHealth.ConsecutiveFailures = failures
	if probeErr != nil {
		tunnelHealth.LastError = probeErr.Error()
	}

	changed := false
	if failures >= c.failureLimit {
		tunnelHealth.ExitReachable = false
		if state != StateError || stateMessage != HealthMessageExitUnreachable {
			state = StateError
			stateMessage = HealthMessageExitUnreachable
			tunnelHealth.ErrorCode = HealthCodeExitUnreachable
			healthError = true
			changed = true
		}
	}
	listener := stateListener
	stateMu.Unlock()

	if changed && listener != nil {
		listener.OnStateChanged(StateError, HealthMessageExitUnreachable)
	}
}

func (c *healthController) recordNoInternet() {
	nowTime := time.Now()
	now := nowTime.Unix()

	stateMu.Lock()
	if currentRuntime != c.rt || !healthStateActiveLocked() {
		stateMu.Unlock()
		return
	}

	if tunnelHealth.InternetVerified && tunnelHealth.LastActivityUnix > 0 &&
		nowTime.Sub(time.Unix(tunnelHealth.LastActivityUnix, 0)) < healthNoInternetGrace {
		stateMu.Unlock()
		return
	}
	tunnelHealth.TunnelUp = true
	tunnelHealth.ExitReachable = true
	tunnelHealth.InternetVerified = false
	tunnelHealth.LastProbeUnix = now
	tunnelHealth.LastSuccessUnix = now
	tunnelHealth.ConsecutiveFailures = 0
	tunnelHealth.ErrorCode = HealthCodeNoInternet
	tunnelHealth.LastError = "user traffic failed without a response"

	changed := state != StateError || stateMessage != HealthMessageNoInternet
	if changed {
		state = StateError
		stateMessage = HealthMessageNoInternet
		healthError = true
	}
	listener := stateListener
	stateMu.Unlock()

	if changed && listener != nil {
		listener.OnStateChanged(StateError, HealthMessageNoInternet)
	}
}

func (c *healthController) recordInboundActivity() {
	c.mu.Lock()
	c.failures = 0
	c.mu.Unlock()

	now := time.Now().Unix()

	stateMu.Lock()
	if currentRuntime != c.rt || !healthStateActiveLocked() {
		stateMu.Unlock()
		return
	}
	tunnelHealth.TunnelUp = true
	tunnelHealth.ExitReachable = true
	tunnelHealth.InternetVerified = true
	tunnelHealth.LastActivityUnix = now
	tunnelHealth.ConsecutiveFailures = 0
	tunnelHealth.LastError = ""
	tunnelHealth.ErrorCode = ""

	recover := state == StateConnecting || state == StateError
	if recover {
		state = StateRunning
		stateMessage = ""
		healthError = false
	}
	listener := stateListener
	stateMu.Unlock()

	if recover && listener != nil {
		listener.OnStateChanged(StateRunning, "")
	}
}

type healthSink interface {
	inbound(int)
	streamFailure(error)
}

type healthObserver struct {
	failureLimit int
	cooldown     time.Duration
	ctl          atomic.Pointer[healthController]

	mu          sync.Mutex
	streak      int
	lastTrigger time.Time
	lastInbound time.Time
	verified    bool
}

func newHealthObserver(failureLimit int) *healthObserver {
	if failureLimit <= 0 {
		failureLimit = 3
	}
	cooldown := healthFailureCooldown
	if cooldown < 0 {
		cooldown = 0
	}
	return &healthObserver{failureLimit: failureLimit, cooldown: cooldown}
}

func (o *healthObserver) attach(c *healthController) {
	o.ctl.Store(c)
}

func (o *healthObserver) inbound(int) {
	now := time.Now()
	o.mu.Lock()
	o.streak = 0
	notify := !o.verified || now.Sub(o.lastInbound) >= time.Second
	if notify {
		o.verified = true
		o.lastInbound = now
	}
	o.mu.Unlock()

	if notify {
		if c := o.ctl.Load(); c != nil {
			c.recordInboundActivity()
		}
	}
}

func (o *healthObserver) streamFailure(err error) {
	now := time.Now()
	trigger := false

	o.mu.Lock()
	o.streak++
	forced := errors.Is(err, transport.ErrTransportReset)
	if (forced || o.streak >= o.failureLimit) && now.Sub(o.lastTrigger) >= o.cooldown {
		o.lastTrigger = now
		o.streak = 0
		o.verified = false
		trigger = true
	}
	o.mu.Unlock()

	if trigger {
		if c := o.ctl.Load(); c != nil {
			c.trigger(healthTrigger{streamFailure: true})
		}
	}
}

type healthMux struct {
	inner multiplexing.StreamMultiplexer
	sink  healthSink
}

func newHealthMux(inner multiplexing.StreamMultiplexer, sink healthSink) *healthMux {
	return &healthMux{inner: inner, sink: sink}
}

func (m *healthMux) Start(ctx context.Context) error { return m.inner.Start(ctx) }
func (m *healthMux) Stop(ctx context.Context) error  { return m.inner.Stop(ctx) }

func (m *healthMux) OpenStream(ctx context.Context, metadata map[string]any) (multiplexing.Stream, error) {
	stream, err := m.inner.OpenStream(ctx, metadata)
	if err != nil {
		m.sink.streamFailure(err)
		return nil, err
	}
	return &healthStream{Stream: stream, sink: m.sink}, nil
}

func (m *healthMux) AcceptStream(ctx context.Context) (multiplexing.Stream, error) {
	stream, err := m.inner.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &healthStream{Stream: stream, sink: m.sink}, nil
}

func (m *healthMux) WalkLayers() []transport.Layer {
	if tw, ok := m.inner.(transport.Telemetry); ok {
		return tw.WalkLayers()
	}
	return nil
}

func (m *healthMux) SessionIdentity() (string, string) {
	if si, ok := m.inner.(interface {
		SessionIdentity() (string, string)
	}); ok {
		return si.SessionIdentity()
	}
	return "", ""
}

func (m *healthMux) NotifyNetworkChanged() {
	if n, ok := m.inner.(interface{ NotifyNetworkChanged() }); ok {
		n.NotifyNetworkChanged()
	}
}

type healthStream struct {
	multiplexing.Stream
	sink       healthSink
	userClosed atomic.Bool
	sawData    atomic.Bool
	lastReport atomic.Int64
}

func (s *healthStream) Read(p []byte) (int, error) {
	n, err := s.Stream.Read(p)
	if n > 0 {
		s.sawData.Store(true)
		now := time.Now().UnixNano()
		prev := s.lastReport.Load()
		if prev == 0 || now-prev >= int64(time.Second) {
			if s.lastReport.CompareAndSwap(prev, now) {
				s.sink.inbound(n)
			}
		}
	}
	if err != nil && !s.userClosed.Load() {
		if errors.Is(err, transport.ErrTransportReset) || !s.sawData.Load() {
			s.sink.streamFailure(err)
		}
	}
	return n, err
}

func (s *healthStream) Write(p []byte) (int, error) {
	n, err := s.Stream.Write(p)
	if err != nil && !s.userClosed.Load() {
		s.sink.streamFailure(err)
	}
	return n, err
}

func (s *healthStream) Close() error {
	s.userClosed.Store(true)
	return s.Stream.Close()
}
