package mobile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/geofilter"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/netdial"
	"github.com/Esteviredzu/shampoore/internal/runtime"
	"github.com/Esteviredzu/shampoore/internal/socks5"
	"github.com/Esteviredzu/shampoore/internal/transport"

	_ "github.com/Esteviredzu/shampoore/internal/multipipe"
	_ "github.com/Esteviredzu/shampoore/internal/transport/livekit_datachannel"
	_ "github.com/Esteviredzu/shampoore/internal/transport/raw_ws"
	_ "github.com/Esteviredzu/shampoore/internal/transport/silo"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_raw"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_squares"
)

const (
	StateIdle       = "idle"
	StateStarting   = "starting"
	StateConnecting = "connecting"
	StateRunning    = "running"
	StateStopping   = "stopping"
	StateError      = "error"
)

type StateListener interface {
	OnStateChanged(state string, message string)
}

var (
	lifecycleMu sync.Mutex
	stateMu     sync.Mutex

	state          = StateIdle
	stateMessage   string
	cancel         context.CancelFunc
	done           chan struct{}
	currentRuntime *app.Runtime
	stateListener  StateListener

	tunnelHealth HealthStatus
	healthError  bool
	healthCtl    *healthController
)

var startTUNFD = runtime.StartTUNFD

var (
	statsMu        sync.Mutex
	currentMux     multiplexing.StreamMultiplexer
	statsAgg       *transport.StatsAggregator
	statsJSONCache = transport.NewJSONCache(transport.DefaultStatsCacheTTL)
)

var (
	filterMu     sync.Mutex
	globalFilter *geofilter.Filter
	rulesDirPath string
)

var (
	protectorMu      sync.Mutex
	androidProtector SocketProtector
)

func init() {

	netdial.InstallDefaultTransport()
}

type SocketProtector interface {
	Protect(fd int) bool
}

func SetSocketProtector(p SocketProtector) {
	protectorMu.Lock()
	androidProtector = p
	protectorMu.Unlock()

	if p == nil {
		netdial.SetProtector(nil)
		return
	}
	netdial.SetProtector(func(fd int) bool { return p.Protect(fd) })
}

func ClearSocketProtector() {
	SetSocketProtector(nil)
}

func InitRules(dir string) {
	filterMu.Lock()
	rulesDirPath = dir
	if globalFilter == nil {
		globalFilter = geofilter.NewFilter(geofilter.ActionBypass)
	}
	filter := globalFilter
	filterMu.Unlock()

	if err := filter.LoadDir(dir); err != nil {
		log.Printf("mobile: load rules from %q: %v", dir, err)
	}
}

func GetRules() string {
	filterMu.Lock()
	filter := globalFilter
	filterMu.Unlock()

	if filter == nil {
		return "[]"
	}
	data, err := json.Marshal(filter.Rules())
	if err != nil {
		log.Printf("mobile: marshal rules: %v", err)
		return "[]"
	}
	return string(data)
}

func ReloadRules() string {
	filterMu.Lock()
	filter := globalFilter
	dir := rulesDirPath
	filterMu.Unlock()

	if filter == nil {
		return "[]"
	}
	if dir == "" {
		return GetRules()
	}
	if err := filter.LoadDir(dir); err != nil {
		log.Printf("mobile: reload rules from %q: %v", dir, err)
		return err.Error()
	}
	return GetRules()
}

func SetRuleEnabled(name string, on bool) string {
	filterMu.Lock()
	filter := globalFilter
	filterMu.Unlock()
	if filter == nil {
		return "filter not initialized"
	}
	if err := filter.SetEnabled(name, on); err != nil {
		return err.Error()
	}
	return ""
}

func SetStateListener(listener StateListener) {
	stateMu.Lock()
	stateListener = listener
	st := state
	msg := stateMessage
	stateMu.Unlock()
	if listener != nil {
		listener.OnStateChanged(st, msg)
	}
}

func GetState() string {
	stateMu.Lock()
	defer stateMu.Unlock()
	return state
}

func transitionState(st, msg string) {
	stateMu.Lock()
	state = st
	stateMessage = msg
	if st == StateStarting {
		resetHealthLocked()
	}
	listener := stateListener
	stateMu.Unlock()
	if listener != nil {
		listener.OnStateChanged(st, msg)
	}
}

func setStateIf(current, next, msg string) bool {
	stateMu.Lock()
	if state != current {
		stateMu.Unlock()
		return false
	}
	state = next
	stateMessage = msg
	listener := stateListener
	stateMu.Unlock()
	if listener != nil {
		listener.OnStateChanged(next, msg)
	}
	return true
}

func StartClient(source string) string {
	return startClient(source, runSocks5, 0, 0)
}

func StartClientVPN(source string, fd int, mtu int) string {
	if fd < 0 {
		return "shampoore: invalid TUN fd"
	}
	if mtu <= 0 {
		mtu = 1500
	}
	return startClient(source, runTUN, fd, mtu)
}

type runMode int

const (
	runSocks5 runMode = iota
	runTUN
)

type clientSession struct {
	ctx        context.Context
	cancel     context.CancelFunc
	rt         *app.Runtime
	mux        multiplexing.StreamMultiplexer
	detachMux  multiplexing.StreamMultiplexer
	healthCtl  *healthController
	tunMode    bool
	tunStopped <-chan struct{}
	fatalCh    chan error
	doneCh     chan struct{}
}

func startClient(source string, mode runMode, vpnFD int, vpnMTU int) string {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	stateMu.Lock()
	current := state
	runtimeAlive := currentRuntime != nil
	stateMu.Unlock()
	switch current {
	case StateStarting, StateConnecting, StateRunning, StateStopping:
		return "shampoore: client already running"
	case StateError:
		if runtimeAlive {
			return "shampoore: client already running"
		}
	}

	transitionState(StateStarting, "")
	sess, err := startLocked(source, mode, vpnFD, vpnMTU)
	if err != nil {
		transitionState(StateError, err.Error())
		return "shampoore: " + err.Error()
	}

	next := StateRunning
	if sess.tunMode {
		next = StateConnecting
	}
	if !setStateIf(StateStarting, next, "") {
		stateMu.Lock()
		msg := stateMessage
		stateMu.Unlock()
		if msg == "" {
			msg = "start aborted"
		}
		sess.cancel()
		go monitorRuntime(sess.ctx, sess.rt, sess.mux, sess.detachMux, sess.tunStopped, sess.fatalCh, sess.doneCh, nil)
		return "shampoore: " + msg
	}

	var healthDone <-chan struct{}
	if sess.tunMode && sess.healthCtl != nil {
		sess.healthCtl.start()
		healthDone = sess.healthCtl.done
	}
	go monitorRuntime(sess.ctx, sess.rt, sess.mux, sess.detachMux, sess.tunStopped, sess.fatalCh, sess.doneCh, healthDone)
	return ""
}

func startLocked(source string, mode runMode, vpnFD int, vpnMTU int) (*clientSession, error) {
	s, err := app.ParseSource(source)
	if err != nil {
		return nil, err
	}
	lg := logging.New("mobile", app.LogLevel == "debug")

	filterMu.Lock()
	filter := globalFilter
	filterMu.Unlock()

	ctx, cancelFn := context.WithCancel(context.Background())

	rt, err := app.BuildRuntime(ctx, s, "client", lg)
	if err != nil {
		cancelFn()
		return nil, err
	}

	var mux multiplexing.StreamMultiplexer
	if rt.StreamMux != nil {
		mux = rt.StreamMux
		lg.Info("mobile: using flowmux")
	} else {
		muxCfg := tunnel.Config{
			FlushThreshold: rt.MuxConfig.FlushThreshold,
			FlushInterval:  time.Duration(rt.MuxConfig.FlushMs) * time.Millisecond,
			UserID:         rt.MuxConfig.UserID,
			ExitNodeID:     rt.MuxConfig.ExitNodeID,
		}
		mux = tunnel.New(rt.Transport, lg, muxCfg)
	}

	if err := mux.Start(ctx); err != nil {
		cleanupRuntime(rt, mux, nil, nil)
		cancelFn()
		return nil, err
	}
	if err := rt.Start(ctx); err != nil {
		cleanupRuntime(rt, mux, nil, nil)
		cancelFn()
		return nil, err
	}

	fatalCh := make(chan error, 1)
	var tunDone <-chan error
	var detachMux multiplexing.StreamMultiplexer
	var newHealthCtl *healthController

	switch mode {
	case runSocks5:
		listen := s.EffectiveAdvanced().String("socks5_listen", "127.0.0.1:1080")
		srv := socks5.NewLocalServer(listen, mux, lg, app.IPv4Only)
		srv.SetGeoFilter(filter, false)
		go func() {
			if err := srv.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
				select {
				case fatalCh <- fmt.Errorf("mobile: socks5: %w", err):
				default:
				}
			}
		}()

	case runTUN:
		observer := newHealthObserver(healthFailureLimit)
		ctl := newHealthController(ctx, rt, mux)
		observer.attach(ctl)
		tunMux := newHealthMux(mux, observer)
		detachMux = tunMux
		newHealthCtl = ctl

		runtime.AttachMuxForTUN(tunMux)
		runtime.AttachFilterForTUN(filter)
		stopped, err := startTUNFD(ctx, vpnFD, vpnMTU)
		if err != nil {
			runtime.DetachMuxForTUN(tunMux)
			cleanupRuntime(rt, mux, nil, nil)
			cancelFn()
			return nil, fmt.Errorf("mobile: start TUN: %w", err)
		}
		tunDone = stopped

	default:
		cleanupRuntime(rt, mux, nil, nil)
		cancelFn()
		return nil, fmt.Errorf("mobile: unknown run mode %d", mode)
	}

	statsMu.Lock()
	currentMux = mux
	statsAgg = transport.NewStatsAggregator()
	statsJSONCache.Invalidate()
	statsMu.Unlock()

	doneCh := make(chan struct{})
	stateMu.Lock()
	cancel = cancelFn
	done = doneCh
	currentRuntime = rt
	stateMu.Unlock()

	var tunStopped <-chan struct{}
	if tunDone != nil {
		tunStopped = watchTUN(ctx, tunDone, fatalCh)
	}

	return &clientSession{
		ctx:        ctx,
		cancel:     cancelFn,
		rt:         rt,
		mux:        mux,
		detachMux:  detachMux,
		healthCtl:  newHealthCtl,
		tunMode:    mode == runTUN,
		tunStopped: tunStopped,
		fatalCh:    fatalCh,
		doneCh:     doneCh,
	}, nil
}

func watchTUN(ctx context.Context, tunDone <-chan error, fatalCh chan<- error) <-chan struct{} {
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		err, ok := <-tunDone
		if !ok || ctx.Err() != nil {
			return
		}
		if err == nil {
			err = errors.New("mobile: TUN engine stopped unexpectedly")
		}
		select {
		case fatalCh <- err:
		default:
		}
	}()
	return stopped
}

func cleanupRuntime(rt *app.Runtime, mux, detachMux multiplexing.StreamMultiplexer, tunStopped <-chan struct{}) {
	if tunStopped != nil {
		select {
		case <-tunStopped:
		case <-time.After(3 * time.Second):
		}
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	if rt != nil {
		if err := rt.Stop(stopCtx); err != nil {
			log.Printf("mobile: stop runtime: %v", err)
		}
	}
	if mux != nil {
		if err := mux.Stop(stopCtx); err != nil {
			log.Printf("mobile: stop mux: %v", err)
		}
	}
	detach := detachMux
	if detach == nil {
		detach = mux
	}
	if detach != nil {
		runtime.DetachMuxForTUN(detach)
	}

	statsMu.Lock()
	currentMux = nil
	statsAgg = nil
	statsJSONCache.Invalidate()
	statsMu.Unlock()
}

func monitorRuntime(
	ctx context.Context,
	rt *app.Runtime,
	mux multiplexing.StreamMultiplexer,
	detachMux multiplexing.StreamMultiplexer,
	tunStopped <-chan struct{},
	fatalCh <-chan error,
	doneCh chan struct{},
	healthDone <-chan struct{},
) {
	var fatalErr error
	select {
	case <-ctx.Done():
	case fatalErr = <-fatalCh:
		stateMu.Lock()
		cancelFn := cancel
		stateMu.Unlock()
		if cancelFn != nil {
			cancelFn()
		}
		<-ctx.Done()
	}

	cleanupRuntime(rt, mux, detachMux, tunStopped)
	if healthDone != nil {
		select {
		case <-healthDone:
		case <-time.After(5 * time.Second):
		}
	}

	stateMu.Lock()
	prev := state
	switch {
	case prev == StateStopping:
		state = StateIdle
		stateMessage = ""
	case fatalErr != nil:
		state = StateError
		stateMessage = fatalErr.Error()
	default:
		state = StateError
		stateMessage = "transport stopped unexpectedly"
	}
	currentRuntime = nil
	cancel = nil
	done = nil
	resetHealthLocked()
	next := state
	msg := stateMessage
	listener := stateListener
	stateMu.Unlock()

	if listener != nil {
		listener.OnStateChanged(next, msg)
	}
	close(doneCh)
}

func waitDone(d chan struct{}, timeout time.Duration) {
	if d == nil {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-d:
	case <-timer.C:
	}
}

func StopClient() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	stateMu.Lock()
	switch state {
	case StateIdle:
		stateMu.Unlock()
		return
	case StateError:
		if currentRuntime == nil && cancel == nil {
			state = StateIdle
			stateMessage = ""
			resetHealthLocked()
			listener := stateListener
			stateMu.Unlock()
			if listener != nil {
				listener.OnStateChanged(StateIdle, "")
			}
			return
		}
	case StateStopping:
		d := done
		stateMu.Unlock()
		waitDone(d, 7*time.Second)
		return
	}

	state = StateStopping
	stateMessage = ""
	cancelFn := cancel
	d := done
	listener := stateListener
	stateMu.Unlock()

	if listener != nil {
		listener.OnStateChanged(StateStopping, "")
	}
	if cancelFn != nil {
		cancelFn()
	}
	waitDone(d, 7*time.Second)
}

func IsRunning() bool {
	stateMu.Lock()
	defer stateMu.Unlock()
	switch state {
	case StateStarting, StateConnecting, StateRunning:
		return true
	case StateError:
		return currentRuntime != nil
	}
	return false
}

func NotifyNetworkChanged() {
	stateMu.Lock()
	rt := currentRuntime
	st := state
	ctl := healthCtl
	stateMu.Unlock()
	if rt == nil {
		return
	}
	switch st {
	case StateConnecting, StateRunning, StateError:
	default:
		return
	}
	rt.NotifyNetworkChanged()
	if ctl != nil {
		ctl.trigger(healthTrigger{})
	}
}

type StatsResult struct {
	Running  bool                     `json:"running"`
	Overall  transport.Snapshot       `json:"overall"`
	Rates    transport.Rates          `json:"rates"`
	Subflows []transport.SubflowStats `json:"subflows,omitempty"`
}

func GetStats() string {
	statsMu.Lock()
	mux := currentMux
	agg := statsAgg
	cache := statsJSONCache
	statsMu.Unlock()

	if mux == nil || agg == nil {
		return `{"running":false}`
	}

	tel, ok := mux.(transport.Telemetry)
	if !ok {
		return `{"running":true}`
	}

	data, err := cache.Get(func() ([]byte, error) {
		stats := agg.Update(tel.WalkLayers())
		out := StatsResult{
			Running:  true,
			Overall:  stats.Overall,
			Rates:    stats.Rates,
			Subflows: stats.Subflows,
		}
		return json.Marshal(out)
	})
	if err != nil {
		log.Printf("mobile: marshal stats: %v", err)
		return `{"running":true}`
	}
	return string(data)
}

type TransportField struct {
	Key         string `json:"key"`
	Label       string `json:"label"`
	Placeholder string `json:"placeholder,omitempty"`
	Hint        string `json:"hint,omitempty"`
	Type        string `json:"type"`
	Required    bool   `json:"required,omitempty"`
}

type TransportSchema struct {
	Kind        string           `json:"kind"`
	DisplayName string           `json:"display_name"`
	Description string           `json:"description,omitempty"`
	Fields      []TransportField `json:"fields"`
}

func GetTransportSchemas() string {
	descs := app.AllDescriptors()
	schemas := make([]TransportSchema, 0, len(descs))
	for _, d := range descs {
		fields := make([]TransportField, 0, len(d.Fields))
		for _, f := range d.Fields {
			fields = append(fields, TransportField{
				Key:         f.Key,
				Label:       f.Label,
				Placeholder: f.Placeholder,
				Hint:        f.Hint,
				Type:        string(f.Type),
				Required:    f.Required,
			})
		}
		schemas = append(schemas, TransportSchema{
			Kind:        d.Kind,
			DisplayName: d.DisplayName,
			Description: d.Description,
			Fields:      fields,
		})
	}
	data, _ := json.Marshal(schemas)
	return string(data)
}
