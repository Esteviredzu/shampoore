package mobile

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/multiplexing"
	"github.com/Esteviredzu/shampoore-core/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore-core/internal/runtime"
	"github.com/Esteviredzu/shampoore-core/internal/socks5"
	"github.com/Esteviredzu/shampoore-core/internal/transport"

	_ "github.com/Esteviredzu/shampoore-core/internal/multipipe"
	_ "github.com/Esteviredzu/shampoore-core/internal/transport/livekit_datachannel"
	_ "github.com/Esteviredzu/shampoore-core/internal/transport/yandex"
	_ "github.com/Esteviredzu/shampoore-core/internal/transport/yandex_telemost_raw"
	_ "github.com/Esteviredzu/shampoore-core/internal/transport/yandex_telemost_squares"
)

var (
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
)

var (
	statsMu    sync.Mutex
	currentMux multiplexing.StreamMultiplexer
	statsAgg   *transport.StatsAggregator
)

func StartClient(source string) string {
	mu.Lock()
	defer mu.Unlock()
	if cancel != nil {
		return "shampoore: client already running"
	}
	if err := startLocked(source, runSocks5, 0, 0); err != nil {
		return "shampoore: " + err.Error()
	}
	return ""
}

func StartClientVPN(source string, fd int, mtu int) string {
	mu.Lock()
	defer mu.Unlock()
	if cancel != nil {
		return "shampoore: client already running"
	}
	if fd <= 0 {
		return "shampoore: invalid TUN fd"
	}
	if err := startLocked(source, runTUN, fd, mtu); err != nil {
		return "shampoore: " + err.Error()
	}
	return ""
}

type runMode int

const (
	runSocks5 runMode = iota
	runTUN
)

func startLocked(source string, mode runMode, vpnFD int, vpnMTU int) error {
	s, err := app.ParseSource(source)
	if err != nil {
		return err
	}
	lg := logging.New("mobile", app.LogLevel == "debug")

	ctx, cancelFn := context.WithCancel(context.Background())

	rt, err := app.BuildRuntime(ctx, s, "client", lg)
	if err != nil {
		cancelFn()
		return err
	}
	muxCfg := tunnel.Config{
		FlushThreshold: rt.MuxConfig.FlushThreshold,
		FlushInterval:  time.Duration(rt.MuxConfig.FlushMs) * time.Millisecond,
	}

	mux := tunnel.New(rt.Transport, lg, muxCfg)

	if err := mux.Start(ctx); err != nil {
		cancelFn()
		return err
	}
	if err := rt.Start(ctx); err != nil {
		_ = mux.Stop(context.Background())
		cancelFn()
		return err
	}

	var tunDone chan struct{}

	switch mode {
	case runSocks5:
		listen := s.EffectiveAdvanced().String("socks5_listen", "127.0.0.1:1080")
		srv := socks5.NewLocalServer(listen, mux, lg, app.IPv4Only)
		go func() { _ = srv.ListenAndServe(ctx) }()

	case runTUN:
		if vpnMTU <= 0 {
			vpnMTU = 1500
		}
		tunDone = make(chan struct{})
		runtime.AttachMuxForTUN(mux)
		go func() {
			defer close(tunDone)
			if err := runtime.StartTUNFD(ctx, vpnFD, vpnMTU); err != nil {
				lg.Warn("mobile: TUN stopped: ", err)
			}
		}()

	default:
		_ = mux.Stop(context.Background())
		cancelFn()
		return fmt.Errorf("mobile: unknown run mode %d", mode)
	}

	statsMu.Lock()
	currentMux = mux
	statsAgg = transport.NewStatsAggregator()
	statsMu.Unlock()

	cancel = cancelFn
	doneCh := make(chan struct{})
	done = doneCh
	go func() {
		<-ctx.Done()
		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStop()
		_ = rt.Stop(stopCtx)
		_ = mux.Stop(stopCtx)
		if tunDone != nil {
			select {
			case <-tunDone:
			case <-time.After(3 * time.Second):
			}
		}
		close(doneCh)
	}()
	return nil
}

func StopClient() {
	mu.Lock()
	defer mu.Unlock()

	cancelFn := cancel
	d := done
	cancel = nil
	done = nil

	statsMu.Lock()
	currentMux = nil
	statsAgg = nil
	statsMu.Unlock()

	if cancelFn == nil {
		return
	}
	cancelFn()
	if d != nil {
		select {
		case <-d:
		case <-time.After(7 * time.Second):
		}
	}
}

func IsRunning() bool {
	mu.Lock()
	defer mu.Unlock()
	return cancel != nil
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
	statsMu.Unlock()

	if mux == nil || agg == nil {
		data, _ := json.Marshal(StatsResult{Running: false})
		return string(data)
	}

	tel, ok := mux.(transport.Telemetry)
	if !ok {
		data, _ := json.Marshal(StatsResult{Running: true})
		return string(data)
	}

	stats := agg.Update(tel.WalkLayers())
	out := StatsResult{
		Running:  true,
		Overall:  stats.Overall,
		Rates:    stats.Rates,
		Subflows: stats.Subflows,
	}
	data, err := json.Marshal(out)
	if err != nil {
		empty, _ := json.Marshal(StatsResult{Running: true})
		return string(empty)
	}
	return string(data)
}