package mobile

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/runtime"
	"github.com/Esteviredzu/shampoore/internal/socks5"
	"github.com/Esteviredzu/shampoore/internal/transport"

	_ "github.com/Esteviredzu/shampoore/internal/multipipe"
	_ "github.com/Esteviredzu/shampoore/internal/transport/livekit_datachannel"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_raw"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_squares"
	_ "github.com/Esteviredzu/shampoore/internal/transport/silo"
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

	var mux multiplexing.StreamMultiplexer
	if rt.StreamMux != nil {
		mux = rt.StreamMux
		lg.Info("mobile: using flowmux")
	} else {
		muxCfg := tunnel.Config{
			FlushThreshold: rt.MuxConfig.FlushThreshold,
			FlushInterval:  time.Duration(rt.MuxConfig.FlushMs) * time.Millisecond,
		}
		mux = tunnel.New(rt.Transport, lg, muxCfg)
	}

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
