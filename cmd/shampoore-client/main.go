package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/geofilter"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/runtime"
	"github.com/Esteviredzu/shampoore/internal/socks5"

	_ "github.com/Esteviredzu/shampoore/internal/transport/all"
)

func main() {
	configPath := flag.String("c", "config.json", "config file or shampoore:// URL")
	modeFlag := flag.String("m", "socks5",
		"mode: socks5 (local SOCKS5 proxy only) | tun (full VPN)")
	rulesDir := flag.String("rules-dir", "./rules",
		"directory with geoip rule files (.txt or .json); matching IPs bypass VPN")
	flag.Parse()

	mode := app.Mode(*modeFlag)
	switch mode {
	case app.ModeSOCKS5, app.ModeTUN:
	default:
		log.Fatalf("mode: invalid %q (want socks5|tun)", *modeFlag)
	}

	s, err := app.ParseSource(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	debug := app.LogLevel == "debug" || app.LogLevel == "trace"
	lg := logging.New("client", debug)

	filter := geofilter.NewFilter(geofilter.ActionBypass)
	if err := filter.LoadDir(*rulesDir); err != nil {
		lg.Warn("client: rules load: ", err)
	}

	hasEnabled := false
	for _, r := range filter.Rules() {
		if r.Enabled {
			hasEnabled = true
			break
		}
	}
	if !hasEnabled {
		filter.EnableAll()
	}

	for _, r := range filter.Rules() {
		lg.Info("client: rule loaded: ", r.Name, " cidrs=", len(r.CIDRs), " enabled=", r.Enabled)
	}

	ctx, cancel := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)
	defer cancel()

	rt, err := app.BuildRuntime(ctx, s, "client", lg)
	if err != nil {
		log.Fatalf("build runtime: %v", err)
	}

	var mux multiplexing.StreamMultiplexer
	if rt.StreamMux != nil {
		mux = rt.StreamMux
		lg.Info("client: using stream mux (mpquic or flowmux)")
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
		log.Fatalf("mux start: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		log.Fatalf("runtime start: %v", err)
	}

	eff := s.EffectiveAdvanced()

	switch mode {
	case app.ModeSOCKS5:
		listen := socks5.NormalizeListen(eff.String("socks5_listen", "127.0.0.1:1080"))
		srv := socks5.NewLocalServer(listen, mux, lg, app.IPv4Only)
		srv.SetGeoFilter(filter, false)
		go func() {
			if err := srv.ListenAndServe(ctx); err != nil {
				log.Printf("socks5 stopped: %v", err)
			}
		}()

	case app.ModeTUN:
		go func() {
			if err := runtime.StartClientTUN(ctx, s, rt, mux); err != nil {
				log.Printf("tun stopped: %v", err)
			}
		}()
		println("tun enabled on", eff.String("tun_name", "shampoore0"))
	}

	println("client: ready")
	<-ctx.Done()
	println("client: stopping")

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()

	_ = rt.Stop(stopCtx)
	_ = mux.Stop(stopCtx)

	println("client: stopped")
}
