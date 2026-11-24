package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/control"
	"github.com/Esteviredzu/shampoore/internal/geofilter"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/server/manager"
	"github.com/Esteviredzu/shampoore/internal/socks5"

	_ "github.com/Esteviredzu/shampoore/internal/transport/all"
)

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ", ") }
func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func parseExitUpstreams(items []string) (map[string]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(items))
	for _, item := range items {
		id, raw, ok := strings.Cut(item, "=")
		id = strings.TrimSpace(id)
		raw = strings.TrimSpace(raw)
		if !ok || id == "" || raw == "" {
			return nil, fmt.Errorf("invalid %q (want id=upstream)", item)
		}
		out[id] = raw
	}
	return out, nil
}

func main() {
	configsDir := flag.String("configs-dir", "./configs",
		"directory with configs (used in both CLI and panel mode)")
	controlSock := flag.String("control-socket", "",
		"unix socket for panel (empty = CLI mode, no state management)")
	rulesDir := flag.String("rules-dir", "./rules",
		"directory with geoip rule files (.txt or .json)")
	blockUnknown := flag.Bool("block-unknown", false,
		"block traffic that does not match any enabled rule")
	socksListen := flag.String("socks-listen", "",
		"optional local SOCKS5 listen address (always loopback; empty = disabled)")

	var configFiles stringList
	flag.Var(&configFiles, "c",
		"config file to run (repeatable; CLI mode, starts every file)")

	var exitUpstreams stringList
	flag.Var(&exitUpstreams, "exit-upstream",
		"exit node upstream: id=socks5://host:port or id=local (repeatable)")

	encode := flag.Bool("encode", false,
		"encode config as shampoore:// URL and exit (needs -c FILE)")
	flag.Parse()

	if *encode {
		if len(configFiles) == 0 {
			log.Fatal("encode: need -c config.json")
		}
		if len(configFiles) > 1 {
			log.Printf("encode: using first file only: %s", configFiles[0])
		}
		s, err := app.Load(configFiles[0])
		if err != nil {
			log.Fatalf("load: %v", err)
		}
		link, err := app.EncodeURL(s)
		if err != nil {
			log.Fatalf("encode: %v", err)
		}
		fmt.Println(link)
		return
	}

	debug := app.LogLevel == "debug" || app.LogLevel == "trace"
	logger := logging.New("server", debug)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	filter := geofilter.NewFilter(geofilter.ActionBlock)
	if err := filter.LoadDir(*rulesDir); err != nil {
		logger.Warn("server: rules load: ", err)
	}

	for _, r := range filter.Rules() {
		logger.Info("server: rule loaded: ", r.Name, " cidrs=", len(r.CIDRs), " enabled=", r.Enabled)
	}

	mgr := manager.New(*configsDir, logger, filter, *blockUnknown)
	upstreamMap, err := parseExitUpstreams(exitUpstreams)
	if err != nil {
		log.Fatalf("exit-upstream: %v", err)
	}
	mgr.SetExitUpstreams(upstreamMap)

	cliMode := len(configFiles) > 0 || *controlSock == ""
	if cliMode {
		mgr.SetStartAll(true)
	}

	switch {
	case len(configFiles) > 0:
		logger.Info("server: CLI mode, loading ", len(configFiles), " config file(s)")
		if err := mgr.LoadFiles(ctx, configFiles); err != nil {
			log.Fatalf("load files: %v", err)
		}
	case cliMode:
		logger.Info("server: CLI mode, loading directory ", *configsDir,
			" (all configs will be started, .state.json ignored)")
		if err := mgr.Load(ctx); err != nil {
			log.Fatalf("manager load: %v", err)
		}
	default:
		logger.Info("server: panel mode, loading directory ", *configsDir,
			" (state file controls what to start)")
		if err := mgr.Load(ctx); err != nil {
			log.Fatalf("manager load: %v", err)
		}
	}

	if listen := socks5.NormalizeListen(*socksListen); listen != "" {
		socks := socks5.NewLocalServer(listen, nil, logger, app.IPv4Only)
		socks.SetGeoFilter(filter, *blockUnknown)
		go func() {
			logger.Info("server: direct SOCKS5 listening on ", listen)
			if err := socks.ListenAndServe(ctx); err != nil {
				logger.Warn("server: socks5: ", err)
			}
		}()
		defer socks.Stop()
	}

	var ctrl *control.Server
	if *controlSock != "" {
		ctrl = control.NewServer(*controlSock, mgr, logger, cancel)
		if err := ctrl.Start(ctx); err != nil {
			log.Fatalf("control start: %v", err)
		}
		defer ctrl.Stop()
	}

	logger.Info("server: ready (", len(mgr.List()), " configs, dir=",
		*configsDir, ", socket=", *controlSock, ", cli_mode=", cliMode,
		", rules=", len(filter.Rules()), ", rules_dir=", *rulesDir,
		", geo_action=", filter.Action(), ", block_unknown=", *blockUnknown, ")")
	<-ctx.Done()
	logger.Info("server: stopping")
	mgr.Close()
	logger.Info("server: stopped")
}
