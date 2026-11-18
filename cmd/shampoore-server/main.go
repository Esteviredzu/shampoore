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

	_ "github.com/Esteviredzu/shampoore/internal/multipipe"
	_ "github.com/Esteviredzu/shampoore/internal/transport/livekit_datachannel"
	_ "github.com/Esteviredzu/shampoore/internal/transport/raw_ws"
	_ "github.com/Esteviredzu/shampoore/internal/transport/silo"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_raw"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_squares"
)

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ", ") }
func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
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

	var configFiles stringList
	flag.Var(&configFiles, "c",
		"config file to run (repeatable; CLI mode, starts every file)")

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
		logger.Info("server: rule loaded: ", r.Name, " cidrs=", len(r.CIDRs), " enabled=", r.Enabled)
	}

	mgr := manager.New(*configsDir, logger, filter, *blockUnknown)

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
		", rules=", len(filter.Rules()), ", block_unknown=", *blockUnknown, ")")
	<-ctx.Done()
	logger.Info("server: stopping")
	mgr.Close()
	logger.Info("server: stopped")
}
