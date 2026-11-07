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

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/control"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/server/manager"

	_ "github.com/Esteviredzu/shampoore-core/internal/multipipe"
	_ "github.com/Esteviredzu/shampoore-core/internal/transport/livekit_datachannel"
	_ "github.com/Esteviredzu/shampoore-core/internal/transport/yandex"
	_ "github.com/Esteviredzu/shampoore-core/internal/transport/yandex_telemost_raw"
	_ "github.com/Esteviredzu/shampoore-core/internal/transport/yandex_telemost_squares"
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

	mgr := manager.New(*configsDir, logger)

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
		*configsDir, ", socket=", *controlSock, ", cli_mode=", cliMode, ")")
	<-ctx.Done()
	logger.Info("server: stopping")
	mgr.Close()
	logger.Info("server: stopped")
}
