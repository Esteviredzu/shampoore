package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Esteviredzu/shampoore/internal/control"
	"github.com/Esteviredzu/shampoore/internal/geofilter"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/panel"

	_ "github.com/Esteviredzu/shampoore/internal/multipipe"
	_ "github.com/Esteviredzu/shampoore/internal/transport/livekit_datachannel"
	_ "github.com/Esteviredzu/shampoore/internal/transport/raw_ws"
	_ "github.com/Esteviredzu/shampoore/internal/transport/silo"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_raw"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_squares"
)

func main() {
	sockPath := flag.String("socket", "/tmp/shampoore.sock", "control socket of shampoore-server")
	listen := flag.String("listen", "127.0.0.1:8080", "panel HTTP listen")
	token := flag.String("token", "", "panel access token; generated and logged when empty")
	configsDir := flag.String("configs-dir", "./configs", "directory with managed configs")
	rulesDir := flag.String("rules-dir", "./rules", "directory with geoip rule files")
	autoStart := flag.Bool("auto-start-server", false,
		"start shampoore-server automatically on panel launch (useful under systemd)")
	flag.Parse()

	logger := logging.New("panel", true)

	tokenValue, err := panel.EnsureToken(*token)
	if err != nil {
		logger.Error("panel: ", err)
		os.Exit(1)
	}
	if strings.TrimSpace(*token) == "" {
		logger.Info("panel: generated access token: ", tokenValue)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	filter := geofilter.NewFilter(geofilter.ActionBlock)
	if err := filter.LoadDir(*rulesDir); err != nil {
		logger.Warn("panel: rules load: ", err)
	}

	sup := panel.NewSupervisor(*sockPath, *configsDir, *rulesDir)
	client := control.NewClient(*sockPath)

	api := panel.New(client, sup, logger, tokenValue, *configsDir, filter, *rulesDir)

	srv := &http.Server{
		Addr:              *listen,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		logger.Info("panel: listening on http://", *listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("panel: ", err)
		}
	}()

	if *autoStart {
		go func() {
			time.Sleep(500 * time.Millisecond)
			if err := sup.Start(); err != nil {
				logger.Warn("panel: auto-start server failed: ", err)
			} else {
				logger.Info("panel: server start initiated")
			}
		}()
	}

	<-ctx.Done()
	logger.Info("panel: stopping")

	shCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
	if err := srv.Shutdown(shCtx); err != nil {
		logger.Warn("panel: shutdown: ", err)
	}
	c()
	logger.Info("panel: stopped")
}
