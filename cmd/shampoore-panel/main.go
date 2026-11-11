package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Esteviredzu/shampoore/internal/control"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/panel"

	_ "github.com/Esteviredzu/shampoore/internal/multipipe"
	_ "github.com/Esteviredzu/shampoore/internal/transport/livekit_datachannel"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_raw"
	_ "github.com/Esteviredzu/shampoore/internal/transport/yandex_telemost_squares"
)

func main() {
	sockPath := flag.String("socket", "/tmp/shampoore.sock", "control socket of shampoore-server")
	listen := flag.String("listen", "127.0.0.1:8080", "panel HTTP listen")
	token := flag.String("token", "", "optional X-Panel-Token for /api/*")
	configsDir := flag.String("configs-dir", "./configs", "directory with managed configs")
	autoStart := flag.Bool("auto-start-server", false,
		"start shampoore-server automatically on panel launch (useful under systemd)")
	flag.Parse()

	logger := logging.New("panel", true)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	sup := panel.NewSupervisor(*sockPath, *configsDir)
	client := control.NewClient(*sockPath)

	api := panel.New(client, sup, logger, *token, *configsDir)

	srv := &http.Server{Addr: *listen, Handler: api.Handler()}
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
				logger.Info("panel: server auto-started")
			}
		}()
	}

	<-ctx.Done()
	logger.Info("panel: stopping")

	shCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
	_ = srv.Shutdown(shCtx)
	c()
	logger.Info("panel: stopped")
}
