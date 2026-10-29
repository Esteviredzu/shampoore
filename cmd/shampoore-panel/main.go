package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/control"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/panel"

	_ "github.com/Esteviredzu/shampoore-core/internal/transport/yandex"
	_ "github.com/Esteviredzu/shampoore-core/internal/transport/yandex_telemost_raw"
	_ "github.com/Esteviredzu/shampoore-core/internal/transport/yandex_telemost_squares"
)

func main() {
	sockPath := flag.String("socket", "/tmp/shampoore.sock", "control socket of shampoore-server")
	listen := flag.String("listen", "127.0.0.1:8080", "panel HTTP listen")
	token := flag.String("token", "", "optional X-Panel-Token for /api/*")
	configsDir := flag.String("configs-dir", "./configs", "directory with managed configs")
	examplesDir := flag.String("examples", "./examples", "directory with template examples")
	flag.Parse()

	logger := logging.New("panel", true)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	sup := panel.NewSupervisor(*sockPath, *configsDir)
	client := control.NewClient(*sockPath)
	
	api := panel.New(client, sup, logger, *token, *examplesDir, *configsDir)
	
	srv := &http.Server{Addr: *listen, Handler: api.Handler()}
	go func() {
		logger.Info("panel: listening on http://", *listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("panel: ", err)
		}
	}()

	<-ctx.Done()
	logger.Info("panel: stopping")

	shCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
	_ = srv.Shutdown(shCtx)
	c()
	logger.Info("panel: stopped")
}