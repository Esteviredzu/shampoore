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
	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/provisioner"
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
	dbPath := flag.String("db", "./panel.db", "SQLite database path for the multiuser control plane (empty disables it)")
	publicListen := flag.String("public-listen", "", "public HTTP listen for /sub/{token} and /api/node/sync (empty disables it)")
	publicURL := flag.String("public-url", "", "externally reachable panel URL, e.g. https://panel.example.com")
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

	externalURL := strings.TrimSpace(*publicURL)
	if externalURL == "" && strings.TrimSpace(*publicListen) != "" {
		externalURL = guessPublicURL(*publicListen)
		logger.Warn("panel: -public-url is not set; using ", externalURL, " (remote nodes may not reach this address)")
	}

	var (
		store *panelstore.Store
		prov  *provisioner.Provisioner
		opts  []panel.Option
	)
	if strings.TrimSpace(*dbPath) != "" {
		store, err = panelstore.Open(ctx, *dbPath)
		if err != nil {
			logger.Error("panel: database: ", err)
			os.Exit(1)
		}
		defer func() {
			if err := store.Close(); err != nil {
				logger.Warn("panel: database close: ", err)
			}
		}()
		prov = provisioner.New(store, provisioner.Options{
			PanelURL: externalURL,
			Logger:   logger,
		})
		opts = append(opts,
			panel.WithStore(store),
			panel.WithProvisioner(prov),
			panel.WithPublicURL(externalURL),
		)
	}

	api := panel.New(client, sup, logger, tokenValue, *configsDir, filter, *rulesDir, opts...)

	srv := &http.Server{
		Addr:              *listen,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		logger.Info("panel: admin listening on http://", *listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("panel: ", err)
		}
	}()

	var publicSrv *http.Server
	if strings.TrimSpace(*publicListen) != "" {
		if store == nil {
			logger.Error("panel: -public-listen requires a database (-db)")
			os.Exit(1)
		}
		publicSrv = &http.Server{
			Addr:              *publicListen,
			Handler:           api.PublicHandler(),
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       60 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		go func() {
			logger.Info("panel: public API listening on http://", *publicListen)
			if err := publicSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("panel: public: ", err)
			}
		}()
	}

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

	shCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	if err := srv.Shutdown(shCtx); err != nil {
		logger.Warn("panel: shutdown: ", err)
	}
	cancelShutdown()
	if publicSrv != nil {
		publicCtx, cancelPublic := context.WithTimeout(context.Background(), 5*time.Second)
		if err := publicSrv.Shutdown(publicCtx); err != nil {
			logger.Warn("panel: public shutdown: ", err)
		}
		cancelPublic()
	}
	logger.Info("panel: stopped")
}

func guessPublicURL(listen string) string {
	listen = strings.TrimSpace(listen)
	switch {
	case strings.HasPrefix(listen, ":"):
		return "http://127.0.0.1" + listen
	case strings.HasPrefix(listen, "0.0.0.0:"):
		return "http://127.0.0.1:" + strings.TrimPrefix(listen, "0.0.0.0:")
	default:
		return "http://" + listen
	}
}
