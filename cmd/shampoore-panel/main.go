package main

import (
	"context"
	"flag"
	"fmt"
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
	"github.com/Esteviredzu/shampoore/internal/version"
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
	listen := flag.String("listen", "127.0.0.1:8080", "panel admin HTTP listen (loopback only; use -allow-public-admin to override)")
	token := flag.String("token", "", "panel access token; generated and logged when empty")
	configsDir := flag.String("configs-dir", "./configs", "directory with managed configs")
	rulesDir := flag.String("rules-dir", "./rules", "directory with geoip rule files")
	dbPath := flag.String("db", "./panel.db", "SQLite database path for the multiuser control plane (empty disables it)")
	publicListen := flag.String("public-listen", "", "public HTTP listen for /sub/{token} and /api/node/sync (empty = settings/default 127.0.0.1:8443, \"off\" disables)")
	publicURL := flag.String("public-url", "", "externally reachable panel URL override, e.g. https://sub.example.com (empty = use panel settings)")
	allowPublicAdmin := flag.Bool("allow-public-admin", false,
		"allow the admin UI to listen on a non-loopback interface (not recommended; use an SSH tunnel)")
	autoStart := flag.Bool("auto-start-server", false,
		"start shampoore-server automatically on panel launch (useful under systemd)")
	cleanupAllNodes := flag.Bool("cleanup-all-nodes", false,
		"clean up every server stored in the panel database via SSH and exit (used by uninstall --purge)")
	showVersion := flag.Bool("version", false, "print the panel version and exit")
	updateRepo := flag.String("update-repo", envDefault("SHAMPOORE_UPDATE_REPO", panel.DefaultUpdateRepo),
		"GitHub repository (owner/name) used for panel self-updates")
	flag.Parse()

	logger := logging.New("panel", true)

	if *showVersion {
		fmt.Println("shampoore-panel", version.Current())
		return
	}

	if *cleanupAllNodes {
		os.Exit(runCleanupAllNodes(*dbPath, *publicURL, logger))
	}

	logger.Info("panel: starting version ", version.Current())

	if err := panel.ValidateAdminListen(*listen, *allowPublicAdmin); err != nil {
		logger.Error(err)
		os.Exit(1)
	}
	if *allowPublicAdmin && !panel.IsLoopbackListen(*listen) {
		logger.Warn("panel: admin UI is exposed beyond localhost; prefer an SSH tunnel")
	}

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
	effectivePublicListen := strings.TrimSpace(*publicListen)
	publicDisabled := strings.EqualFold(effectivePublicListen, "off") || strings.EqualFold(effectivePublicListen, "none")

	var (
		store *panelstore.Store
		prov  *provisioner.Provisioner
		opts  []panel.Option
	)
	opts = append(opts,
		panel.WithPanelVersion(version.Current()),
		panel.WithUpdateRepo(*updateRepo),
		panel.WithUpdateToken(os.Getenv("SHAMPOORE_GITHUB_TOKEN")),
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
		settings, err := store.GetSettings(ctx)
		if err != nil {
			logger.Error("panel: settings: ", err)
		} else {
			if effectivePublicListen == "" && !publicDisabled {
				effectivePublicListen = settings.PublicListen
			}
			if externalURL == "" {
				externalURL = strings.TrimSpace(settings.PublicURL)
				if externalURL == "" && strings.TrimSpace(settings.PublicDomain) != "" {
					externalURL = "https://" + strings.TrimSpace(settings.PublicDomain)
				}
			}
		}
		if effectivePublicListen == "" && !publicDisabled {
			effectivePublicListen = panelstore.DefaultPublicListen
		}
		if externalURL == "" && !publicDisabled {
			externalURL = guessPublicURL(effectivePublicListen)
			logger.Warn("panel: public URL is not set; using ", externalURL, " (set the subscription domain in the panel UI)")
		}
		prov = provisioner.New(store, provisioner.Options{
			PanelURL:   strings.TrimSpace(*publicURL),
			RulesDir:   *rulesDir,
			Logger:     logger,
			LocalHosts: provisioner.LocalHostIPs(),
		})
		opts = append(opts,
			panel.WithStore(store),
			panel.WithProvisioner(prov),
			panel.WithCaddy(panel.NewLocalCaddy()),
			panel.WithLocalHosts(provisioner.LocalHostIPs()),
		)
		if !publicDisabled && effectivePublicListen != "" {
			opts = append(opts, panel.WithPublicListen(effectivePublicListen))
		}
		if strings.TrimSpace(*publicURL) != "" {
			opts = append(opts, panel.WithPublicURL(*publicURL))
		}
	}

	api := panel.New(client, sup, logger, tokenValue, *configsDir, filter, *rulesDir, opts...)
	api.StartGeoRuleUpdater(ctx)

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
	if !publicDisabled && store != nil && strings.TrimSpace(effectivePublicListen) != "" {
		publicSrv = &http.Server{
			Addr:              effectivePublicListen,
			Handler:           api.PublicHandler(),
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       60 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		go func() {
			logger.Info("panel: public API listening on http://", effectivePublicListen)
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

type nodeCleaner interface {
	CleanupNode(ctx context.Context, node panelstore.Node) (*provisioner.CleanupResult, error)
}

func runCleanupAllNodes(dbPath, publicURL string, logger *logging.Logger) int {
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		logger.Error("panel: --cleanup-all-nodes requires a non-empty --db path")
		return 1
	}
	if _, err := os.Stat(dbPath); err != nil {
		logger.Error("panel: --cleanup-all-nodes: ", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := panelstore.Open(ctx, dbPath)
	if err != nil {
		logger.Error("panel: database: ", err)
		return 1
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Warn("panel: database close: ", err)
		}
	}()

	nodes, err := store.ListNode(ctx)
	if err != nil {
		logger.Error("panel: list nodes: ", err)
		return 1
	}
	if len(nodes) == 0 {
		logger.Info("panel: no servers in the database; nothing to clean up")
		return 0
	}

	cleaner := provisioner.New(store, provisioner.Options{
		PanelURL:   strings.TrimSpace(publicURL),
		Logger:     logger,
		LocalHosts: provisioner.LocalHostIPs(),
	})
	ok, failed := cleanupNodes(ctx, nodes, cleaner, logger)
	logger.Info("panel: cleanup finished: ", ok, " ok, ", failed, " failed")
	if failed > 0 {
		return 1
	}
	return 0
}

func cleanupNodes(ctx context.Context, nodes []panelstore.Node, cleaner nodeCleaner, logger *logging.Logger) (int, int) {
	ok, failed := 0, 0
	for _, node := range nodes {
		logger.Info("panel: cleaning up node ", node.ID, " (", node.Name, ") at ", node.SSHUser, "@", node.SSHHost)
		nodeCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		result, err := cleaner.CleanupNode(nodeCtx, node)
		cancel()
		if err != nil {
			failed++
			logger.Warn("panel: cleanup of node ", node.ID, " (", node.Name, ") failed: ", err)
			if result != nil {
				for _, step := range result.Steps {
					if step.Error != "" {
						logger.Warn("panel: node ", node.ID, " step ", step.Name, ": ", step.Error)
					}
				}
			}
			continue
		}
		if result != nil && !result.Success {
			failed++
			logger.Warn("panel: cleanup of node ", node.ID, " (", node.Name, ") finished with errors")
			continue
		}
		ok++
		logger.Info("panel: node ", node.ID, " (", node.Name, ") cleaned up")
	}
	return ok, failed
}

func envDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
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
