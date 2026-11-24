package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/nodeagent"
)

func main() {
	panelURL := flag.String("panel", os.Getenv("PANEL_URL"), "panel public URL")
	nodeIDRaw := flag.String("node-id", os.Getenv("NODE_ID"), "node ID")
	secret := flag.String("secret", os.Getenv("NODE_SECRET"), "node secret")
	controlSocket := flag.String("control-socket", envDefault("CONTROL_SOCKET", "/run/shampoore/shampoore.sock"), "shampoore-server control socket")
	configsDir := flag.String("configs-dir", envDefault("CONFIGS_DIR", "/opt/shampoore/configs"), "server configs directory")
	stateFile := flag.String("state", envDefault("STATE_FILE", "/opt/shampoore/agent-state.json"), "agent state file")
	interval := flag.Duration("interval", envDuration("INTERVAL", 5*time.Second), "sync interval (also the host metrics heartbeat)")
	flag.Parse()

	logger := logging.New("node-agent", true)
	nodeID, err := strconv.ParseInt(*nodeIDRaw, 10, 64)
	if err != nil || nodeID <= 0 {
		logger.Error("node-agent: invalid NODE_ID: ", *nodeIDRaw)
		os.Exit(1)
	}
	if *panelURL == "" || *secret == "" {
		logger.Error("node-agent: PANEL_URL and NODE_SECRET are required")
		os.Exit(1)
	}

	agent := nodeagent.New(nodeagent.Config{
		PanelURL:      *panelURL,
		NodeID:        nodeID,
		NodeSecret:    *secret,
		ControlSocket: *controlSocket,
		ConfigsDir:    *configsDir,
		StateFile:     *stateFile,
		Interval:      *interval,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	logger.Info("node-agent: syncing node ", nodeID, " with ", *panelURL)
	if err := agent.Run(ctx); err != nil {
		logger.Error("node-agent: ", err)
		os.Exit(1)
	}
	logger.Info("node-agent: stopped")
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
