package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/relay"
)

func main() {
	listen := flag.String("listen", envDefault("SHAMPOORE_RELAY_LISTEN", ":8787"), "HTTP listen address")
	password := flag.String("password", os.Getenv("SHAMPOORE_RELAY_PASSWORD"), "shared room password")
	decoyDir := flag.String("decoy-dir", os.Getenv("SHAMPOORE_RELAY_DECOY_DIR"), "optional directory with a decoy site (must contain index.html); empty disables the decoy site")
	maxSize := flag.Int64("max-message-size", envInt64("SHAMPOORE_RELAY_MAX_MESSAGE", relay.DefaultMaxMessageSize), "maximum websocket message size")
	flag.Parse()

	logger := logging.New("relay", true)
	srv := relay.New(*password)
	srv.SetMaxMessageSize(*maxSize)
	if *decoyDir != "" {
		srv.SetDecoyDir(*decoyDir)
		logger.Info("relay: decoy site directory ", *decoyDir)
	}
	if *password == "" {
		logger.Warn("relay: password is empty; anyone can join relay rooms")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	logger.Info("relay: listening on ", *listen)
	if err := srv.Run(ctx, *listen); err != nil {
		logger.Error("relay: ", err)
		os.Exit(1)
	}
	logger.Info("relay: stopped")
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
