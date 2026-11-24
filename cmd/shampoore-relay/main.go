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
	"github.com/Esteviredzu/shampoore/internal/transport/jellyfin"
)

func main() {
	listen := flag.String("listen", envDefault("SHAMPOORE_RELAY_LISTEN", ":8787"), "HTTP listen address for the WebSocket relay (empty disables it)")
	password := flag.String("password", os.Getenv("SHAMPOORE_RELAY_PASSWORD"), "shared room password")
	decoyDir := flag.String("decoy-dir", os.Getenv("SHAMPOORE_RELAY_DECOY_DIR"), "optional directory with a decoy site (must contain index.html); empty disables the decoy site")
	jellyfinListen := flag.String("jellyfin-listen", os.Getenv("SHAMPOORE_RELAY_JELLYFIN_LISTEN"), "HTTP listen address for the Jellyfin rendezvous broker (empty disables it)")
	jellyfinSecret := flag.String("jellyfin-secret", os.Getenv("SHAMPOORE_RELAY_JELLYFIN_SECRET"), "shared Jellyfin session secret (HMAC for X-Emby-Token)")
	jellyfinUpstream := flag.String("jellyfin-upstream", envDefault("SHAMPOORE_RELAY_JELLYFIN_UPSTREAM", "http://jellyfin:8096"), "real Jellyfin upstream used for requests without a valid session token")
	maxSize := flag.Int64("max-message-size", envInt64("SHAMPOORE_RELAY_MAX_MESSAGE", relay.DefaultMaxMessageSize), "maximum websocket message size")
	flag.Parse()

	logger := logging.New("relay", true)
	srv := relay.New(*password)
	srv.SetMaxMessageSize(*maxSize)
	if *decoyDir != "" {
		srv.SetDecoyDir(*decoyDir)
		logger.Info("relay: decoy site directory ", *decoyDir)
		logger.Info("relay: traffic shape ", srv.ShapeSource())
		if err := srv.ShapeError(); err != nil {
			logger.Warn("relay: ws-shape.json: ", err, " (using defaults)")
		}
	}
	if *password == "" && *listen != "" {
		logger.Warn("relay: password is empty; anyone can join relay rooms")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var broker *jellyfin.Broker
	if *jellyfinListen != "" {
		b, err := jellyfin.NewBroker(jellyfin.BrokerConfig{
			Secret:      *jellyfinSecret,
			UpstreamURL: *jellyfinUpstream,
			Listen:      *jellyfinListen,
			Logger:      logging.New("relay-jellyfin", true),
		})
		if err != nil {
			logger.Error("relay: jellyfin broker: ", err)
			os.Exit(1)
		}
		if err := b.Start(ctx, *jellyfinListen); err != nil {
			logger.Error("relay: jellyfin broker: ", err)
			os.Exit(1)
		}
		broker = b
	} else if *jellyfinUpstream != "" && *jellyfinSecret != "" {
		logger.Debug("relay: jellyfin broker is disabled (no -jellyfin-listen)")
	}

	if *listen == "" {
		if broker == nil {
			logger.Warn("relay: both the WebSocket relay and the jellyfin broker are disabled")
		}
		<-ctx.Done()
	} else {
		logger.Info("relay: listening on ", *listen)
		if err := srv.Run(ctx, *listen); err != nil {
			logger.Error("relay: ", err)
			if broker != nil {
				_ = broker.Stop(context.Background())
			}
			os.Exit(1)
		}
	}
	if broker != nil {
		if err := broker.Stop(context.Background()); err != nil {
			logger.Warn("relay: stop jellyfin broker: ", err)
		}
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
