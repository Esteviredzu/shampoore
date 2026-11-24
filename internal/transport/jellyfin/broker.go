package jellyfin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
	"github.com/Esteviredzu/shampoore/internal/transport/httpstream"
)

const defaultBrokerListen = ":8097"

type BrokerConfig struct {
	Secret      string
	UpstreamURL string
	Listen      string
	UTLSProfile string
	Logger      logging.ContextLogger
}

type Broker struct {
	cfg    BrokerConfig
	logger logging.ContextLogger
	engine *httpstream.PairBroker

	mu       sync.Mutex
	httpSrv  *http.Server
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	started  bool
	wg       sync.WaitGroup
}

func NewBroker(cfg BrokerConfig) (*Broker, error) {
	cfg.Secret = strings.TrimSpace(cfg.Secret)
	cfg.UpstreamURL = strings.TrimSpace(cfg.UpstreamURL)
	cfg.Listen = strings.TrimSpace(cfg.Listen)
	cfg.UTLSProfile = strings.TrimSpace(cfg.UTLSProfile)
	if cfg.Secret == "" {
		return nil, errors.New("jellyfin: broker secret is required")
	}
	if cfg.UpstreamURL == "" {
		cfg.UpstreamURL = defaultUpstreamURL
	}
	if cfg.Listen == "" {
		cfg.Listen = defaultBrokerListen
	}
	if cfg.Logger == nil {
		cfg.Logger = logging.New("jellyfin-broker", false)
	}
	engine, err := httpstream.NewPairBroker(profileFor(cfg.UTLSProfile), httpstream.ServerConfig{
		Secret:          cfg.Secret,
		FallbackHandler: newFallbackHandler(cfg.UpstreamURL, cfg.Logger),
		Logger:          cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("jellyfin: broker: %w", err)
	}
	return &Broker{cfg: cfg, logger: cfg.Logger, engine: engine}, nil
}

func (b *Broker) Handler() http.Handler {
	if b == nil || b.engine == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "jellyfin: broker is not configured", http.StatusInternalServerError)
		})
	}
	return b.engine
}

func (b *Broker) Addr() string {
	if b == nil {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listener == nil {
		return ""
	}
	return b.listener.Addr().String()
}

func (b *Broker) Start(ctx context.Context, listen string) error {
	if b == nil || b.engine == nil {
		return errors.New("jellyfin: broker is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	listen = strings.TrimSpace(listen)
	if listen == "" {
		listen = b.cfg.Listen
	}

	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return nil
	}
	b.ctx, b.cancel = context.WithCancel(ctx)
	b.started = true
	b.mu.Unlock()

	listener, err := net.Listen("tcp", listen)
	if err != nil {
		b.mu.Lock()
		b.started = false
		if b.cancel != nil {
			b.cancel()
			b.cancel = nil
		}
		b.mu.Unlock()
		return fmt.Errorf("jellyfin: broker listen %s: %w", listen, err)
	}
	httpSrv := &http.Server{
		Handler:           b.engine,
		ReadHeaderTimeout: 10 * time.Second,
	}
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		_ = listener.Close()
		return nil
	}
	b.listener = listener
	b.httpSrv = httpSrv
	b.wg.Add(1)
	b.mu.Unlock()

	go func() {
		defer b.wg.Done()
		if serveErr := httpSrv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			b.logger.Warn("jellyfin: broker http server stopped: ", serveErr)
		}
	}()
	b.logger.Info("jellyfin: rendezvous broker on ", listener.Addr().String(),
		" (upstream=", b.cfg.UpstreamURL, ")")
	return nil
}

func (b *Broker) Stop(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	httpSrv := b.httpSrv
	cancel := b.cancel
	b.httpSrv = nil
	b.listener = nil
	b.cancel = nil
	b.started = false
	b.mu.Unlock()

	if httpSrv != nil {
		if ctx == nil {
			var stopCancel context.CancelFunc
			ctx, stopCancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
		}
		if err := httpSrv.Shutdown(ctx); err != nil {
			b.logger.Debug("jellyfin: broker http shutdown: ", err)
		}
	}
	if cancel != nil {
		cancel()
	}
	if b.engine != nil {
		if err := b.engine.Close(); err != nil {
			b.logger.Debug("jellyfin: broker engine close: ", err)
		}
	}
	b.wg.Wait()
	return nil
}

func (b *Broker) SessionCount() int {
	if b == nil || b.engine == nil {
		return 0
	}
	return b.engine.SessionCount()
}

func (b *Broker) RoomCount() int {
	if b == nil || b.engine == nil {
		return 0
	}
	return b.engine.RoomCount()
}

func (b *Broker) WalkLayers() []transport.Layer {
	if b == nil || b.engine == nil {
		return nil
	}
	return renameLayers(b.engine.WalkLayers())
}

var _ transport.Telemetry = (*Broker)(nil)
