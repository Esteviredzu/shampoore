package jellyfin

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
	"github.com/Esteviredzu/shampoore/internal/transport/httpstream"
)

type Client struct {
	*httpstream.Client
}

func newClient(cfg Config, exitNodeID string, httpClient *http.Client, logger logging.ContextLogger) (*Client, error) {
	cfg = cfg.withDefaults()
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, fmt.Errorf("jellyfin: url is required")
	}
	engineCfg := httpstream.ClientConfig{
		BaseURL:           cfg.URL,
		Secret:            cfg.Secret,
		Namespace:         cfg.Namespace,
		UserID:            cfg.UserID,
		ExitNodeID:        strings.TrimSpace(exitNodeID),
		Role:              cfg.Role,
		HTTPClient:        httpClient,
		Logger:            logger,
		AllowInsecureHTTP: cfg.AllowInsecureHTTP,
	}
	engine, err := httpstream.NewClient(profileFor(cfg.UTLSProfile), engineCfg)
	if err != nil {
		return nil, fmt.Errorf("jellyfin: %w", err)
	}
	return &Client{Client: engine}, nil
}

func (c *Client) WalkLayers() []transport.Layer {
	if c == nil || c.Client == nil {
		return nil
	}
	return renameLayers(c.Client.WalkLayers())
}

type notifyTransport struct {
	transport.Transport
	notify func()
}

func (t notifyTransport) NotifyNetworkChanged() {
	if t.notify != nil {
		t.notify()
	}
}

func (t notifyTransport) WalkLayers() []transport.Layer {
	if tel, ok := t.Transport.(transport.Telemetry); ok {
		return tel.WalkLayers()
	}
	return nil
}

func renameLayers(layers []transport.Layer) []transport.Layer {
	for i := range layers {
		layers[i].Type = "jellyfin"
	}
	return layers
}

var _ transport.Transport = (*Client)(nil)
var _ transport.Telemetry = (*Client)(nil)
