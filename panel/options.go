package panel

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/provisioner"
)

type Option func(*API)

type NodeDeployer interface {
	DeployNode(ctx context.Context, node panelstore.Node) (*provisioner.DeployResult, error)
	CleanupNode(ctx context.Context, node panelstore.Node) (*provisioner.CleanupResult, error)
}

func WithStore(store *panelstore.Store) Option {
	return func(a *API) { a.store = store }
}

func WithProvisioner(p NodeDeployer) Option {
	return func(a *API) { a.provisioner = p }
}

func WithPublicURL(url string) Option {
	return func(a *API) { a.publicURL = strings.TrimSpace(url) }
}

func WithPublicListen(addr string) Option {
	return func(a *API) { a.publicListen = strings.TrimSpace(addr) }
}

func WithCaddy(c CaddySyncer) Option {
	return func(a *API) { a.caddy = c }
}

func WithLocalHosts(hosts []string) Option {
	return func(a *API) { a.localHosts = append([]string(nil), hosts...) }
}

func WithGeoRuleHTTPClient(client *http.Client) Option {
	return func(a *API) {
		if client != nil {
			a.geoClient = client
		}
	}
}

func WithGeoRuleUpdateInterval(interval time.Duration) Option {
	return func(a *API) {
		if interval > 0 {
			a.geoUpdateEvery = interval
		}
	}
}
