package panel

import (
	"context"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/provisioner"
)

type Option func(*API)

type NodeDeployer interface {
	DeployNode(ctx context.Context, node panelstore.Node) (*provisioner.DeployResult, error)
}

func WithStore(store *panelstore.Store) Option {
	return func(a *API) { a.store = store }
}

func WithProvisioner(p NodeDeployer) Option {
	return func(a *API) { a.provisioner = p }
}

func WithPublicURL(url string) Option {
	return func(a *API) { a.publicURL = url }
}
