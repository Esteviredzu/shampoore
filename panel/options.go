package panel

import (
	"context"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/provisioner"
)

// Option configures the optional multiuser control plane.
type Option func(*API)

// NodeDeployer is implemented by provisioner.Provisioner and test fakes.
type NodeDeployer interface {
	DeployNode(ctx context.Context, node panelstore.Node) (*provisioner.DeployResult, error)
}

// WithStore enables node/route/user management and public subscriptions.
func WithStore(store *panelstore.Store) Option {
	return func(a *API) { a.store = store }
}

// WithProvisioner enables POST /api/nodes/{id}/deploy.
func WithProvisioner(p NodeDeployer) Option {
	return func(a *API) { a.provisioner = p }
}

// WithPublicURL sets the externally reachable panel URL used by node templates
// and subscription links.
func WithPublicURL(url string) Option {
	return func(a *API) { a.publicURL = url }
}
