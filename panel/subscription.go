package panel

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type OfflineBundle struct {
	Name  string   `json:"name"`
	Links []string `json:"links"`
}

func (a *API) offlineBundle(r *http.Request, user panelstore.User) (OfflineBundle, error) {
	settings, err := a.subscriptionSettings(r, user)
	if err != nil {
		return OfflineBundle{}, err
	}
	bundle := OfflineBundle{
		Name:  strings.TrimSpace(user.Name),
		Links: make([]string, 0, len(settings)),
	}
	if bundle.Name == "" {
		bundle.Name = user.ID
	}
	for i := range settings {
		link, err := app.EncodeURL(&settings[i])
		if err != nil {
			if a.logger != nil {
				a.logger.Warn("panel: encode offline bundle link: ", err)
			}
			continue
		}
		bundle.Links = append(bundle.Links, link)
	}
	return bundle, nil
}

func (a *API) subscriptionSettings(r *http.Request, user panelstore.User) ([]app.Settings, error) {
	routes, err := a.store.ListRoutes(r.Context())
	if err != nil {
		return nil, err
	}
	nodes, err := a.store.ListNode(r.Context())
	if err != nil {
		return nil, err
	}
	byID := make(map[int64]panelstore.Node, len(nodes))
	for _, n := range nodes {
		byID[n.ID] = n
	}

	out := make([]app.Settings, 0, len(routes))
	for _, route := range routes {
		if !route.Enabled || !user.RouteAllowed(route.ID) {
			continue
		}
		entry, ok := byID[route.EntryNodeID]
		if !ok {
			continue
		}
		exit, ok := byID[route.ExitNodeID]
		if !ok {
			continue
		}
		tr, ok := entry.EnabledTransport(route.TransportType)
		if !ok {
			continue
		}
		descriptor, ok := app.DescriptorFor(route.TransportType)
		if !ok || descriptor.BuildSubscriptionConfig == nil {
			continue
		}
		settings, err := descriptor.BuildSubscriptionConfig(app.RouteParams{
			RouteID:    route.ID,
			RouteName:  route.Name,
			UserID:     user.ID,
			ExitNodeID: strconv.FormatInt(exit.ID, 10),
			Domain:     tr.Domain,
			Secrets:    entry.GeneratedSecrets,
		})
		if err != nil {
			continue
		}
		if strings.TrimSpace(settings.Type) == "" {
			settings.Type = descriptor.Kind
		}
		if strings.TrimSpace(settings.Name) == "" {
			settings.Name = route.Name
		}
		out = append(out, settings)
	}
	return out, nil
}
