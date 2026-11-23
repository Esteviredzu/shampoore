package panel

import (
	"fmt"
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
	routeByID := make(map[int64]panelstore.Route, len(routes))
	for _, route := range routes {
		routeByID[route.ID] = route
	}

	out := make([]app.Settings, 0, len(routes))
	for _, route := range routes {
		if !route.Enabled || !user.RouteAllowed(route.ID) {
			continue
		}
		if route.TransportType != "" {
			if descriptor, ok := app.DescriptorFor(route.TransportType); ok && descriptor.Caps.Multi != nil {
				settings, err := multiRouteSettings(route, routeByID, byID, user)
				if err != nil {
					if a.logger != nil {
						a.logger.Warn("panel: skip multi route: ", err)
					}
					continue
				}
				out = append(out, settings)
				continue
			}
		}
		if settings, ok := singleRouteSettings(route, byID, user); ok {
			out = append(out, settings)
		}
	}
	return out, nil
}

func singleRouteSettings(route panelstore.Route, byID map[int64]panelstore.Node, user panelstore.User) (app.Settings, bool) {
	return singleRouteSettingsIn(route, byID, user, route)
}

func singleRouteSettingsIn(
	route panelstore.Route,
	byID map[int64]panelstore.Node,
	user panelstore.User,
	namespace panelstore.Route,
) (app.Settings, bool) {
	entry, ok := byID[route.EntryNodeID]
	if !ok {
		return app.Settings{}, false
	}
	exit, ok := byID[route.ExitNodeID]
	if !ok {
		return app.Settings{}, false
	}
	tr, ok := entry.EnabledTransport(route.TransportType)
	if !ok {
		return app.Settings{}, false
	}
	descriptor, ok := app.DescriptorFor(route.TransportType)
	if !ok || descriptor.Caps.Client == nil {
		return app.Settings{}, false
	}
	settings, err := descriptor.ClientConfig(app.RouteContext{
		Role:               app.RouteRoleSubscription,
		RouteID:            namespace.ID,
		RouteName:          namespace.Name,
		SubRouteID:         route.ID,
		SubRouteName:       route.Name,
		UserID:             user.ID,
		ExitNodeID:         strconv.FormatInt(exit.ID, 10),
		Domain:             tr.Domain,
		EncryptionPassword: entry.GeneratedSecrets["encryption_password"],
		Secrets:            entry.GeneratedSecrets,
	})
	if err != nil {
		return app.Settings{}, false
	}
	if strings.TrimSpace(settings.Type) == "" {
		settings.Type = descriptor.Kind
	}
	if strings.TrimSpace(settings.Name) == "" {
		settings.Name = route.Name
	}
	return settings, true
}

func multiRouteSettings(
	route panelstore.Route,
	routeByID map[int64]panelstore.Route,
	byID map[int64]panelstore.Node,
	user panelstore.User,
) (app.Settings, error) {
	descriptor, ok := app.DescriptorFor(route.TransportType)
	if !ok || descriptor.Caps.Multi == nil {
		return app.Settings{}, fmt.Errorf("multi route %d: transport %q is not a multi composer", route.ID, route.TransportType)
	}
	entry, ok := byID[route.EntryNodeID]
	if !ok {
		return app.Settings{}, fmt.Errorf("multi route %d: entry node %d not found", route.ID, route.EntryNodeID)
	}
	exit, ok := byID[route.ExitNodeID]
	if !ok {
		return app.Settings{}, fmt.Errorf("multi route %d: exit node %d not found", route.ID, route.ExitNodeID)
	}
	req := app.MultiRequest{
		Strategy:           route.MultiStrategy,
		RouteID:            route.ID,
		RouteName:          route.Name,
		UserID:             user.ID,
		ExitNodeID:         strconv.FormatInt(exit.ID, 10),
		EncryptionPassword: entry.GeneratedSecrets["encryption_password"],
	}
	for _, id := range route.SubRouteIDs {
		child, ok := routeByID[id]
		if !ok || !child.Enabled {
			continue
		}
		childDescriptor, ok := app.DescriptorFor(child.TransportType)
		if !ok || childDescriptor.Caps.Multi != nil {
			continue
		}
		childSettings, ok := singleRouteSettingsIn(child, byID, user, route)
		if !ok {
			continue
		}
		req.Subflows = append(req.Subflows, app.SubflowSpec{
			Type:      childSettings.Type,
			Name:      child.Name,
			Transport: childSettings.Transport,
		})
	}
	if err := descriptor.Caps.Multi.Validate(req); err != nil {
		return app.Settings{}, fmt.Errorf("multi route %d: %w", route.ID, err)
	}
	return descriptor.Caps.Multi.BuildSubscription(req)
}
