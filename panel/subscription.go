package panel

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

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
		settings, err := clientSettings(user, entry, exit, route, tr)
		if err != nil {
			continue
		}
		out = append(out, settings)
	}
	return out, nil
}

func clientSettings(user panelstore.User, entry, exit panelstore.Node, route panelstore.Route, tr panelstore.Transport) (app.Settings, error) {
	secrets := entry.GeneratedSecrets
	routeID := strconv.FormatInt(route.ID, 10)
	exitID := strconv.FormatInt(exit.ID, 10)
	transport := map[string]any{"user_id": user.ID}

	var kind string
	switch route.TransportType {
	case panelstore.TransportRawWS:
		kind = "raw_ws"
		serverURL := subscriptionWSURL(tr.Domain)
		transport["server_url"] = serverURL
		transport["room"] = "route-" + routeID
		transport["password"] = secrets["raw_ws_password"]
		transport["batch"] = true
		transport["padding"] = true
		if strings.HasPrefix(serverURL, "ws://") {
			transport["allow_insecure_ws"] = true
		}
	case panelstore.TransportSilo:
		kind = "silo"
		transport["endpoint"] = subscriptionHost(tr.Domain)
		transport["access_key"] = secrets["s3_access_key"]
		transport["secret_key"] = secrets["s3_secret_key"]
		transport["bucket"] = secrets["s3_bucket"]
		transport["use_ssl"] = !strings.HasPrefix(strings.TrimSpace(tr.Domain), "http://")
		transport["tunnel_id"] = "route-" + routeID
	case panelstore.TransportLiveKit:
		kind = "livekit_datachannel"
		transport["signal_url"] = subscriptionWSURL(tr.Domain)
		transport["api_key"] = secrets["livekit_api_key"]
		transport["api_secret"] = secrets["livekit_api_secret"]
		transport["room"] = "shampoore-route-" + routeID + "-" + strings.ToLower(user.ID)
		transport["room_prefix"] = "shampoore-route-" + routeID
		transport["data_topic"] = "shampoore"
		if tokenURL := strings.TrimSpace(secrets["livekit_token_url"]); tokenURL != "" {
			transport["token_url"] = tokenURL
		}
	default:
		return app.Settings{}, fmt.Errorf("unsupported transport %q", route.TransportType)
	}

	raw, err := json.Marshal(transport)
	if err != nil {
		return app.Settings{}, err
	}
	return app.Settings{
		Name:               route.Name,
		UserID:             user.ID,
		ExitNodeID:         exitID,
		Type:               kind,
		Transport:          raw,
		EncryptionPassword: secrets["encryption_password"],
	}, nil
}

func subscriptionWSURL(domain string) string {
	domain = strings.TrimSpace(domain)
	switch {
	case strings.HasPrefix(domain, "wss://"), strings.HasPrefix(domain, "ws://"):
		return domain
	case strings.HasPrefix(domain, "https://"):
		return "wss://" + strings.TrimPrefix(domain, "https://")
	case strings.HasPrefix(domain, "http://"):
		return "ws://" + strings.TrimPrefix(domain, "http://")
	default:
		return "wss://" + domain
	}
}

func subscriptionHost(domain string) string {
	domain = strings.TrimSpace(domain)
	domain = strings.TrimPrefix(domain, "https://")
	domain = strings.TrimPrefix(domain, "http://")
	domain = strings.TrimSuffix(domain, "/")
	if host, port, err := net.SplitHostPort(domain); err == nil {
		return net.JoinHostPort(host, port)
	}
	return domain
}
