package panel

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/provisioner"
)

type settingsView struct {
	panelstore.Settings
	PublicURLOverride bool `json:"public_url_override"`
}

type settingsRequest struct {
	PublicDomain         *string `json:"public_domain"`
	PublicURL            *string `json:"public_url"`
	PublicListen         *string `json:"public_listen"`
	SubscriptionsEnabled *bool   `json:"subscriptions_enabled"`
}

func (a *API) currentSettings() panelstore.Settings {
	a.settingsMu.RLock()
	defer a.settingsMu.RUnlock()
	return a.settings
}

func (a *API) setCurrentSettings(s panelstore.Settings) {
	a.settingsMu.Lock()
	a.settings = s
	a.settingsMu.Unlock()
}

func (a *API) currentPublicURL() string {
	if url := strings.TrimSpace(a.publicURL); url != "" {
		return url
	}
	settings := a.currentSettings()
	if url := strings.TrimSpace(settings.PublicURL); url != "" {
		return url
	}
	if domain := strings.TrimSpace(settings.PublicDomain); domain != "" {
		return "https://" + domain
	}
	return ""
}

func (a *API) getSettings(w http.ResponseWriter, r *http.Request) {
	settings := a.currentSettings()
	if a.store != nil {
		if fresh, err := a.store.GetSettings(r.Context()); err != nil {
			a.logger.Warn("panel: load settings: ", err)
		} else {
			settings = fresh
			a.setCurrentSettings(fresh)
		}
	}
	writeJSON(w, http.StatusOK, settingsView{
		Settings:          settings,
		PublicURLOverride: strings.TrimSpace(a.publicURL) != "",
	})
}

func (a *API) updateSettings(w http.ResponseWriter, r *http.Request) {
	if a.store == nil {
		writeErr(w, http.StatusServiceUnavailable, errNoStore())
		return
	}
	var req settingsRequest
	if err := decodeBody(w, r, &req, 1<<20); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	current, err := a.store.GetSettings(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	in := current
	if req.PublicDomain != nil {
		in.PublicDomain = *req.PublicDomain
		if strings.TrimSpace(*req.PublicDomain) == "" && req.PublicURL == nil {
			in.PublicURL = ""
		}
	}
	if req.PublicURL != nil {
		in.PublicURL = *req.PublicURL
	}
	if req.PublicListen != nil {
		in.PublicListen = *req.PublicListen
	}
	switch {
	case req.SubscriptionsEnabled != nil:
		in.SetSubscriptionsEnabled(*req.SubscriptionsEnabled)
	case strings.TrimSpace(in.PublicDomain) == "":
		in.SetSubscriptionsEnabled(false)
	case strings.TrimSpace(current.PublicDomain) == "":

		in.SetSubscriptionsEnabled(true)
	}
	saved, err := a.store.UpdateSettings(r.Context(), in)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	a.setCurrentSettings(saved)
	status := a.syncLocalCaddy(r.Context())

	a.redeployDeployedExits("public URL changed")
	writeJSON(w, http.StatusOK, map[string]any{
		"settings": settingsView{
			Settings:          saved,
			PublicURLOverride: strings.TrimSpace(a.publicURL) != "",
		},
		"caddy": status,
	})
}

func (a *API) deleteSettingsDomain(w http.ResponseWriter, r *http.Request) {
	if a.store == nil {
		writeErr(w, http.StatusServiceUnavailable, errNoStore())
		return
	}
	in, err := a.store.GetSettings(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	in.PublicDomain = ""
	in.PublicURL = ""
	in.SetSubscriptionsEnabled(false)
	saved, err := a.store.UpdateSettings(r.Context(), in)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	a.setCurrentSettings(saved)
	status := a.syncLocalCaddy(r.Context())
	a.redeployDeployedExits("public domain removed")
	writeJSON(w, http.StatusOK, map[string]any{
		"settings": settingsView{
			Settings:          saved,
			PublicURLOverride: strings.TrimSpace(a.publicURL) != "",
		},
		"caddy": status,
	})
}

func (a *API) syncCaddy(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"caddy": a.syncLocalCaddy(r.Context())})
}

func (a *API) syncLocalCaddy(ctx context.Context) CaddyStatus {
	a.caddyMu.Lock()
	defer a.caddyMu.Unlock()
	if a.caddy == nil {
		return CaddyStatus{Applied: false, Message: "Caddy integration is not configured"}
	}
	sites, err := a.localCaddySites(ctx)
	if err != nil {
		a.logger.Warn("panel: collect caddy sites: ", err)
		return CaddyStatus{Applied: false, Message: err.Error()}
	}
	status, err := a.caddy.Sync(ctx, sites)
	if err != nil {
		a.logger.Warn("panel: sync caddy: ", err)
		if status.Message == "" {
			status.Message = err.Error()
		}
	}
	return status
}

func (a *API) localCaddySites(ctx context.Context) ([]CaddySite, error) {
	settings := a.currentSettings()
	var sites []CaddySite
	if settings.SubscriptionsEnabled {
		if domain := strings.TrimSpace(settings.PublicDomain); domain != "" {
			listen := strings.TrimSpace(a.publicListen)
			if listen == "" {
				listen = settings.PublicListen
			}
			upstream := localUpstream(listen)
			if upstream == "" {
				upstream = localUpstream(panelstore.DefaultPublicListen)
			}
			sites = append(sites, CaddySite{Domain: domain, Upstream: upstream})
		}
	}
	if a.store == nil {
		return sites, nil
	}
	nodes, err := a.store.ListNode(ctx)
	if err != nil {
		return nil, err
	}
	localIPs := a.localHosts
	for _, node := range nodes {
		if !node.IsRelay || !provisioner.IsLocalHost(node.SSHHost, localIPs) {
			continue
		}
		for _, tr := range node.Transports {
			if !tr.Enabled || strings.TrimSpace(tr.Domain) == "" {
				continue
			}
			descriptor, ok := app.DescriptorFor(tr.Type)
			if !ok {
				continue
			}
			upstream := loopbackUpstream(descriptor.RelayUpstream)
			if upstream == "" {
				continue
			}
			sites = append(sites, CaddySite{Domain: app.HostOnly(tr.Domain), Upstream: upstream})
		}
	}
	return sites, nil
}

func loopbackUpstream(upstream string) string {
	upstream = strings.TrimSpace(upstream)
	if upstream == "" {
		return ""
	}
	_, port, err := net.SplitHostPort(upstream)
	if err != nil {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", port)
}

func errNoStore() error {
	return panelError("multiuser storage is not configured")
}

type panelError string

func (e panelError) Error() string { return string(e) }
