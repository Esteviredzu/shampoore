package panel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/provisioner"
)

func (a *API) registerControlPlane(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/nodes", a.listNodes)
	mux.HandleFunc("POST /api/nodes", a.createNode)
	mux.HandleFunc("GET /api/nodes/{id}", a.getNode)
	mux.HandleFunc("PUT /api/nodes/{id}", a.updateNode)
	mux.HandleFunc("DELETE /api/nodes/{id}", a.deleteNode)
	mux.HandleFunc("POST /api/nodes/{id}/deploy", a.deployNode)
	mux.HandleFunc("POST /api/nodes/{id}/start", a.startNode)
	mux.HandleFunc("POST /api/nodes/{id}/stop", a.stopNode)

	mux.HandleFunc("GET /api/routes", a.listRoutes)
	mux.HandleFunc("POST /api/routes", a.createRoute)
	mux.HandleFunc("GET /api/routes/{id}", a.getRoute)
	mux.HandleFunc("PUT /api/routes/{id}", a.updateRoute)
	mux.HandleFunc("DELETE /api/routes/{id}", a.deleteRoute)

	mux.HandleFunc("GET /api/users", a.listUsers)
	mux.HandleFunc("POST /api/users", a.createUser)
	mux.HandleFunc("GET /api/users/{id}", a.getUser)
	mux.HandleFunc("GET /api/users/{id}/bundle", a.userBundle)
	mux.HandleFunc("PUT /api/users/{id}", a.updateUser)
	mux.HandleFunc("DELETE /api/users/{id}", a.deleteUser)
	mux.HandleFunc("POST /api/users/{id}/reset-traffic", a.resetUserTraffic)

	mux.HandleFunc("GET /api/settings", a.getSettings)
	mux.HandleFunc("PUT /api/settings", a.updateSettings)
	mux.HandleFunc("DELETE /api/settings/domain", a.deleteSettingsDomain)
	mux.HandleFunc("POST /api/settings/caddy", a.syncCaddy)

	mux.HandleFunc("GET /api/georules", a.listGeoRuleSources)
	mux.HandleFunc("POST /api/georules/import-url", a.importGeoRule)
	mux.HandleFunc("POST /api/georules/{name}/refresh", a.refreshGeoRule)
	mux.HandleFunc("PUT /api/georules/{name}/nodes", a.updateGeoRuleNodes)
	mux.HandleFunc("DELETE /api/georules/{name}", a.deleteRule)
}

func (a *API) registerSystemRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/system/version", a.systemVersion)
	mux.HandleFunc("GET /api/system/update", a.checkSystemUpdate)
	mux.HandleFunc("POST /api/system/update", a.installSystemUpdate)
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any, limit int64) error {
	if limit <= 0 {
		limit = 1 << 20
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	return nil
}

func writeStoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, panelstore.ErrNotFound):
		writeErr(w, http.StatusNotFound, err)
	case errors.Is(err, panelstore.ErrConflict):
		writeErr(w, http.StatusConflict, err)
	case errors.Is(err, panelstore.ErrInvalid):
		writeErr(w, http.StatusBadRequest, err)
	default:
		writeErr(w, http.StatusInternalServerError, err)
	}
}

func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(strings.TrimSpace(r.PathValue("id")), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid id")
	}
	return id, nil
}

func (a *API) listNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := a.store.ListNode(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nodes)
}

func (a *API) createNode(w http.ResponseWriter, r *http.Request) {
	var node panelstore.Node
	if err := decodeBody(w, r, &node, 1<<20); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	node.ID = 0
	created, err := a.store.CreateNode(r.Context(), node)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (a *API) getNode(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	node, err := a.store.GetNode(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (a *API) updateNode(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var node panelstore.Node
	if err := decodeBody(w, r, &node, 1<<20); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	node.ID = id
	updated, err := a.store.UpdateNode(r.Context(), node)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	a.syncLocalCaddy(context.Background())
	writeJSON(w, http.StatusOK, updated)
}

func (a *API) deleteNode(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	node, err := a.store.GetNode(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	force := false
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("force"))) {
	case "1", "true", "yes":
		force = true
	}

	a.deployMu.Lock()
	if _, busy := a.deploying[id]; busy {
		a.deployMu.Unlock()
		writeErr(w, http.StatusConflict, fmt.Errorf("node %d is busy; wait for the current operation to finish", id))
		return
	}
	a.deploying[id] = struct{}{}
	a.deployMu.Unlock()
	defer func() {
		a.deployMu.Lock()
		delete(a.deploying, id)
		a.deployMu.Unlock()
	}()

	var cleanup *provisioner.CleanupResult
	if a.provisioner != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
		cleanup, err = a.provisioner.CleanupNode(ctx, node)
		cancel()
		if err != nil && !force {
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"error":   fmt.Sprintf("cleanup failed: %v (retry with ?force=true if the VPS is already gone)", err),
				"cleanup": cleanup,
			})
			return
		}
	}

	routes, _ := a.store.ListRoutes(r.Context())
	affectedExits := make(map[int64]struct{})
	for _, route := range routes {
		if route.ExitNodeID != id {
			affectedExits[route.ExitNodeID] = struct{}{}
		}
	}

	if err := a.store.DeleteNode(r.Context(), id); err != nil {
		writeStoreErr(w, err)
		return
	}
	a.syncLocalCaddy(context.Background())
	for exitID := range affectedExits {
		a.scheduleRedeployIfDeployed(exitID)
	}
	response := map[string]any{"ok": true}
	if cleanup != nil {
		response["cleanup"] = cleanup
	}
	if err != nil && force {
		response["warning"] = fmt.Sprintf("server cleanup failed, node removed anyway: %v", err)
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *API) deployNode(w http.ResponseWriter, r *http.Request) {
	if a.provisioner == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("provisioner is not configured"))
		return
	}
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	node, err := a.store.GetNode(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}

	a.deployMu.Lock()
	if _, busy := a.deploying[id]; busy {
		a.deployMu.Unlock()
		writeErr(w, http.StatusConflict, fmt.Errorf("node %d is already being deployed", id))
		return
	}
	a.deploying[id] = struct{}{}
	a.deployMu.Unlock()
	defer func() {
		a.deployMu.Lock()
		delete(a.deploying, id)
		a.deployMu.Unlock()
	}()

	if err := a.store.SetNodeStatus(r.Context(), id, panelstore.NodeStatusDeploying, ""); err != nil {
		writeStoreErr(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
	defer cancel()
	result, err := a.provisioner.DeployNode(ctx, node)
	statusCtx, statusCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer statusCancel()
	if err != nil {
		_ = a.store.SetNodeStatus(statusCtx, id, panelstore.NodeStatusError, err.Error())
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error":      err.Error(),
			"deployment": result,
		})
		return
	}
	if err := a.store.SetNodeStatus(statusCtx, id, panelstore.NodeStatusDeployed, ""); err != nil {
		writeStoreErr(w, err)
		return
	}
	a.syncLocalCaddy(context.Background())
	writeJSON(w, http.StatusOK, result)
}

func (a *API) startNode(w http.ResponseWriter, r *http.Request) {
	a.setNodeRunning(w, r, true)
}

func (a *API) stopNode(w http.ResponseWriter, r *http.Request) {
	a.setNodeRunning(w, r, false)
}

func (a *API) setNodeRunning(w http.ResponseWriter, r *http.Request, running bool) {
	if a.provisioner == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("provisioner is not configured"))
		return
	}
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	node, err := a.store.GetNode(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if node.Status == panelstore.NodeStatusNew {
		writeErr(w, http.StatusConflict, fmt.Errorf("node %q is not deployed yet", node.Name))
		return
	}

	a.deployMu.Lock()
	if _, busy := a.deploying[id]; busy {
		a.deployMu.Unlock()
		writeErr(w, http.StatusConflict, fmt.Errorf("node %d is busy; wait for the current operation to finish", id))
		return
	}
	a.deploying[id] = struct{}{}
	a.deployMu.Unlock()
	defer func() {
		a.deployMu.Lock()
		delete(a.deploying, id)
		a.deployMu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
	defer cancel()
	result, err := a.provisioner.SetNodeRunning(ctx, node, running)

	statusCtx, statusCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer statusCancel()
	if err != nil {
		_ = a.store.SetNodeStatus(statusCtx, id, node.Status, err.Error())
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error":   err.Error(),
			"service": result,
		})
		return
	}

	status := panelstore.NodeStatusDeployed
	if !running {
		status = panelstore.NodeStatusStopped
	}
	if err := a.store.SetNodeStatus(statusCtx, id, status, ""); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) listRoutes(w http.ResponseWriter, r *http.Request) {
	routes, err := a.store.ListRoutes(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, routes)
}

func (a *API) createRoute(w http.ResponseWriter, r *http.Request) {
	var route panelstore.Route
	if err := decodeBody(w, r, &route, 1<<20); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	route.ID = 0
	created, err := a.store.CreateRoute(r.Context(), route)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	a.scheduleRedeployIfDeployed(created.ExitNodeID)
	writeJSON(w, http.StatusCreated, created)
}

func (a *API) getRoute(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	route, err := a.store.GetRoute(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, route)
}

func (a *API) updateRoute(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var route panelstore.Route
	if err := decodeBody(w, r, &route, 1<<20); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	route.ID = id
	old, oldErr := a.store.GetRoute(r.Context(), id)
	updated, err := a.store.UpdateRoute(r.Context(), route)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if oldErr == nil && old.ExitNodeID != updated.ExitNodeID {
		a.scheduleRedeployIfDeployed(old.ExitNodeID)
	}
	a.scheduleRedeployIfDeployed(updated.ExitNodeID)
	writeJSON(w, http.StatusOK, updated)
}

func (a *API) deleteRoute(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	route, err := a.store.GetRoute(r.Context(), id)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	if err := a.store.DeleteRoute(r.Context(), id); err != nil {
		writeStoreErr(w, err)
		return
	}
	a.scheduleRedeployIfDeployed(route.ExitNodeID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type userView struct {
	panelstore.User
	SubscriptionURL string `json:"subscription_url,omitempty"`
	BundleURL       string `json:"bundle_url,omitempty"`
}

func (a *API) userView(u panelstore.User) userView {
	view := userView{User: u}
	if url := a.currentPublicURL(); url != "" {
		view.SubscriptionURL = strings.TrimRight(url, "/") + "/sub/" + u.SubToken
	}
	if u.ID != "" {
		view.BundleURL = "/api/users/" + u.ID + "/bundle"
	}
	return view
}

func (a *API) listUsers(w http.ResponseWriter, r *http.Request) {
	users, err := a.store.ListUsers(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	out := make([]userView, 0, len(users))
	for _, u := range users {
		out = append(out, a.userView(u))
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) createUser(w http.ResponseWriter, r *http.Request) {
	var user panelstore.User
	if err := decodeBody(w, r, &user, 1<<20); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	created, err := a.store.CreateUser(r.Context(), user)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, a.userView(created))
}

func (a *API) getUser(w http.ResponseWriter, r *http.Request) {
	user, err := a.store.GetUser(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a.userView(user))
}

func (a *API) userBundle(w http.ResponseWriter, r *http.Request) {
	user, err := a.store.GetUser(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	bundle, err := a.offlineBundle(r, user)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, bundle)
}

func (a *API) updateUser(w http.ResponseWriter, r *http.Request) {
	var user panelstore.User
	if err := decodeBody(w, r, &user, 1<<20); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	user.ID = r.PathValue("id")
	updated, err := a.store.UpdateUser(r.Context(), user)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a.userView(updated))
}

func (a *API) deleteUser(w http.ResponseWriter, r *http.Request) {
	if err := a.store.DeleteUser(r.Context(), r.PathValue("id")); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *API) resetUserTraffic(w http.ResponseWriter, r *http.Request) {
	if err := a.store.ResetUserTraffic(r.Context(), r.PathValue("id")); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *API) scheduleRedeployIfDeployed(nodeID int64) {
	if nodeID <= 0 || a.provisioner == nil || a.store == nil {
		return
	}
	node, err := a.store.GetNode(context.Background(), nodeID)
	if err != nil {
		return
	}
	if node.Status != panelstore.NodeStatusDeployed && node.Status != panelstore.NodeStatusOnline {
		return
	}
	a.scheduleDeploy(nodeID, "route configuration changed")
}

func (a *API) redeployDeployedExits(reason string) {
	if a.provisioner == nil || a.store == nil {
		return
	}
	nodes, err := a.store.ListNode(context.Background())
	if err != nil {
		a.logger.Warn("panel: list nodes for redeploy: ", err)
		return
	}
	for _, node := range nodes {
		if !node.IsExit {
			continue
		}
		if node.Status != panelstore.NodeStatusDeployed && node.Status != panelstore.NodeStatusOnline {
			continue
		}
		a.scheduleDeploy(node.ID, reason)
	}
}

func (a *API) scheduleDeploy(nodeID int64, reason string) {
	if nodeID <= 0 || a.provisioner == nil || a.store == nil {
		return
	}
	a.deployMu.Lock()
	if _, busy := a.deploying[nodeID]; busy {
		a.pending[nodeID] = struct{}{}
		a.deployMu.Unlock()
		return
	}
	a.deploying[nodeID] = struct{}{}
	a.deployMu.Unlock()
	a.logger.Info(fmt.Sprintf("panel: scheduling redeploy of node %d (%s)", nodeID, reason))
	go a.backgroundDeploy(nodeID)
}

func (a *API) backgroundDeploy(nodeID int64) {
	defer func() {
		a.deployMu.Lock()
		delete(a.deploying, nodeID)
		delete(a.pending, nodeID)
		a.deployMu.Unlock()
	}()
	for {
		node, err := a.store.GetNode(context.Background(), nodeID)
		if err != nil {
			a.logger.Warn("panel: background deploy: node disappeared: ", err)
			return
		}
		_ = a.store.SetNodeStatus(context.Background(), nodeID, panelstore.NodeStatusDeploying, "")
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		_, err = a.provisioner.DeployNode(ctx, node)
		cancel()
		statusCtx, statusCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err != nil {
			a.logger.Warn("panel: background deploy failed: ", err)
			_ = a.store.SetNodeStatus(statusCtx, nodeID, panelstore.NodeStatusError, err.Error())
		} else {
			_ = a.store.SetNodeStatus(statusCtx, nodeID, panelstore.NodeStatusDeployed, "")
			a.syncLocalCaddy(context.Background())
		}
		statusCancel()

		a.deployMu.Lock()
		if _, again := a.pending[nodeID]; again {
			delete(a.pending, nodeID)
			a.deployMu.Unlock()
			continue
		}
		a.deployMu.Unlock()
		return
	}
}
