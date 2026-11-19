package panel

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

func (a *API) PublicHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /sub/{sub_token}", a.subscription)
	mux.HandleFunc("POST /api/node/sync", a.nodeSync)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}

func nodeSecret(r *http.Request) string {
	if header := strings.TrimSpace(r.Header.Get("Authorization")); header != "" {
		const prefix = "bearer "
		if len(header) > len(prefix) && strings.EqualFold(header[:len(prefix)], prefix) {
			return strings.TrimSpace(header[len(prefix):])
		}
	}
	return strings.TrimSpace(r.Header.Get("X-Node-Secret"))
}

func validSubToken(token string) bool {
	token = strings.TrimSpace(token)
	if len(token) < 16 || len(token) > 128 {
		return false
	}
	for _, r := range token {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		return false
	}
	return true
}

func (a *API) nodeSync(w http.ResponseWriter, r *http.Request) {
	if a.store == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("multiuser storage is not configured"))
		return
	}
	secret := nodeSecret(r)
	if secret == "" {
		w.Header().Set("WWW-Authenticate", `Bearer realm="shampoore-node"`)
		writeErr(w, http.StatusUnauthorized, fmt.Errorf("node secret is required"))
		return
	}
	node, err := a.store.FindNodeBySecret(r.Context(), secret)
	if err != nil {
		if errors.Is(err, panelstore.ErrNotFound) {
			writeErr(w, http.StatusUnauthorized, fmt.Errorf("invalid node secret"))
		} else {
			writeErr(w, http.StatusInternalServerError, err)
		}
		return
	}

	var req struct {
		NodeID  int64                     `json:"node_id"`
		Traffic []panelstore.TrafficDelta `json:"traffic"`
	}
	if err := decodeBody(w, r, &req, 4<<20); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.NodeID != 0 && req.NodeID != node.ID {
		writeErr(w, http.StatusForbidden, fmt.Errorf("node id mismatch"))
		return
	}
	if len(req.Traffic) > 10000 {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("too many traffic entries"))
		return
	}
	if err := a.store.AddTraffic(r.Context(), req.Traffic); err != nil {
		switch {
		case errors.Is(err, panelstore.ErrInvalid), errors.Is(err, panelstore.ErrNotFound):
			writeErr(w, http.StatusBadRequest, err)
		default:
			writeErr(w, http.StatusInternalServerError, err)
		}
		return
	}
	_ = a.store.TouchNode(r.Context(), node.ID)

	now := time.Now()
	users, err := a.store.ActiveUsersForExit(r.Context(), node.ID, now)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	exitID := strconv.FormatInt(node.ID, 10)
	specs := make([]auth.UserSpec, 0, len(users))
	for _, u := range users {
		specs = append(specs, auth.UserSpec{
			UserID:            u.ID,
			Enabled:           true,
			ExpiresAt:         u.ExpiresAt,
			TrafficLimitBytes: u.TrafficLimitBytes,
			AllowedExits:      []string{exitID},
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":   node.ID,
		"synced_at": now.UTC(),
		"users":     specs,
	})
}

func (a *API) subscription(w http.ResponseWriter, r *http.Request) {
	if a.store == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("multiuser storage is not configured"))
		return
	}
	token := r.PathValue("sub_token")
	if !validSubToken(token) {
		writeErr(w, http.StatusNotFound, fmt.Errorf("subscription not found"))
		return
	}
	user, err := a.store.FindUserBySubToken(r.Context(), token)
	if err != nil {
		if errors.Is(err, panelstore.ErrNotFound) {
			writeErr(w, http.StatusNotFound, fmt.Errorf("subscription not found"))
		} else {
			writeErr(w, http.StatusInternalServerError, err)
		}
		return
	}
	if !user.Active(time.Now()) {
		writeErr(w, http.StatusForbidden, fmt.Errorf("subscription is inactive"))
		return
	}
	settings, err := a.subscriptionSettings(r, user)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, settings)
}
