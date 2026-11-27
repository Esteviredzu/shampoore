package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type HTTPPanel struct {
	BaseURL string
	Secret  string
	Client  *http.Client
}

func NewHTTPPanel(baseURL, secret string) *HTTPPanel {
	return &HTTPPanel{
		BaseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		Secret:  strings.TrimSpace(secret),
		Client:  &http.Client{Timeout: 30 * time.Second},
	}
}

type syncResponse struct {
	NodeID int64           `json:"node_id"`
	Users  []auth.UserSpec `json:"users"`
}

func (p *HTTPPanel) Sync(ctx context.Context, nodeID int64, traffic []panelstore.TrafficDelta, metrics *panelstore.NodeMetrics, activeUserIDs []string) ([]auth.UserSpec, error) {
	if p == nil || p.BaseURL == "" {
		return nil, fmt.Errorf("nodeagent: panel url is empty")
	}
	if traffic == nil {
		traffic = []panelstore.TrafficDelta{}
	}
	payload := map[string]any{
		"node_id": nodeID,
		"traffic": traffic,
	}
	if metrics != nil {
		payload["metrics"] = metrics
	}
	if len(activeUserIDs) > 0 {
		payload["active_user_ids"] = activeUserIDs
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/api/node/sync", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.Secret)

	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("nodeagent: panel sync: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var out syncResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("nodeagent: decode sync response: %w", err)
	}
	if out.Users == nil {
		out.Users = []auth.UserSpec{}
	}
	return out.Users, nil
}
