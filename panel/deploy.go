package panel

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"github.com/Esteviredzu/shampoore/internal/provisioner"
)

type deployJob struct {
	nodeID  int64
	done    chan struct{}
	cancel  context.CancelFunc
	outcome deployOutcome
}

type deployOutcome struct {
	result *provisioner.DeployResult
	err    error
}

func (a *API) nodeLock(id int64) *sync.Mutex {
	a.nodeLocksMu.Lock()
	defer a.nodeLocksMu.Unlock()
	if mu, ok := a.nodeLocks[id]; ok {
		return mu
	}
	mu := &sync.Mutex{}
	a.nodeLocks[id] = mu
	return mu
}

func (a *API) forgetNodeLock(id int64) {
	a.nodeLocksMu.Lock()
	delete(a.nodeLocks, id)
	a.nodeLocksMu.Unlock()
}

func (a *API) enqueueDeploy(nodeID int64, reason string) *deployJob {
	if nodeID <= 0 || a.provisioner == nil || a.store == nil {
		return nil
	}
	a.deployMu.Lock()
	a.pending[nodeID] = reason
	if job, running := a.deploys[nodeID]; running {
		job.cancel()
		a.deployMu.Unlock()
		return job
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	job := &deployJob{nodeID: nodeID, done: make(chan struct{}), cancel: cancel}
	a.deploys[nodeID] = job
	a.deployMu.Unlock()
	a.logger.Info(fmt.Sprintf("panel: scheduling deploy of node %d (%s)", nodeID, reason))
	go a.runDeployLoop(ctx, job)
	return job
}

func (a *API) runDeployLoop(ctx context.Context, job *deployJob) {
	for {
		a.deployMu.Lock()
		reason, ok := a.pending[job.nodeID]
		if !ok {
			delete(a.deploys, job.nodeID)
			a.deployMu.Unlock()
			close(job.done)
			return
		}
		delete(a.pending, job.nodeID)
		a.deployMu.Unlock()

		outcome := a.performDeploy(ctx, job.nodeID, reason)

		a.deployMu.Lock()
		if _, again := a.pending[job.nodeID]; !again {
			job.outcome = outcome
			delete(a.deploys, job.nodeID)
			a.deployMu.Unlock()
			close(job.done)
			return
		}
		if ctx.Err() != nil {
			oldCancel := job.cancel
			if oldCancel != nil {
				oldCancel()
			}
			ctx, job.cancel = context.WithTimeout(context.Background(), 15*time.Minute)
		}
		a.deployMu.Unlock()
	}
}

func (a *API) performDeploy(ctx context.Context, nodeID int64, reason string) deployOutcome {
	select {
	case a.deploySem <- struct{}{}:
		defer func() { <-a.deploySem }()
	case <-ctx.Done():
		return deployOutcome{err: ctx.Err()}
	}
	nodeMu := a.nodeLock(nodeID)
	nodeMu.Lock()
	defer nodeMu.Unlock()

	node, err := a.store.GetNode(ctx, nodeID)
	if err != nil {
		a.logger.Warn("panel: deploy: node disappeared: ", err)
		return deployOutcome{err: err}
	}
	_ = a.store.SetNodeStatus(ctx, nodeID, panelstore.NodeStatusDeploying, "")
	a.logger.Info(fmt.Sprintf("panel: deploying node %d (%s)", nodeID, reason))
	result, err := a.provisioner.DeployNode(ctx, node)
	statusCtx, statusCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer statusCancel()
	if err != nil {

		if ctx.Err() == nil {
			_ = a.store.RecordNodeDeploy(statusCtx, nodeID, panelstore.NodeStatusError, err.Error())
		}
		return deployOutcome{result: result, err: err}
	}
	if err := a.store.RecordNodeDeploy(statusCtx, nodeID, panelstore.NodeStatusDeployed, ""); err != nil {
		a.logger.Warn("panel: deploy: set status: ", err)
	}
	a.syncLocalCaddy(context.Background())
	return deployOutcome{result: result}
}

func (a *API) cancelDeploy(nodeID int64, wait time.Duration) {
	a.deployMu.Lock()
	delete(a.pending, nodeID)
	job := a.deploys[nodeID]
	if job != nil && job.cancel != nil {
		job.cancel()
	}
	a.deployMu.Unlock()
	if job == nil || wait <= 0 {
		return
	}
	select {
	case <-job.done:
	case <-time.After(wait):
		a.logger.Warn("panel: deploy of node ", nodeID, " did not stop in time")
	}
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
	a.enqueueDeploy(nodeID, "configuration changed")
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
		a.enqueueDeploy(node.ID, reason)
	}
}

func (a *API) redeployNodesWithAgents(reason string) {
	if a.provisioner == nil || a.store == nil {
		return
	}
	nodes, err := a.store.ListNode(context.Background())
	if err != nil {
		a.logger.Warn("panel: list nodes for redeploy: ", err)
		return
	}
	for _, node := range nodes {
		if node.Status != panelstore.NodeStatusDeployed && node.Status != panelstore.NodeStatusOnline {
			continue
		}
		a.enqueueDeploy(node.ID, reason)
	}
}

func (a *API) scheduleDeploy(nodeID int64, reason string) {
	a.enqueueDeploy(nodeID, reason)
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
	if _, err := a.store.GetNode(r.Context(), id); err != nil {
		writeStoreErr(w, err)
		return
	}
	job := a.enqueueDeploy(id, "manual deploy requested")
	if job == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("provisioner is not configured"))
		return
	}
	select {
	case <-job.done:
		outcome := job.outcome
		if outcome.err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"error":      outcome.err.Error(),
				"deployment": outcome.result,
			})
			return
		}
		writeJSON(w, http.StatusOK, outcome.result)
	case <-r.Context().Done():

	}
}

func (a *API) redeployAllNodes(w http.ResponseWriter, r *http.Request) {
	if a.provisioner == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("provisioner is not configured"))
		return
	}
	nodes, err := a.store.ListNode(r.Context())
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	type nodeRef struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	queued := make([]nodeRef, 0, len(nodes))
	skipped := make([]nodeRef, 0)
	for _, node := range nodes {
		if node.Status != panelstore.NodeStatusDeployed && node.Status != panelstore.NodeStatusOnline {
			skipped = append(skipped, nodeRef{ID: node.ID, Name: node.Name})
			continue
		}
		_ = a.store.SetNodeStatus(context.Background(), node.ID, panelstore.NodeStatusDeploying, "")
		a.enqueueDeploy(node.ID, "redeploy all requested")
		queued = append(queued, nodeRef{ID: node.ID, Name: node.Name})
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"ok":      true,
		"queued":  queued,
		"skipped": skipped,
	})
}
