package panel

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

func deployCalls(f *fakeDeployer) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func cleanupCalls(f *fakeDeployer) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cleanupCalls
}

func waitFor(t *testing.T, what string, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestRedeployAllQueuesDeployedNodes(t *testing.T) {
	api, store, deployer, _ := newControlTestAPI(t)
	h := api.Handler()

	create := func(name, host string, exit bool) panelstore.Node {
		t.Helper()
		body := `{"name":"` + name + `","ssh_host":"` + host + `","is_relay":true,"transports":[{"type":"raw_ws","domain":"` + name + `.example.com","enabled":true}]}`
		if exit {
			body = `{"name":"` + name + `","ssh_host":"` + host + `","is_exit":true}`
		}
		rec := perform(h, http.MethodPost, "/api/nodes", body, authRequest())
		if rec.Code != http.StatusCreated {
			t.Fatalf("create %s = %d: %s", name, rec.Code, rec.Body.String())
		}
		var node panelstore.Node
		_ = json.Unmarshal(rec.Body.Bytes(), &node)
		return node
	}

	relay := create("relay-all", "203.0.113.71", false)
	exit := create("exit-all", "198.51.100.71", true)
	pending := create("pending-all", "198.51.100.72", true)

	for _, node := range []panelstore.Node{relay, exit} {
		rec := perform(h, http.MethodPost, "/api/nodes/"+itoa(node.ID)+"/deploy", "", authRequest())
		if rec.Code != http.StatusOK {
			t.Fatalf("deploy node %d = %d: %s", node.ID, rec.Code, rec.Body.String())
		}
	}
	before := deployCalls(deployer)

	rec := perform(h, http.MethodPost, "/api/nodes/redeploy-all", "", authRequest())
	if rec.Code != http.StatusAccepted {
		t.Fatalf("redeploy-all = %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Queued  []struct{ ID int64 } `json:"queued"`
		Skipped []struct{ ID int64 } `json:"skipped"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Queued) != 2 || len(resp.Skipped) != 1 || resp.Skipped[0].ID != pending.ID {
		t.Fatalf("redeploy-all response = %+v", resp)
	}

	waitFor(t, "queued redeploys", 5*time.Second, func() bool {
		return deployCalls(deployer) >= before+2
	})
	waitFor(t, "nodes to settle", 5*time.Second, func() bool {
		for _, id := range []int64{relay.ID, exit.ID} {
			node, err := store.GetNode(context.Background(), id)
			if err != nil || (node.Status != panelstore.NodeStatusDeployed && node.Status != panelstore.NodeStatusOnline) {
				return false
			}
		}
		return true
	})
}

func TestUserChangesUseSoftSyncWithoutRedeploy(t *testing.T) {
	api, store, deployer, _ := newControlTestAPI(t)
	topo := seedTopology(t, store)

	rec := perform(api.Handler(), http.MethodPatch, "/api/users/"+topo.user.ID, `{"enabled":false}`, authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("patch user = %d: %s", rec.Code, rec.Body.String())
	}
	time.Sleep(150 * time.Millisecond)
	if calls := deployCalls(deployer); calls != 0 {
		t.Fatalf("user update triggered %d redeploys, want 0 (soft sync via node-agent)", calls)
	}
}
func TestNodeConfigUpdateTriggersRedeploy(t *testing.T) {
	api, store, deployer, _ := newControlTestAPI(t)
	h := api.Handler()
	rec := perform(h, http.MethodPost, "/api/nodes", `{"name":"relay-update","ssh_host":"203.0.113.81","is_relay":true,"transports":[{"type":"raw_ws","domain":"ws.update.example.com","enabled":true}]}`, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", rec.Code, rec.Body.String())
	}
	var node panelstore.Node
	_ = json.Unmarshal(rec.Body.Bytes(), &node)
	if rec := perform(h, http.MethodPost, "/api/nodes/"+itoa(node.ID)+"/deploy", "", authRequest()); rec.Code != http.StatusOK {
		t.Fatalf("deploy = %d: %s", rec.Code, rec.Body.String())
	}
	before := deployCalls(deployer)
	body := `{"name":"relay-update","ssh_host":"203.0.113.81","is_relay":true,"transports":[{"type":"raw_ws","domain":"ws.updated.example.com","enabled":true}]}`
	if rec := perform(h, http.MethodPut, "/api/nodes/"+itoa(node.ID), body, authRequest()); rec.Code != http.StatusOK {
		t.Fatalf("update = %d: %s", rec.Code, rec.Body.String())
	}
	waitFor(t, "node redeploy after update", 5*time.Second, func() bool {
		return deployCalls(deployer) > before
	})
	got, err := store.GetNode(context.Background(), node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Transports[0].Domain != "ws.updated.example.com" {
		t.Fatalf("transport domain = %q", got.Transports[0].Domain)
	}
}

func TestDeleteNodeForceRemovesDeadHost(t *testing.T) {
	api, store, deployer, _ := newControlTestAPI(t)
	h := api.Handler()
	rec := perform(h, http.MethodPost, "/api/nodes", `{"name":"dead-host","ssh_host":"203.0.113.91","is_exit":true}`, authRequest())
	if rec.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", rec.Code, rec.Body.String())
	}
	var node panelstore.Node
	_ = json.Unmarshal(rec.Body.Bytes(), &node)

	deployer.mu.Lock()
	deployer.cleanupErr = errors.New("ssh: connection refused")
	deployer.mu.Unlock()

	rec = perform(h, http.MethodDelete, "/api/nodes/"+itoa(node.ID), "", authRequest())
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("cleanup failure = %d, want 502: %s", rec.Code, rec.Body.String())
	}
	if _, err := store.GetNode(context.Background(), node.ID); err != nil {
		t.Fatal("node was deleted without force")
	}
	if cleanupCalls(deployer) != 1 {
		t.Fatalf("cleanup calls = %d", cleanupCalls(deployer))
	}

	rec = perform(h, http.MethodDelete, "/api/nodes/"+itoa(node.ID)+"?force=true", "", authRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("forced delete = %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := store.GetNode(context.Background(), node.ID); err == nil {
		t.Fatal("node still exists after forced delete")
	}
	if cleanupCalls(deployer) != 2 {
		t.Fatalf("cleanup calls after force = %d", cleanupCalls(deployer))
	}
}
