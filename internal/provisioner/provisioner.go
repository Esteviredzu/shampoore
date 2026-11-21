package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"golang.org/x/crypto/ssh"
)

type NodeStore interface {
	GetNode(ctx context.Context, id int64) (panelstore.Node, error)
	ListNode(ctx context.Context) ([]panelstore.Node, error)
	ListRoutes(ctx context.Context) ([]panelstore.Route, error)
	ListRoutesForExit(ctx context.Context, exitNodeID int64) ([]panelstore.Route, error)
	GetSettings(ctx context.Context) (panelstore.Settings, error)
	ListGeoRuleBindings(ctx context.Context) (map[string][]int64, error)
}

type Options struct {
	PanelURL        string
	LocalBinDir     string
	RulesDir        string
	Logger          *logging.Logger
	Dial            func(ctx context.Context, node panelstore.Node) (Remote, error)
	HostKeyCallback ssh.HostKeyCallback

	HTTPClient *http.Client

	LocalHosts []string
}

type Provisioner struct {
	store NodeStore
	opts  Options
}

func New(store NodeStore, opts Options) *Provisioner {
	return &Provisioner{store: store, opts: opts}
}

type Step struct {
	Name    string `json:"name"`
	Command string `json:"command,omitempty"`
	Output  string `json:"output,omitempty"`
	Error   string `json:"error,omitempty"`
}

type DeployResult struct {
	NodeID  int64  `json:"node_id"`
	Success bool   `json:"success"`
	Steps   []Step `json:"steps"`
}

func (p *Provisioner) logf(format string, args ...any) {
	if p.opts.Logger != nil {
		p.opts.Logger.Info(fmt.Sprintf(format, args...))
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (p *Provisioner) panelURL(ctx context.Context) (string, error) {
	if url := strings.TrimSpace(p.opts.PanelURL); url != "" {
		return url, nil
	}
	settings, err := p.store.GetSettings(ctx)
	if err != nil {
		return "", fmt.Errorf("provisioner: read panel settings: %w", err)
	}
	if url := strings.TrimSpace(settings.PublicURL); url != "" {
		return url, nil
	}
	return "", fmt.Errorf("provisioner: public URL is not configured; set it in the panel settings first")
}

func (p *Provisioner) render(ctx context.Context, node panelstore.Node) ([]File, error) {
	if !node.IsRelay && !node.IsExit {
		return nil, fmt.Errorf("provisioner: node %q has no role", node.Name)
	}
	panelURL, err := p.panelURL(ctx)
	if err != nil && node.IsExit {
		return nil, err
	}
	localRelay := node.IsRelay && IsLocalHost(node.SSHHost, p.opts.LocalHosts)
	if node.IsRelay && !node.IsExit {
		return RenderRelay(node, RenderOptions{PanelURL: panelURL, LocalRelay: localRelay})
	}
	routes, err := p.store.ListRoutesForExit(ctx, node.ID)
	if err != nil {
		return nil, err
	}

	singleRoutes := make([]panelstore.Route, 0, len(routes))
	for _, route := range routes {
		if route.TransportType == panelstore.TransportMulti {
			continue
		}
		singleRoutes = append(singleRoutes, route)
	}
	nodes, err := p.store.ListNode(ctx)
	if err != nil {
		return nil, err
	}
	ruleFiles, err := p.exitRuleFiles(ctx, node)
	if err != nil {
		return nil, err
	}
	exitFiles, err := RenderExit(node, singleRoutes, nodes, RenderOptions{PanelURL: panelURL, RuleFiles: ruleFiles})
	if err != nil {
		return nil, err
	}
	if !node.IsRelay {
		return exitFiles, nil
	}
	relayFiles, err := RenderRelay(node, RenderOptions{PanelURL: panelURL, LocalRelay: localRelay})
	if err != nil {
		return nil, err
	}
	return mergeRoleFiles(relayFiles, exitFiles)
}

const exitRuleStateFile = "rules.state.json"

func (p *Provisioner) exitRuleFiles(ctx context.Context, node panelstore.Node) ([]File, error) {
	keep := File{Name: "rules/.keep", Mode: 0o644}
	dir := strings.TrimSpace(p.opts.RulesDir)
	if dir == "" {
		return []File{keep}, nil
	}

	state := map[string]bool{}
	statePath := filepath.Join(dir, exitRuleStateFile)
	if data, err := os.ReadFile(statePath); err == nil {
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, fmt.Errorf("provisioner: parse %s: %w", statePath, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("provisioner: read %s: %w", statePath, err)
	}

	bindings, err := p.store.ListGeoRuleBindings(ctx)
	if err != nil {
		return nil, fmt.Errorf("provisioner: read geo rule bindings: %w", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []File{keep}, nil
		}
		return nil, fmt.Errorf("provisioner: read rules dir %s: %w", dir, err)
	}

	files := []File{keep}
	active := make(map[string]bool)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileName := entry.Name()
		if strings.HasPrefix(fileName, ".") || fileName == exitRuleStateFile {
			continue
		}
		ext := filepath.Ext(fileName)
		switch strings.ToLower(ext) {
		case ".txt", ".json":
		default:
			continue
		}
		ruleName := strings.TrimSuffix(fileName, ext)
		if !state[ruleName] {
			continue
		}
		if ids, ok := bindings[ruleName]; ok && len(ids) > 0 && !containsNodeID(ids, node.ID) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, fileName))
		if err != nil {
			return nil, fmt.Errorf("provisioner: read rule %q: %w", fileName, err)
		}
		active[ruleName] = true
		files = append(files, File{Name: "rules/" + fileName, Mode: 0o644, Data: data})
	}

	stateData, err := json.MarshalIndent(active, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("provisioner: encode rule state: %w", err)
	}
	stateData = append(stateData, '\n')
	files = append(files, File{Name: "rules/" + exitRuleStateFile, Mode: 0o600, Data: stateData})
	return files, nil
}

func containsNodeID(ids []int64, id int64) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}

func (p *Provisioner) binaryFiles(node panelstore.Node) ([]File, error) {
	var names []string
	if node.IsRelay {
		names = append(names, "shampoore-relay")
	}
	if node.IsExit {
		names = append(names, "shampoore-server", "shampoore-node-agent")
	}
	files := make([]File, 0, len(names))
	for _, name := range names {
		path, err := findBinary(name, p.opts.LocalBinDir)
		if err != nil {
			return nil, err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("provisioner: read %s: %w", path, err)
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("provisioner: binary %q is empty", path)
		}
		files = append(files, File{Name: "bin/" + name, Mode: 0o755, Data: data})
	}
	return files, nil
}

func findBinary(name, explicitDir string) (string, error) {
	var dirs []string
	if strings.TrimSpace(explicitDir) != "" {
		dirs = append(dirs, explicitDir)
	}
	if exe, err := os.Executable(); err == nil {
		dirs = append(dirs, filepath.Dir(exe))
	}
	dirs = append(dirs, ".", "bin", "..")
	for _, dir := range dirs {
		path := filepath.Join(dir, name)
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path, nil
		}
	}
	return "", fmt.Errorf("provisioner: binary %q not found (build it with release.sh or set LocalBinDir)", name)
}

func (p *Provisioner) DeployNode(ctx context.Context, node panelstore.Node) (*DeployResult, error) {
	if p == nil || p.store == nil {
		return nil, errors.New("provisioner: store is not configured")
	}
	if !node.IsRelay && !node.IsExit {
		return nil, fmt.Errorf("provisioner: node %q has no role", node.Name)
	}
	if strings.TrimSpace(node.SSHHost) == "" {
		return nil, fmt.Errorf("provisioner: node %q has no ssh_host", node.Name)
	}

	files, err := p.render(ctx, node)
	if err != nil {
		return &DeployResult{NodeID: node.ID, Steps: []Step{{Name: "render", Error: err.Error()}}}, err
	}
	binaries, err := p.binaryFiles(node)
	if err != nil {
		return &DeployResult{NodeID: node.ID, Steps: []Step{{Name: "binaries", Error: err.Error()}}}, err
	}
	files = append(files, binaries...)

	decoy, err := p.decoyFiles(ctx, node)
	if err != nil {
		return &DeployResult{NodeID: node.ID, Steps: []Step{{Name: "decoy site", Error: err.Error()}}}, err
	}
	files = append(files, decoy...)

	result := &DeployResult{NodeID: node.ID}
	addStep := func(step Step) { result.Steps = append(result.Steps, step) }
	fail := func(step Step, err error) (*DeployResult, error) {
		step.Error = err.Error()
		addStep(step)
		result.Success = false
		return result, err
	}

	dial := p.opts.Dial
	if dial == nil {
		dial = func(ctx context.Context, n panelstore.Node) (Remote, error) {
			return dialSSH(ctx, n, p.opts.HostKeyCallback)
		}
	}
	remote, err := dial(ctx, node)
	if err != nil {
		return fail(Step{Name: "ssh connect", Command: node.SSHHost}, err)
	}
	defer remote.Close()
	addStep(Step{Name: "ssh connect", Output: node.SSHUser + "@" + node.SSHHost})

	sudo := ""
	out, err := remote.Run(ctx, "id -u")
	if err == nil && strings.TrimSpace(out) != "0" {
		sudo = "sudo -n "
	}
	if err != nil {
		addStep(Step{Name: "detect user", Command: "id -u", Output: out, Error: err.Error()})
	}

	dockerVersion, err := remote.Run(ctx, sudo+"docker --version")
	if err != nil {
		addStep(Step{Name: "check docker", Command: sudo + "docker --version", Output: dockerVersion, Error: err.Error()})
		install := []struct {
			name string
			cmd  string
		}{
			{"install curl", sudo + "sh -c 'command -v curl >/dev/null 2>&1 || (apt-get update && apt-get install -y curl)'"},
			{"install docker", sudo + "sh -c 'curl -fsSL https://get.docker.com | sh'"},
			{"enable docker", sudo + "systemctl enable --now docker"},
		}
		for _, item := range install {
			out, err := remote.Run(ctx, item.cmd)
			addStep(Step{Name: item.name, Command: item.cmd, Output: out, Error: errString(err)})
			if err != nil {
				return fail(Step{Name: item.name, Command: item.cmd}, err)
			}
		}
		out, err = remote.Run(ctx, sudo+"docker --version")
		if err != nil {
			return fail(Step{Name: "check docker", Command: sudo + "docker --version", Output: out}, err)
		}
		dockerVersion = out
	}
	addStep(Step{Name: "check docker", Command: sudo + "docker --version", Output: strings.TrimSpace(dockerVersion)})

	out, err = remote.Run(ctx, sudo+"mkdir -p /opt/shampoore /opt/shampoore/bin /opt/shampoore/configs /opt/shampoore/rules /opt/shampoore/decoy")
	addStep(Step{Name: "prepare directory", Command: "mkdir -p /opt/shampoore", Output: out, Error: errString(err)})
	if err != nil {
		return fail(Step{Name: "prepare directory"}, err)
	}

	if node.IsExit {
		cleanRulesCmd := sudo + "rm -rf /opt/shampoore/rules"
		out, err = remote.Run(ctx, cleanRulesCmd)
		addStep(Step{Name: "clean rules", Command: cleanRulesCmd, Output: out, Error: errString(err)})
		if err != nil {
			return fail(Step{Name: "clean rules", Command: cleanRulesCmd}, err)
		}
	}

	if node.IsRelay {
		cleanDecoyCmd := sudo + "rm -rf /opt/shampoore/decoy"
		out, err = remote.Run(ctx, cleanDecoyCmd)
		addStep(Step{Name: "clean decoy", Command: cleanDecoyCmd, Output: out, Error: errString(err)})
		if err != nil {
			return fail(Step{Name: "clean decoy", Command: cleanDecoyCmd}, err)
		}
	}

	if err := remote.Upload(ctx, "/opt/shampoore", files, sudo != ""); err != nil {
		return fail(Step{Name: "upload files"}, err)
	}
	addStep(Step{Name: "upload files", Output: fmt.Sprintf("%d files -> /opt/shampoore", len(files))})

	compose := ""
	if out, err := remote.Run(ctx, sudo+"docker compose version"); err == nil {
		compose = sudo + "docker compose"
		addStep(Step{Name: "detect compose", Command: sudo + "docker compose version", Output: strings.TrimSpace(out)})
	} else if out, err := remote.Run(ctx, sudo+"docker-compose version"); err == nil {
		compose = sudo + "docker-compose"
		addStep(Step{Name: "detect compose", Command: sudo + "docker-compose version", Output: strings.TrimSpace(out)})
	} else {
		return fail(Step{Name: "detect compose", Command: "docker compose version"}, errors.New("docker compose is not available"))
	}

	upCmd := compose + " -f /opt/shampoore/docker-compose.yml up -d --force-recreate --remove-orphans"
	out, err = remote.Run(ctx, upCmd)
	addStep(Step{Name: "compose up", Command: upCmd, Output: out, Error: errString(err)})
	if err != nil {
		return fail(Step{Name: "compose up", Command: upCmd}, err)
	}

	if hasEnabledTransport(node, panelstore.TransportSilo) {
		bucket := strings.TrimSpace(node.GeneratedSecrets["s3_bucket"])
		if bucket == "" {
			return fail(Step{Name: "create silo bucket"}, errors.New("silo transport is enabled but s3_bucket is empty"))
		}
		bucketCmd := sudo + "docker exec shampoore-silo mkdir -p " + shellQuote("/data/"+bucket)
		out, err = remote.Run(ctx, bucketCmd)
		addStep(Step{Name: "create silo bucket", Command: bucketCmd, Output: out, Error: errString(err)})
		if err != nil {
			return fail(Step{Name: "create silo bucket", Command: bucketCmd}, err)
		}
	}

	psCmd := compose + " -f /opt/shampoore/docker-compose.yml ps"
	out, err = remote.Run(ctx, psCmd)
	addStep(Step{Name: "compose ps", Command: psCmd, Output: out, Error: errString(err)})
	if err != nil {
		return fail(Step{Name: "compose ps", Command: psCmd}, err)
	}

	result.Success = true
	p.logf("provisioner: node %d deployed", node.ID)
	return result, nil
}

type ServiceResult struct {
	NodeID  int64  `json:"node_id"`
	Running bool   `json:"running"`
	Success bool   `json:"success"`
	Steps   []Step `json:"steps"`
}

func (p *Provisioner) SetNodeRunning(ctx context.Context, node panelstore.Node, running bool) (*ServiceResult, error) {
	if p == nil {
		return nil, errors.New("provisioner: not configured")
	}
	if strings.TrimSpace(node.SSHHost) == "" {
		return nil, fmt.Errorf("provisioner: node %q has no ssh_host", node.Name)
	}

	result := &ServiceResult{NodeID: node.ID, Running: running}
	addStep := func(step Step) { result.Steps = append(result.Steps, step) }
	fail := func(step Step, err error) (*ServiceResult, error) {
		step.Error = err.Error()
		addStep(step)
		result.Success = false
		return result, err
	}

	dial := p.opts.Dial
	if dial == nil {
		dial = func(ctx context.Context, n panelstore.Node) (Remote, error) {
			return dialSSH(ctx, n, p.opts.HostKeyCallback)
		}
	}
	remote, err := dial(ctx, node)
	if err != nil {
		return fail(Step{Name: "ssh connect", Command: node.SSHHost}, err)
	}
	defer remote.Close()
	addStep(Step{Name: "ssh connect", Output: node.SSHUser + "@" + node.SSHHost})

	sudo := ""
	if out, err := remote.Run(ctx, "id -u"); err == nil && strings.TrimSpace(out) != "0" {
		sudo = "sudo -n "
	}

	checkCmd := sudo + "test -f /opt/shampoore/docker-compose.yml"
	out, err := remote.Run(ctx, checkCmd)
	addStep(Step{Name: "check compose file", Command: checkCmd, Output: out, Error: errString(err)})
	if err != nil {
		return fail(Step{Name: "check compose file", Command: checkCmd},
			errors.New("docker-compose.yml was not found; deploy the node first"))
	}

	compose := ""
	if out, err := remote.Run(ctx, sudo+"docker compose version"); err == nil {
		compose = sudo + "docker compose"
		addStep(Step{Name: "detect compose", Command: sudo + "docker compose version", Output: strings.TrimSpace(out)})
	} else if out, err := remote.Run(ctx, sudo+"docker-compose version"); err == nil {
		compose = sudo + "docker-compose"
		addStep(Step{Name: "detect compose", Command: sudo + "docker-compose version", Output: strings.TrimSpace(out)})
	} else {
		return fail(Step{Name: "detect compose", Command: "docker compose version"}, errors.New("docker compose is not available"))
	}

	action := "stop"
	if running {
		action = "up -d"
	}
	cmd := compose + " -f /opt/shampoore/docker-compose.yml " + action
	out, err = remote.Run(ctx, cmd)
	addStep(Step{Name: "compose " + action, Command: cmd, Output: out, Error: errString(err)})
	if err != nil {
		return fail(Step{Name: "compose " + action, Command: cmd}, err)
	}

	psCmd := compose + " -f /opt/shampoore/docker-compose.yml ps"
	out, err = remote.Run(ctx, psCmd)
	addStep(Step{Name: "compose ps", Command: psCmd, Output: out, Error: errString(err)})
	if err != nil {
		return fail(Step{Name: "compose ps", Command: psCmd}, err)
	}

	result.Success = true
	p.logf("provisioner: node %d services running=%v", node.ID, running)
	return result, nil
}

func hasEnabledTransport(node panelstore.Node, kind string) bool {
	for _, tr := range node.Transports {
		if tr.Enabled && tr.Type == kind {
			return true
		}
	}
	return false
}

type CleanupResult struct {
	NodeID  int64  `json:"node_id"`
	Success bool   `json:"success"`
	Steps   []Step `json:"steps"`
}

func (p *Provisioner) CleanupNode(ctx context.Context, node panelstore.Node) (*CleanupResult, error) {
	result := &CleanupResult{NodeID: node.ID}
	addStep := func(step Step) { result.Steps = append(result.Steps, step) }
	fail := func(step Step, err error) (*CleanupResult, error) {
		step.Error = err.Error()
		addStep(step)
		result.Success = false
		return result, err
	}

	dial := p.opts.Dial
	if dial == nil {
		dial = func(ctx context.Context, n panelstore.Node) (Remote, error) {
			return dialSSH(ctx, n, p.opts.HostKeyCallback)
		}
	}
	remote, err := dial(ctx, node)
	if err != nil {
		return fail(Step{Name: "ssh connect", Command: node.SSHHost}, err)
	}
	defer remote.Close()
	addStep(Step{Name: "ssh connect", Output: node.SSHUser + "@" + node.SSHHost})

	sudo := ""
	if out, err := remote.Run(ctx, "id -u"); err == nil && strings.TrimSpace(out) != "0" {
		sudo = "sudo -n "
	}

	if out, err := remote.Run(ctx, sudo+"test -f /opt/shampoore/docker-compose.yml"); err == nil {
		_ = out
		compose := ""
		if out, err := remote.Run(ctx, sudo+"docker compose version"); err == nil {
			compose = sudo + "docker compose"
			addStep(Step{Name: "detect compose", Command: sudo + "docker compose version", Output: strings.TrimSpace(out)})
		} else if out, err := remote.Run(ctx, sudo+"docker-compose version"); err == nil {
			compose = sudo + "docker-compose"
			addStep(Step{Name: "detect compose", Command: sudo + "docker-compose version", Output: strings.TrimSpace(out)})
		}
		if compose != "" {
			downCmd := compose + " -f /opt/shampoore/docker-compose.yml down -v --remove-orphans"
			out, err := remote.Run(ctx, downCmd)
			addStep(Step{Name: "compose down", Command: downCmd, Output: out, Error: errString(err)})
			if err != nil {
				return fail(Step{Name: "compose down", Command: downCmd}, err)
			}
		}
	}

	rmCmd := sudo + "rm -rf /opt/shampoore"
	out, err := remote.Run(ctx, rmCmd)
	addStep(Step{Name: "remove files", Command: rmCmd, Output: out, Error: errString(err)})
	if err != nil {
		return fail(Step{Name: "remove files", Command: rmCmd}, err)
	}

	result.Success = true
	p.logf("provisioner: node %d cleaned up", node.ID)
	return result, nil
}
