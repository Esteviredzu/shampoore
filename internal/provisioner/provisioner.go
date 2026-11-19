package provisioner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
	"golang.org/x/crypto/ssh"
)

// NodeStore is the subset of panelstore used by the provisioner.
type NodeStore interface {
	GetNode(ctx context.Context, id int64) (panelstore.Node, error)
	ListNode(ctx context.Context) ([]panelstore.Node, error)
	ListRoutes(ctx context.Context) ([]panelstore.Route, error)
	ListRoutesForExit(ctx context.Context, exitNodeID int64) ([]panelstore.Route, error)
}

// Options configures rendering and deployment.
type Options struct {
	PanelURL        string
	LocalBinDir     string
	Logger          *logging.Logger
	Dial            func(ctx context.Context, node panelstore.Node) (Remote, error)
	HostKeyCallback ssh.HostKeyCallback
}

// Provisioner renders node configuration and deploys it over SSH.
type Provisioner struct {
	store NodeStore
	opts  Options
}

func New(store NodeStore, opts Options) *Provisioner {
	return &Provisioner{store: store, opts: opts}
}

// Step is one deployment action with captured output.
type Step struct {
	Name    string `json:"name"`
	Command string `json:"command,omitempty"`
	Output  string `json:"output,omitempty"`
	Error   string `json:"error,omitempty"`
}

// DeployResult is returned to the panel and shown to the operator.
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

func (p *Provisioner) render(ctx context.Context, node panelstore.Node) ([]File, error) {
	if !node.IsRelay && !node.IsExit {
		return nil, fmt.Errorf("provisioner: node %q has no role", node.Name)
	}
	if node.IsRelay && !node.IsExit {
		return RenderRelay(node, RenderOptions{PanelURL: p.opts.PanelURL})
	}
	routes, err := p.store.ListRoutesForExit(ctx, node.ID)
	if err != nil {
		return nil, err
	}
	nodes, err := p.store.ListNode(ctx)
	if err != nil {
		return nil, err
	}
	exitFiles, err := RenderExit(node, routes, nodes, RenderOptions{PanelURL: p.opts.PanelURL})
	if err != nil {
		return nil, err
	}
	if !node.IsRelay {
		return exitFiles, nil
	}
	relayFiles, err := RenderRelay(node, RenderOptions{PanelURL: p.opts.PanelURL})
	if err != nil {
		return nil, err
	}
	return mergeRoleFiles(relayFiles, exitFiles)
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

// DeployNode renders the node role and runs the deployment over SSH.
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

	out, err = remote.Run(ctx, sudo+"mkdir -p /opt/shampoore /opt/shampoore/bin /opt/shampoore/configs /opt/shampoore/rules")
	addStep(Step{Name: "prepare directory", Command: "mkdir -p /opt/shampoore", Output: out, Error: errString(err)})
	if err != nil {
		return fail(Step{Name: "prepare directory"}, err)
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

	upCmd := compose + " -f /opt/shampoore/docker-compose.yml up -d --remove-orphans"
	out, err = remote.Run(ctx, upCmd)
	addStep(Step{Name: "compose up", Command: upCmd, Output: out, Error: errString(err)})
	if err != nil {
		return fail(Step{Name: "compose up", Command: upCmd}, err)
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
