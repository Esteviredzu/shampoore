package panel

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

const (
	caddyConfFile    = "shampoore-sub.caddy"
	caddyBlockBegin  = "# BEGIN shampoore managed block"
	caddyBlockEnd    = "# END shampoore managed block"
	defaultCaddyfile = "/etc/caddy/Caddyfile"
	defaultCaddyConf = "/etc/caddy/conf.d"
)

type CaddySite struct {
	Domain   string `json:"domain"`
	Upstream string `json:"upstream"`
}

type CaddyStatus struct {
	Applied bool     `json:"applied"`
	Message string   `json:"message,omitempty"`
	Files   []string `json:"files,omitempty"`
}

type CaddySyncer interface {
	Sync(ctx context.Context, sites []CaddySite) (CaddyStatus, error)
}

type localCaddy struct {
	caddyfile string
	confDir   string
	readFile  func(string) ([]byte, error)
	writeFile func(string, []byte) error
	mkdirAll  func(string, os.FileMode) error
	run       func(ctx context.Context, name string, args ...string) (string, error)
}

func NewLocalCaddy() CaddySyncer {
	return &localCaddy{
		caddyfile: defaultCaddyfile,
		confDir:   defaultCaddyConf,
		readFile:  os.ReadFile,
		writeFile: writeFileAtomic,
		mkdirAll:  os.MkdirAll,
		run:       runCommand,
	}
}

func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".shampoore-caddy-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Chmod(0o644); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func runCommand(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (c *localCaddy) Sync(ctx context.Context, sites []CaddySite) (CaddyStatus, error) {
	normalized, err := normalizeCaddySites(sites)
	if err != nil {
		return CaddyStatus{Applied: false, Message: err.Error()}, err
	}
	body := renderCaddySites(normalized)

	existing, err := c.readFile(c.caddyfile)
	if err != nil {
		if os.IsNotExist(err) {
			return CaddyStatus{
				Applied: false,
				Message: "Caddy is not installed on this host (no " + c.caddyfile + "); configure HTTPS manually",
			}, nil
		}
		return CaddyStatus{Applied: false, Message: err.Error()}, err
	}

	status := CaddyStatus{Applied: true}
	var written string
	if hasCaddyImport(string(existing)) {
		path := filepath.Join(c.confDir, caddyConfFile)
		if strings.TrimSpace(body) == "" {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return CaddyStatus{Applied: false, Message: err.Error()}, err
			}
			written = path
		} else {
			if err := c.mkdirAll(c.confDir, 0o755); err != nil {
				return CaddyStatus{Applied: false, Message: err.Error()}, err
			}
			if err := c.writeFile(path, []byte(body)); err != nil {
				return CaddyStatus{Applied: false, Message: err.Error()}, err
			}
			written = path
		}
		if cleaned, changed := stripCaddyBlock(string(existing)); changed {
			if err := c.writeFile(c.caddyfile, []byte(cleaned)); err != nil {
				status.Message = "drop-in written, but the legacy managed block could not be removed: " + err.Error()
			}
		}
	} else {
		updated := replaceCaddyBlock(string(existing), body)
		if err := c.writeFile(c.caddyfile, []byte(updated)); err != nil {
			return CaddyStatus{Applied: false, Message: err.Error()}, err
		}
		written = c.caddyfile
	}
	if written != "" {
		status.Files = append(status.Files, written)
	}

	if err := c.reload(ctx); err != nil {
		status.Message = "configuration written, but Caddy reload failed: " + err.Error()
		return status, nil
	}
	status.Message = "Caddy configuration applied and reloaded"
	return status, nil
}

func (c *localCaddy) reload(ctx context.Context) error {
	out, err := c.run(ctx, "systemctl", "reload", "caddy")
	if err == nil {
		return nil
	}
	first := strings.TrimSpace(out)
	sudoOut, sudoErr := c.run(ctx, "sudo", "-n", "systemctl", "reload", "caddy")
	if sudoErr == nil {
		return nil
	}

	if adminErr := c.reloadViaAdminAPI(ctx); adminErr == nil {
		return nil
	} else {
		return fmt.Errorf("%v (%s); sudo fallback: %v (%s); admin API: %v",
			err, first, sudoErr, strings.TrimSpace(sudoOut), adminErr)
	}
}

func (c *localCaddy) reloadViaAdminAPI(ctx context.Context) error {
	data, err := c.readFile(c.caddyfile)
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, "http://127.0.0.1:2019/load", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/caddyfile")
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("admin API returned %s", resp.Status)
	}
	return nil
}

func normalizeCaddySites(sites []CaddySite) ([]CaddySite, error) {
	byDomain := make(map[string]string, len(sites))
	for _, site := range sites {
		domain, err := panelstore.NormalizeDomain(site.Domain)
		if err != nil {
			return nil, err
		}
		if domain == "" {
			continue
		}
		upstream := strings.TrimSpace(site.Upstream)
		if upstream == "" {
			return nil, fmt.Errorf("panel: caddy site %q has no upstream", domain)
		}
		if existing, ok := byDomain[domain]; ok && existing != upstream {
			return nil, fmt.Errorf("panel: domain %q points to both %s and %s", domain, existing, upstream)
		}
		byDomain[domain] = upstream
	}
	out := make([]CaddySite, 0, len(byDomain))
	for domain, upstream := range byDomain {
		out = append(out, CaddySite{Domain: domain, Upstream: upstream})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Domain < out[j].Domain })
	return out, nil
}

func renderCaddySites(sites []CaddySite) string {
	var b strings.Builder
	for _, site := range sites {
		b.WriteString(site.Domain)
		b.WriteString(" {\n\treverse_proxy ")
		b.WriteString(site.Upstream)
		b.WriteString("\n}\n")
	}
	return b.String()
}

func hasCaddyImport(data string) bool {
	for _, line := range strings.Split(data, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "import ") && strings.Contains(line, "conf.d/") {
			return true
		}
	}
	return false
}

func replaceCaddyBlock(data, body string) string {
	cleaned, _ := stripCaddyBlock(data)
	cleaned = strings.TrimRight(cleaned, "\n")
	body = strings.TrimRight(strings.TrimSpace(body), "\n")
	if body == "" {
		return cleaned + "\n"
	}
	if cleaned == "" {
		return caddyBlockBegin + "\n" + body + "\n" + caddyBlockEnd + "\n"
	}
	return cleaned + "\n\n" + caddyBlockBegin + "\n" + body + "\n" + caddyBlockEnd + "\n"
}

func stripCaddyBlock(data string) (string, bool) {
	start := strings.Index(data, caddyBlockBegin)
	if start < 0 {
		return data, false
	}
	endRel := strings.Index(data[start:], caddyBlockEnd)
	if endRel < 0 {
		return data, false
	}
	end := start + endRel + len(caddyBlockEnd)
	cleaned := strings.TrimRight(data[:start]+data[end:], "\n")
	if cleaned != "" {
		cleaned += "\n"
	}
	return cleaned, true
}

func localUpstream(listen string) string {
	host, port, err := net.SplitHostPort(strings.TrimSpace(listen))
	if err != nil {
		return ""
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}
