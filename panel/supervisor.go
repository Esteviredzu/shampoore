package panel

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/Esteviredzu/shampoore/internal/control"
	"github.com/Esteviredzu/shampoore/internal/server/logbuf"
)

type ServerManager interface {
	IsRunning() bool
	Start() error
	Stop() error
	Logs(limit int) []string
}

type Supervisor struct {
	mu         sync.Mutex
	cmd        *exec.Cmd
	waitCh     chan struct{}
	sockPath   string
	configsDir string
	rulesDir   string
	logRing    *logbuf.Ring
}

func NewSupervisor(sock, configs, rules string) *Supervisor {
	return &Supervisor{
		sockPath:   sock,
		configsDir: configs,
		rulesDir:   rules,
		logRing:    logbuf.New(2000),
	}
}

func (s *Supervisor) Logs(limit int) []string {
	return s.logRing.Lines(limit)
}

func (s *Supervisor) IsRunning() bool {
	if _, err := os.Stat(s.sockPath); err != nil {
		return false
	}
	c := control.NewClient(s.sockPath)
	_, err := c.Status()
	return err == nil
}

func (s *Supervisor) resolveBinary() string {
	exe, _ := os.Executable()
	dir := filepath.Dir(exe)
	for _, c := range []string{
		filepath.Join(dir, "shampoore-server"),
		"./shampoore-server",
		"../shampoore-server",
		"bin/shampoore-server",
	} {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return "shampoore-server"
}

func (s *Supervisor) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.IsRunning() {
		return nil
	}

	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Signal(syscall.SIGTERM)
		if s.waitCh != nil {
			select {
			case <-s.waitCh:
			case <-time.After(2 * time.Second):
				_ = s.cmd.Process.Signal(syscall.SIGKILL)
			}
		}
		s.cmd = nil
		s.waitCh = nil
	}

	_ = os.Remove(s.sockPath)

	cmd := exec.Command(s.resolveBinary(),
		"--configs-dir", s.configsDir,
		"--control-socket", s.sockPath,
		"--rules-dir", s.rulesDir,
	)
	cmd.Stdout = s.logRing
	cmd.Stderr = s.logRing
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start server: %w", err)
	}

	waitCh := make(chan struct{})
	procDone := make(chan error, 1)
	s.cmd = cmd
	s.waitCh = waitCh

	go func(c *exec.Cmd, ch chan struct{}, done chan error) {
		err := c.Wait()
		select {
		case done <- err:
		default:
		}
		close(ch)
		s.mu.Lock()
		if s.cmd == c {
			s.cmd = nil
		}
		s.mu.Unlock()
	}(cmd, waitCh, procDone)

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(10 * time.Second)

	for {
		select {
		case <-procDone:
			return fmt.Errorf("server exited before socket was ready")
		case <-timeout:
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_ = cmd.Process.Signal(syscall.SIGKILL)
			return fmt.Errorf("timeout waiting for server socket")
		case <-ticker.C:
		}

		if _, err := os.Stat(s.sockPath); err == nil {
			if s.IsRunning() {
				return nil
			}
		}
	}
}

func (s *Supervisor) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cmd := s.cmd
	waitCh := s.waitCh

	if s.IsRunning() {
		c := control.NewClient(s.sockPath)
		_ = c.Shutdown()
		for i := 0; i < 50; i++ {
			if !s.IsRunning() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		if waitCh != nil {
			select {
			case <-waitCh:
			case <-time.After(3 * time.Second):
				_ = cmd.Process.Signal(syscall.SIGKILL)
				select {
				case <-waitCh:
				case <-time.After(time.Second):
				}
			}
		}
	}

	s.cmd = nil
	s.waitCh = nil
	_ = os.Remove(s.sockPath)
	return nil
}
