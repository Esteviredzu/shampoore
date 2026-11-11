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
	logRing    *logbuf.Ring
}

func NewSupervisor(sock, configs string) *Supervisor {
	return &Supervisor{
		sockPath:   sock,
		configsDir: configs,
		logRing:    logbuf.New(2000),
	}
}

func (s *Supervisor) Logs(limit int) []string {
	return s.logRing.Lines(limit)
}

func (s *Supervisor) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isRunningLocked()
}

func (s *Supervisor) isRunningLocked() bool {
	if _, err := os.Stat(s.sockPath); err != nil {
		return false
	}
	c := control.NewClient(s.sockPath)
	_, err := c.Status()
	return err == nil
}

func (s *Supervisor) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunningLocked() {
		return nil
	}

	_ = os.Remove(s.sockPath)

	bin := "shampoore-server"
	exe, _ := os.Executable()
	dir := filepath.Dir(exe)

	candidates := []string{
		filepath.Join(dir, "shampoore-server"),
		"./shampoore-server",
		"../shampoore-server",
		"bin/shampoore-server",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			bin = c
			break
		}
	}

	cmd := exec.Command(bin,
		"--configs-dir", s.configsDir,
		"--control-socket", s.sockPath,
	)
	cmd.Stdout = s.logRing
	cmd.Stderr = s.logRing
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start server: %w", err)
	}
	s.cmd = cmd

	waitCh := make(chan struct{})
	s.waitCh = waitCh
	go func(c *exec.Cmd, ch chan struct{}) {
		_ = c.Wait()
		close(ch)
		s.mu.Lock()
		if s.cmd == c {
			s.cmd = nil
		}
		s.mu.Unlock()
	}(cmd, waitCh)

	for i := 0; i < 100; i++ {
		if _, err := os.Stat(s.sockPath); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for server socket")
}

func (s *Supervisor) Stop() error {
	s.mu.Lock()
	cmd := s.cmd
	waitCh := s.waitCh
	s.mu.Unlock()

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

	s.mu.Lock()
	s.cmd = nil
	s.waitCh = nil
	s.mu.Unlock()

	_ = os.Remove(s.sockPath)
	return nil
}
