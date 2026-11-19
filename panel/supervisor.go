package panel

import (
	"errors"
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
	opMu sync.Mutex
	mu   sync.Mutex

	cmd       *exec.Cmd
	waitCh    chan struct{}
	starting  bool
	startDone chan struct{}
	binary    string

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

func (s *Supervisor) logf(format string, args ...any) {
	if _, err := fmt.Fprintf(s.logRing, "panel: "+format+"\n", args...); err != nil {
		return
	}
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
	dir := "."
	if exe, err := os.Executable(); err == nil {
		dir = filepath.Dir(exe)
	}
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

func (s *Supervisor) binaryPath() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.binary == "" {
		s.binary = s.resolveBinary()
	}
	return s.binary
}

func (s *Supervisor) Start() error {
	s.opMu.Lock()
	defer s.opMu.Unlock()

	if s.IsRunning() {
		return nil
	}

	s.mu.Lock()
	if s.starting {
		s.mu.Unlock()
		return nil
	}
	oldCmd, oldWait := s.cmd, s.waitCh
	s.mu.Unlock()

	if oldCmd != nil && oldCmd.Process != nil {
		s.stopProcess(oldCmd, oldWait)
	}

	if err := os.Remove(s.sockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale socket: %w", err)
	}

	cmd := exec.Command(s.binaryPath(),
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
	done := make(chan struct{})

	s.mu.Lock()
	s.cmd = cmd
	s.waitCh = waitCh
	s.starting = true
	s.startDone = done
	s.mu.Unlock()

	go s.waitProcess(cmd, waitCh, procDone)
	go s.awaitReady(cmd, procDone, done)
	return nil
}

func (s *Supervisor) waitProcess(cmd *exec.Cmd, waitCh chan struct{}, procDone chan<- error) {
	err := cmd.Wait()
	select {
	case procDone <- err:
	default:
	}
	close(waitCh)

	s.mu.Lock()
	if s.cmd == cmd {
		s.cmd = nil
	}
	s.mu.Unlock()
}

func (s *Supervisor) awaitReady(cmd *exec.Cmd, procDone <-chan error, done chan struct{}) {
	defer func() {
		s.mu.Lock()
		s.starting = false
		s.startDone = nil
		s.mu.Unlock()
		close(done)
	}()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case err := <-procDone:
			if err != nil {
				s.logf("server exited before socket was ready: %v", err)
			} else {
				s.logf("server exited before socket was ready")
			}
			return
		case <-timeout.C:
			s.logf("timeout waiting for server socket")
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
				s.logf("signal server: %v", err)
			}
			if err := cmd.Process.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
				s.logf("kill server: %v", err)
			}
			return
		case <-ticker.C:
			if _, err := os.Stat(s.sockPath); err == nil && s.IsRunning() {
				return
			}
		}
	}
}

func (s *Supervisor) stopProcess(cmd *exec.Cmd, waitCh <-chan struct{}) {
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		s.logf("signal server: %v", err)
	}
	if waitCh == nil {
		return
	}
	select {
	case <-waitCh:
	case <-time.After(2 * time.Second):
		if err := cmd.Process.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
			s.logf("kill server: %v", err)
		}
		select {
		case <-waitCh:
		case <-time.After(time.Second):
			s.logf("server process did not exit after SIGKILL")
		}
	}
}

func (s *Supervisor) Stop() error {
	s.opMu.Lock()
	defer s.opMu.Unlock()

	s.mu.Lock()
	cmd, waitCh, startDone := s.cmd, s.waitCh, s.startDone
	s.mu.Unlock()

	if s.IsRunning() {
		c := control.NewClient(s.sockPath)
		if err := c.Shutdown(); err != nil {
			s.logf("shutdown request failed: %v", err)
		}
		for i := 0; i < 50; i++ {
			if !s.IsRunning() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	if cmd != nil && cmd.Process != nil {
		s.stopProcess(cmd, waitCh)
	}
	if startDone != nil {
		select {
		case <-startDone:
		case <-time.After(2 * time.Second):
			s.logf("startup watcher did not finish in time")
		}
	}

	s.mu.Lock()
	s.cmd = nil
	s.waitCh = nil
	s.mu.Unlock()

	if err := os.Remove(s.sockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove socket: %w", err)
	}
	return nil
}
