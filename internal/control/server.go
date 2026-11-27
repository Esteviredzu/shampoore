package control

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"sync"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/server/manager"
)

type Server struct {
	path       string
	mgr        *manager.Manager
	logger     *logging.Logger
	onShutdown func()

	ln     net.Listener
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewServer(path string, mgr *manager.Manager, logger *logging.Logger, onShutdown func()) *Server {
	return &Server{path: path, mgr: mgr, logger: logger, onShutdown: onShutdown}
}

func (s *Server) Start(ctx context.Context) error {
	_ = os.Remove(s.path)
	ln, err := net.Listen("unix", s.path)
	if err != nil {
		return err
	}
	_ = os.Chmod(s.path, 0o600)

	s.ln = ln
	s.ctx, s.cancel = context.WithCancel(ctx)

	s.wg.Add(1)
	go s.acceptLoop()

	s.logger.Info("control: listening on ", s.path)
	return nil
}

func (s *Server) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.ln != nil {
		_ = s.ln.Close()
	}
	s.wg.Wait()
	_ = os.Remove(s.path)
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			s.logger.Warn("control: accept: ", err)
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(conn)
		}()
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	dec := json.NewDecoder(bufio.NewReader(conn))
	enc := json.NewEncoder(conn)

	var req Request
	if err := dec.Decode(&req); err != nil {
		_ = enc.Encode(Response{OK: false, Error: "decode: " + err.Error()})
		return
	}
	_ = enc.Encode(s.dispatch(req))
}

func (s *Server) dispatch(req Request) Response {
	switch req.Cmd {
	case "shutdown":
		if s.onShutdown != nil {
			go s.onShutdown()
		}
		return ok(nil)
	case "status":
		return ok(map[string]any{"has_dir": s.mgr.HasDir()})
	case "list":
		return ok(s.mgr.List())
	case "stats":
		return ok(s.mgr.Stats())
	case "usage":
		return ok(s.mgr.Usage())
	case "active":
		return ok(s.mgr.ActiveUserIDs())
	case "sync-users":
		if req.Entry == nil {
			return errResp("entry required")
		}
		restarted, err := s.mgr.SyncUsers(req.Name, *req.Entry)
		if err != nil {
			return errResp(err.Error())
		}
		return ok(map[string]bool{"restarted": restarted})
	case "get":
		v, found := s.mgr.Get(req.Name)
		if !found {
			return errResp("not found")
		}
		return ok(v)
	case "add":
		if err := s.mgr.Add(req.Name, req.Template, req.Content); err != nil {
			return errResp(err.Error())
		}
		return ok(nil)
	case "update":
		if req.Entry == nil {
			return errResp("entry required")
		}
		if err := s.mgr.Update(req.Name, *req.Entry); err != nil {
			return errResp(err.Error())
		}
		return ok(nil)
	case "remove":
		if err := s.mgr.Remove(req.Name); err != nil {
			return errResp(err.Error())
		}
		return ok(nil)
	case "enable":
		if err := s.mgr.SetEnabled(req.Name, true); err != nil {
			return errResp(err.Error())
		}
		return ok(nil)
	case "disable":
		if err := s.mgr.SetEnabled(req.Name, false); err != nil {
			return errResp(err.Error())
		}
		return ok(nil)
	case "restart":
		if err := s.mgr.Restart(req.Name); err != nil {
			return errResp(err.Error())
		}
		return ok(nil)
	case "logs":
		lines, found := s.mgr.Logs(req.Name, req.Limit)
		if !found {
			return errResp("not found")
		}
		return ok(map[string]any{"lines": lines})
	default:
		return errResp("unknown command: " + req.Cmd)
	}
}

func ok(v any) Response {
	var raw json.RawMessage
	if v != nil {
		raw, _ = json.Marshal(v)
	}
	return Response{OK: true, Data: raw}
}

func errResp(s string) Response {
	return Response{OK: false, Error: s}
}
