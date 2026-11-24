package relay

import (
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const maxDecoyFile = 4 << 20

func (s *Server) SetDecoyDir(dir string) {
	s.decoyMu.Lock()
	s.decoyDir = strings.TrimSpace(dir)
	s.decoyMu.Unlock()
	s.reloadShape()
}

func (s *Server) DecoyDir() string {
	s.decoyMu.Lock()
	defer s.decoyMu.Unlock()
	return s.decoyDir
}

func (s *Server) hasDecoySite() bool {
	dir := s.DecoyDir()
	if dir == "" {
		return false
	}
	info, err := os.Stat(filepath.Join(dir, "index.html"))
	return err == nil && !info.IsDir()
}

func (s *Server) serveDecoy(w http.ResponseWriter, r *http.Request) {
	if !s.hasDecoySite() {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	name := path.Clean("/" + r.URL.Path)
	if name == "/" || name == "." {
		name = "/index.html"
	}
	rel := strings.TrimPrefix(name, "/")
	if rel == "" || strings.Contains(rel, "..") || strings.Contains(rel, `\`) {
		http.NotFound(w, r)
		return
	}

	full := filepath.Join(s.DecoyDir(), filepath.FromSlash(rel))
	info, err := os.Stat(full)
	if err != nil || info.IsDir() || info.Size() > maxDecoyFile {
		http.NotFound(w, r)
		return
	}
	data, err := os.ReadFile(full)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	writeDecoy(w, r, name, data)
}

func writeDecoy(w http.ResponseWriter, r *http.Request, name string, data []byte) {
	ctype := mime.TypeByExtension(path.Ext(name))
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	if strings.HasSuffix(name, ".html") {
		ctype = "text/html; charset=utf-8"
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(data)
}
