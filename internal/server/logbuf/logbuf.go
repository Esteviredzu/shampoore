package logbuf

import (
	"bytes"
	"sync"
)

type Ring struct {
	mu    sync.Mutex
	lines []string
	max   int
	buf   []byte
}

func New(max int) *Ring {
	if max <= 0 {
		max = 500
	}
	return &Ring{max: max, lines: make([]string, 0, max)}
}

func (r *Ring) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(p)
	for len(p) > 0 {
		idx := bytes.IndexByte(p, '\n')
		if idx < 0 {
			r.buf = append(r.buf, p...)
			break
		}
		if len(r.buf) == 0 {
			r.push(string(p[:idx]))
		} else {
			r.buf = append(r.buf, p[:idx]...)
			r.push(string(r.buf))
			r.buf = r.buf[:0]
		}
		p = p[idx+1:]
	}
	return n, nil
}

func (r *Ring) push(line string) {
	r.lines = append(r.lines, line)
	if len(r.lines) > r.max {
		r.lines = r.lines[1:]
	}
}

func (r *Ring) Lines(limit int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.lines)
	if limit <= 0 || limit > n {
		limit = n
	}
	out := make([]string, limit)
	copy(out, r.lines[n-limit:])
	return out
}
