package logbuf

import "sync"

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
	for _, b := range p {
		if b == '\n' {
			if len(r.buf) > 0 {
				r.lines = append(r.lines, string(r.buf))
				if len(r.lines) > r.max {
					r.lines = r.lines[1:]
				}
				r.buf = r.buf[:0]
			}
		} else {
			r.buf = append(r.buf, b)
		}
	}
	return len(p), nil
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