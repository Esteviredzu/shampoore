package multipipe

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/app"
)

func decodeURL(q url.Values) (json.RawMessage, error) {
	indices := collectIndices(q)
	if len(indices) == 0 {
		return nil, errors.New("multipipe: no subflows found (no t<N>.* keys)")
	}

	var cfg config
	if s := q.Get("strategy"); s != "" {
		cfg.Strategy = s
	}

	for _, n := range indices {
		prefix := fmt.Sprintf("t%d.", n)
		subQ := url.Values{}
		for key, vals := range q {
			if strings.HasPrefix(key, prefix) {
				subQ[key[len(prefix):]] = vals
			}
		}
		spec, err := parseSubflow(subQ)
		if err != nil {
			return nil, fmt.Errorf("multipipe: subflow %d: %w", n, err)
		}
		cfg.Transports = append(cfg.Transports, spec)
	}

	out, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("multipipe: marshal: %w", err)
	}
	return json.RawMessage(out), nil
}

func collectIndices(q url.Values) []int {
	seen := make(map[int]bool)
	for key := range q {
		if !strings.HasPrefix(key, "t") {
			continue
		}
		rest := key[1:]
		dot := strings.Index(rest, ".")
		if dot <= 0 {
			continue
		}
		n, err := strconv.Atoi(rest[:dot])
		if err != nil {
			continue
		}
		seen[n] = true
	}
	out := make([]int, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}

func parseSubflow(q url.Values) (subflowSpec, error) {
	var spec subflowSpec

	spec.Type = q.Get("type")
	if spec.Type == "" {
		return spec, errors.New("type required")
	}

	if rl := q.Get("rl"); rl != "" {
		if n, err := strconv.Atoi(rl); err == nil && n > 0 {
			spec.RateLimitKB = n
		}
	}

	adv := app.NewValues()
	transportQ := url.Values{}
	for key, vals := range q {
		switch {
		case key == "type" || key == "rl":
			continue
		case strings.HasPrefix(key, "a."):
			if len(vals) > 0 {
				adv[strings.TrimPrefix(key, "a.")] = vals[0]
			}
		default:
			transportQ[key] = vals
		}
	}
	if len(adv) > 0 {
		spec.Advanced = adv
	}

	d, ok := app.DescriptorFor(spec.Type)
	if !ok {
		return spec, fmt.Errorf("unknown transport type %q", spec.Type)
	}
	if d.Caps.Link == nil && d.DecodeURL == nil {
		return spec, fmt.Errorf("transport type %q does not support URL encoding", spec.Type)
	}
	raw, err := d.DecodeLink(transportQ)
	if err != nil {
		return spec, fmt.Errorf("decode %q: %w", spec.Type, err)
	}
	spec.Transport = raw

	return spec, nil
}

func encodeURL(raw json.RawMessage, q url.Values) error {
	if len(raw) == 0 {
		return errors.New("multipipe: empty transport")
	}
	var cfg config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	if len(cfg.Transports) == 0 {
		return errors.New("multipipe: no transports")
	}

	if cfg.Strategy != "" {
		q.Set("strategy", cfg.Strategy)
	}

	for i, spec := range cfg.Transports {
		prefix := fmt.Sprintf("t%d.", i)

		q.Set(prefix+"type", spec.Type)

		if spec.RateLimitKB > 0 {
			q.Set(prefix+"rl", strconv.Itoa(spec.RateLimitKB))
		}

		for k, v := range spec.Advanced {
			if k == "" || v == nil {
				continue
			}
			q.Set(prefix+"a."+k, app.EncodeValue(v))
		}

		if len(spec.Transport) == 0 {
			continue
		}

		d, ok := app.DescriptorFor(spec.Type)
		if !ok {
			return fmt.Errorf("multipipe: subflow %d: unknown type %q", i, spec.Type)
		}
		if d.Caps.Link == nil && d.EncodeURL == nil {
			return fmt.Errorf("multipipe: subflow %d: type %q does not support URL encoding", i, spec.Type)
		}
		subQ := url.Values{}
		if err := d.EncodeLink(spec.Transport, subQ); err != nil {
			return fmt.Errorf("multipipe: subflow %d: %w", i, err)
		}
		for key, vals := range subQ {
			if len(vals) > 0 {
				q.Set(prefix+key, vals[0])
			}
		}
	}
	return nil
}
