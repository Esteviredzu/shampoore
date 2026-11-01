package multi

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/core/channel/vp8"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/multiplexing"
)

type subflowSpec struct {
	Type      string          `json:"type"`
	Transport json.RawMessage `json:"transport,omitempty"`
	Advanced  app.Values      `json:"advanced,omitempty"`
	Weight    int             `json:"weight,omitempty"`
}

type config struct {
	Transports []subflowSpec `json:"transports"`
}

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "multi",
		DisplayName: "MultiPipe (bonding)",
		DecodeURL:   decodeURL,
		EncodeURL:   encodeURL,
		Defaults: app.Values{
			"mux_flush_ms":        20,
			"mux_flush_threshold": 12000,
		},
		Build: build,
	})
	if err != nil {
		panic(err)
	}
}

type multiProvider struct {
	providers []app.Provider
}

func (p *multiProvider) Start(ctx context.Context) error {
	for i, pr := range p.providers {
		if err := pr.Start(ctx); err != nil {
			for j := i - 1; j >= 0; j-- {
				_ = p.providers[j].Stop(ctx)
			}
			return err
		}
	}
	return nil
}

func (p *multiProvider) Stop(ctx context.Context) error {
	for i := len(p.providers) - 1; i >= 0; i-- {
		_ = p.providers[i].Stop(ctx)
	}
	return nil
}

func (p *multiProvider) Tunnel() *vp8.Tunnel { return nil }

// build constructs one independent transport per configured subflow and
// returns them via BuildResult.Subflows. It does NOT aggregate them into
// a single Transport — the consumer is expected to wrap them in
// multiplexing/multi.MultiStreamMux, which pins each stream to one
// subflow. That is what makes throughput additive without HOL blocking.
//
// Subflows are built in parallel so that N telemost conferences boot in
// max(30s) instead of N*30s.
func build(
	ctx context.Context,
	s *app.Settings,
	role string,
	logger logging.ContextLogger,
	eff app.Values,
) (*app.BuildResult, error) {
	_ = eff // per-subflow defaults are resolved inside each sub-Build

	var cfg config
	if data := s.TransportData(); len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	if len(cfg.Transports) == 0 {
		return nil, fmt.Errorf("multi: at least one transport required")
	}

	type outcome struct {
		idx  int
		res  *app.BuildResult
		name string
		err  error
	}

	done := make(chan outcome, len(cfg.Transports))
	var wg sync.WaitGroup

	for i, spec := range cfg.Transports {
		i, spec := i, spec
		wg.Add(1)
		go func() {
			defer wg.Done()

			if spec.Type == "" {
				done <- outcome{idx: i, err: fmt.Errorf("multi: transport %d: type required", i)}
				return
			}
			d, ok := app.DescriptorFor(spec.Type)
			if !ok {
				done <- outcome{idx: i, err: fmt.Errorf("multi: unknown transport type %q", spec.Type)}
				return
			}
			if d.Kind == "multi" {
				done <- outcome{idx: i, err: fmt.Errorf("multi: nested multi is not supported")}
				return
			}

			sub := &app.Settings{
				Type:      spec.Type,
				Transport: spec.Transport,
				Advanced:  spec.Advanced,
				IPv4Only:  app.IPv4Only,
			}
			subEff := d.Defaults.Merge(sub.Advanced)

			res, err := d.Build(ctx, sub, role, logger, subEff)
			if err != nil {
				done <- outcome{idx: i, err: fmt.Errorf("multi: transport %d (%s): %w", i, spec.Type, err)}
				return
			}
			if res.Transport == nil {
				done <- outcome{idx: i, err: fmt.Errorf("multi: transport %d (%s): build returned no transport", i, spec.Type)}
				return
			}

			w := spec.Weight
			if w <= 0 {
				w = 1
			}
			name := fmt.Sprintf("%d-%s", i, spec.Type)
			done <- outcome{idx: i, res: res, name: name}
		}()
	}
	wg.Wait()
	close(done)

	outcomes := make([]outcome, len(cfg.Transports))
	for o := range done {
		outcomes[o.idx] = o
	}

	// If anything failed, shut down whatever succeeded to avoid leaks.
	var firstErr error
	for _, o := range outcomes {
		if o.err != nil && firstErr == nil {
			firstErr = o.err
		}
	}
	if firstErr != nil {
		for _, o := range outcomes {
			if o.err != nil || o.res == nil {
				continue
			}
			if o.res.Provider != nil {
				_ = o.res.Provider.Stop(context.Background())
			}
			if o.res.Transport != nil {
				_ = o.res.Transport.Stop(context.Background())
			}
		}
		return nil, firstErr
	}

	var (
		subflows  []multiplexing.SubflowTransport
		bypass    []string
		sides     []app.SideService
		providers []app.Provider
		connected []func() []string
	)

	for _, o := range outcomes {
		tr := o.res.Transport
		if s.EncryptionPassword != "" {
			st, err := newSecureTransport(tr, s.EncryptionPassword, logger)
			if err != nil {
				return nil, fmt.Errorf("multi: encrypt %s: %w", o.name, err)
			}
			tr = st
		}

		w := cfg.Transports[o.idx].Weight
		if w <= 0 {
			w = 1
		}

		subflows = append(subflows, multiplexing.SubflowTransport{
			Name:      o.name,
			Transport: tr,
			Weight:    w,
		})

		bypass = append(bypass, o.res.BypassHosts...)
		sides = append(sides, o.res.Side...)
		if o.res.Provider != nil {
			providers = append(providers, o.res.Provider)
		}
		if o.res.ConnectedIPs != nil {
			connected = append(connected, o.res.ConnectedIPs)
		}
	}

	result := &app.BuildResult{
		Subflows:    subflows,
		BypassHosts: bypass,
		Side:        sides,
	}
	if len(providers) > 0 {
		result.Provider = &multiProvider{providers: providers}
	}
	if len(connected) > 0 {
		result.ConnectedIPs = func() []string {
			var out []string
			for _, f := range connected {
				out = append(out, f()...)
			}
			return out
		}
	}
	return result, nil
}