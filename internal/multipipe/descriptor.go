package multipipe

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/core/channel/vp8"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multipipe/bond"
	"github.com/Esteviredzu/shampoore/internal/multipipe/flowmux"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type subflowSpec struct {
	Type        string          `json:"type"`
	Transport   json.RawMessage `json:"transport,omitempty"`
	Advanced    app.Values      `json:"advanced,omitempty"`
	RateLimitKB int             `json:"rate_limit_kb,omitempty"`
}

type config struct {
	Transports []subflowSpec `json:"transports"`

	Strategy string `json:"strategy,omitempty"`
}

type outcome struct {
	idx  int
	res  *app.BuildResult
	name string
	err  error
}

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "multi",
		DisplayName: "MultiPipe",
		Description: "Aggregate multiple transports",
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

func build(
	ctx context.Context,
	s *app.Settings,
	role string,
	logger logging.ContextLogger,
	eff app.Values,
) (*app.BuildResult, error) {
	var cfg config
	if data := s.TransportData(); len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	if len(cfg.Transports) == 0 {
		return nil, fmt.Errorf("multipipe: at least one transport required")
	}

	outcomes := buildAll(ctx, s, role, logger, cfg)

	var firstErr error
	for _, o := range outcomes {
		if o.err != nil && firstErr == nil {
			firstErr = o.err
		}
	}
	if firstErr != nil {
		stopAll(outcomes)
		return nil, firstErr
	}

	var (
		transports []transport.Transport
		names      []string
		bypass     []string
		sides      []app.SideService
		providers  []app.Provider
		connected  []func() []string
	)

	for _, o := range outcomes {
		transports = append(transports, o.res.Transport)
		names = append(names, o.name)
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

	switch cfg.Strategy {
	case "", "flowmux":
		fmCfg := flowmux.DefaultConfig()
		fmCfg.FlushThreshold = eff.Int("mux_flush_threshold", 12000)
		fmCfg.FlushInterval = time.Duration(eff.Int("mux_flush_ms", 20)) * time.Millisecond

		fm, err := flowmux.New(transports, names, logger, fmCfg)
		if err != nil {
			stopAll(outcomes)
			return nil, fmt.Errorf("multipipe: flowmux: %w", err)
		}
		result.StreamMux = fm
		logger.Info("multipipe: flowmux enabled, subflows=", len(transports))
		return result, nil

	case "bond":
		bm, err := bond.NewMux(transports, names, logger)
		if err != nil {
			stopAll(outcomes)
			return nil, fmt.Errorf("multipipe: bond: %w", err)
		}
		result.StreamMux = bm
		logger.Info("multipipe: bond enabled, subflows=", len(transports))
		return result, nil

	default:
		stopAll(outcomes)
		return nil, fmt.Errorf("multipipe: unknown strategy %q (want flowmux|bond)", cfg.Strategy)
	}
}

func buildAll(
	ctx context.Context,
	s *app.Settings,
	role string,
	logger logging.ContextLogger,
	cfg config,
) []outcome {
	done := make(chan outcome, len(cfg.Transports))
	var wg sync.WaitGroup

	for i, spec := range cfg.Transports {
		i, spec := i, spec
		wg.Add(1)
		go func() {
			defer wg.Done()
			done <- buildOne(ctx, s, role, logger, i, spec)
		}()
	}

	wg.Wait()
	close(done)

	outcomes := make([]outcome, len(cfg.Transports))
	for o := range done {
		outcomes[o.idx] = o
	}
	return outcomes
}

func buildOne(
	ctx context.Context,
	s *app.Settings,
	role string,
	logger logging.ContextLogger,
	i int,
	spec subflowSpec,
) outcome {
	if spec.Type == "" {
		return outcome{idx: i, err: fmt.Errorf("multipipe: transport %d: type required", i)}
	}
	d, ok := app.DescriptorFor(spec.Type)
	if !ok {
		return outcome{idx: i, err: fmt.Errorf("multipipe: unknown transport type %q", spec.Type)}
	}
	if d.Kind == "multi" {
		return outcome{idx: i, err: fmt.Errorf("multipipe: nested multi is not supported")}
	}

	sub := &app.Settings{
		Type:               spec.Type,
		Transport:          spec.Transport,
		Advanced:           spec.Advanced,
		EncryptionPassword: s.EncryptionPassword,
		IPv4Only:           app.IPv4Only,
	}
	subEff := d.Defaults.Merge(sub.Advanced)

	res, err := d.Build(ctx, sub, role, logger, subEff)
	if err != nil {
		return outcome{idx: i, err: fmt.Errorf("multipipe: transport %d (%s): %w", i, spec.Type, err)}
	}
	if res.Transport == nil {
		return outcome{idx: i, err: fmt.Errorf("multipipe: transport %d (%s): build returned no transport", i, spec.Type)}
	}

	if spec.RateLimitKB > 0 {
		logger.Info("multipipe: subflow ", i, " (", spec.Type,
			") rate-limited to ", spec.RateLimitKB, " KB/s")
		res.Transport = transport.NewRateLimited(res.Transport, spec.RateLimitKB)
	}

	return outcome{
		idx:  i,
		res:  res,
		name: fmt.Sprintf("%d-%s", i, spec.Type),
	}
}

func stopAll(outcomes []outcome) {
	for _, o := range outcomes {
		if o.res == nil {
			continue
		}
		if o.res.Provider != nil {
			_ = o.res.Provider.Stop(context.Background())
		}
		if o.res.Transport != nil {
			_ = o.res.Transport.Stop(context.Background())
		}
	}
}
