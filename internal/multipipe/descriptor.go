package multipipe

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/core/channel/vp8"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/multipipe/iods"
)

type subflowSpec struct {
	Type      string          `json:"type"`
	Transport json.RawMessage `json:"transport,omitempty"`
	Advanced  app.Values      `json:"advanced,omitempty"`
}

type config struct {
	Transports []subflowSpec `json:"transports"`
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
	_ = eff

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
		subflows  []iods.Subflow
		bypass    []string
		sides     []app.SideService
		providers []app.Provider
		connected []func() []string
	)

	for _, o := range outcomes {
		subflows = append(subflows, iods.Subflow{
			Name:      o.name,
			Transport: o.res.Transport,
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

	if len(subflows) == 1 {
		result.Transport = subflows[0].Transport
		return result, nil
	}

	bonded, err := iods.New(iods.Config{
		Subflows: subflows,
		Logger:   logger,
	})
	if err != nil {
		stopAll(outcomes)
		return nil, fmt.Errorf("multipipe: build iods: %w", err)
	}
	result.Transport = bonded

	return result, nil
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
