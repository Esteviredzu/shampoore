package multi

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/core/channel/vp8"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
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
		DisplayName: "MultiPipe (aggregation)",
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
		return nil, fmt.Errorf("multi: at least one transport required")
	}

	var subs []Subflow
	var bypass []string
	var sides []app.SideService
	var providers []app.Provider
	var connected []func() []string

	for i, spec := range cfg.Transports {
		if spec.Type == "" {
			return nil, fmt.Errorf("multi: transport %d: type required", i)
		}
		d, ok := app.DescriptorFor(spec.Type)
		if !ok {
			return nil, fmt.Errorf("multi: unknown transport type %q", spec.Type)
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
			return nil, fmt.Errorf("multi: transport %d (%s): %w", i, spec.Type, err)
		}
		weight := spec.Weight
		if weight <= 0 {
			weight = 1
		}
		name := fmt.Sprintf("%d-%s", i, spec.Type)
		subs = append(subs, Subflow{Name: name, Transport: res.Transport, Weight: weight})
		bypass = append(bypass, res.BypassHosts...)
		sides = append(sides, res.Side...)
		if res.Provider != nil {
			providers = append(providers, res.Provider)
		}
		if res.ConnectedIPs != nil {
			connected = append(connected, res.ConnectedIPs)
		}
	}

	mp, err := New(subs, logger)
	if err != nil {
		return nil, err
	}

	var finalTransport transport.Transport = mp
	if s.EncryptionPassword != "" {
		st, err := newSecureTransport(mp, s.EncryptionPassword, logger)
		if err != nil {
			return nil, err
		}
		finalTransport = st
		logger.Info("multi: encryption enabled (outer)")
	}

	result := &app.BuildResult{
		Transport:   finalTransport,
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