package multipipe

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/app"
)

type composer struct{}

func (composer) Strategies() []app.MultiStrategy {
	return []app.MultiStrategy{
		{ID: "flowmux", Label: "flowmux"},
		{ID: "bond", Label: "bond"},
	}
}

func (composer) Validate(req app.MultiRequest) error {
	if err := validateStrategy(req.Strategy); err != nil {
		return err
	}
	if len(req.Subflows) == 0 {
		return fmt.Errorf("multipipe: at least one transport required")
	}
	for i, sf := range req.Subflows {
		if strings.TrimSpace(sf.Type) == "" {
			return fmt.Errorf("multipipe: transport %d: type required", i)
		}
		d, ok := app.DescriptorFor(sf.Type)
		if !ok {
			return fmt.Errorf("multipipe: unknown transport type %q", sf.Type)
		}
		if d.Caps.Multi != nil {
			return fmt.Errorf("multipipe: nested multi is not supported")
		}
	}
	return nil
}

func (composer) BuildSubscription(req app.MultiRequest) (app.Settings, error) {
	return buildMultiSettings(req, false)
}

func (composer) BuildExit(req app.MultiRequest) (app.Settings, error) {
	return buildMultiSettings(req, true)
}

func buildMultiSettings(req app.MultiRequest, server bool) (app.Settings, error) {
	c := composer{}
	if err := c.Validate(req); err != nil {
		return app.Settings{}, err
	}
	cfg := config{Strategy: strings.TrimSpace(req.Strategy)}
	for _, sf := range req.Subflows {
		cfg.Transports = append(cfg.Transports, subflowSpec{
			Type:        sf.Type,
			Transport:   sf.Transport,
			Advanced:    sf.Advanced,
			RateLimitKB: sf.RateLimitKB,
		})
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return app.Settings{}, err
	}
	out := app.Settings{
		Name:               req.RouteName,
		Type:               "multi",
		ExitNodeID:         req.ExitNodeID,
		EncryptionPassword: req.EncryptionPassword,
		Transport:          raw,
	}
	if !server {
		out.UserID = req.UserID
	}
	return out, nil
}

func validateStrategy(strategy string) error {
	switch strings.TrimSpace(strategy) {
	case "", "flowmux", "bond":
		return nil
	default:
		return fmt.Errorf("multipipe: unknown strategy %q (want flowmux|bond)", strategy)
	}
}
