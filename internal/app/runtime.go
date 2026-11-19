package app

import (
	"context"
	"errors"
	"sync"

	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type SideService interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type Runtime struct {
	Transport    transport.Transport
	StreamMux    multiplexing.StreamMultiplexer
	Provider     Provider
	BypassHosts  []string
	ConnectedIPs func() []string
	MuxConfig    MuxConfig
	sideServices []SideService

	mu      sync.Mutex
	started bool
}

func NewRuntime(
	t transport.Transport,
	provider Provider,
	bypass []string,
	side []SideService,
) *Runtime {
	return &Runtime{
		Transport:    t,
		Provider:     provider,
		BypassHosts:  bypass,
		sideServices: side,
	}
}

func (r *Runtime) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return nil
	}
	for i, svc := range r.sideServices {
		if err := svc.Start(ctx); err != nil {
			var cleanup []error
			for j := i - 1; j >= 0; j-- {
				if stopErr := r.sideServices[j].Stop(ctx); stopErr != nil {
					cleanup = append(cleanup, stopErr)
				}
			}
			if r.Provider != nil {
				if stopErr := r.Provider.Stop(ctx); stopErr != nil {
					cleanup = append(cleanup, stopErr)
				}
			}
			return errors.Join(append([]error{err}, cleanup...)...)
		}
	}
	r.started = true
	return nil
}

func (r *Runtime) Stop(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	started := r.started
	r.started = false

	var errs []error
	if started {
		for i := len(r.sideServices) - 1; i >= 0; i-- {
			if err := r.sideServices[i].Stop(ctx); err != nil {
				errs = append(errs, err)
			}
		}
	}

	if r.Provider != nil {
		if err := r.Provider.Stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Runtime) NotifyNetworkChanged() {
	if r == nil {
		return
	}
	if n, ok := r.Provider.(interface{ NotifyNetworkChanged() }); ok {
		n.NotifyNetworkChanged()
	}
	if n, ok := r.StreamMux.(interface{ NotifyNetworkChanged() }); ok {
		n.NotifyNetworkChanged()
	}
	if n, ok := r.Transport.(interface{ NotifyNetworkChanged() }); ok {
		n.NotifyNetworkChanged()
	}
}
