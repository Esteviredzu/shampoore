package transport

import (
	"context"

	"golang.org/x/time/rate"
)

type rateLimited struct {
	Transport
	lim   *rate.Limiter
	burst int
}

const defaultRateLimitBurst = 16384

func NewRateLimited(inner Transport, kbPerSec int) Transport {
	if kbPerSec <= 0 {
		return inner
	}

	burst := defaultRateLimitBurst
	if m := inner.MaxMessageSize(); m > burst {
		burst = m
	}
	return &rateLimited{
		Transport: inner,
		lim:       rate.NewLimiter(rate.Limit(kbPerSec*1024), burst),
		burst:     burst,
	}
}

func (r *rateLimited) Send(ctx context.Context, data []byte) error {
	n := len(data)
	if n > r.burst {

		for n > 0 {
			chunk := n
			if chunk > r.burst {
				chunk = r.burst
			}
			if err := r.lim.WaitN(ctx, chunk); err != nil {
				return err
			}
			n -= chunk
		}
	} else if err := r.lim.WaitN(ctx, n); err != nil {
		return err
	}
	return r.Transport.Send(ctx, data)
}

func (r *rateLimited) WalkLayers() []Layer {
	if tw, ok := r.Transport.(Telemetry); ok {
		return tw.WalkLayers()
	}
	return nil
}

func (r *rateLimited) Authenticated() bool {
	if a, ok := r.Transport.(interface{ Authenticated() bool }); ok {
		return a.Authenticated()
	}
	return false
}
