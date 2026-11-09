package transport

import (
	"context"

	"golang.org/x/time/rate"
)

type rateLimited struct {
	Transport
	lim *rate.Limiter
}

func NewRateLimited(inner Transport, kbPerSec int) Transport {
	if kbPerSec <= 0 {
		return inner
	}
	return &rateLimited{
		Transport: inner,
		lim:       rate.NewLimiter(rate.Limit(kbPerSec*1024), 16384),
	}
}

func (r *rateLimited) Send(ctx context.Context, data []byte) error {
	if err := r.lim.WaitN(ctx, len(data)); err != nil {
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
