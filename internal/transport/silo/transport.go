package silo

import (
	"context"
	"sync/atomic"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

type siloTransport struct {
	pipe           transport.Pipe
	bytesDelivered atomic.Int64
	bytesAcked     atomic.Int64
}

func newTransport(p transport.Pipe) *siloTransport {
	return &siloTransport{pipe: p}
}

func (t *siloTransport) Start(ctx context.Context) error { return t.pipe.Start(ctx) }
func (t *siloTransport) Stop(ctx context.Context) error  { return t.pipe.Stop(ctx) }

func (t *siloTransport) Send(ctx context.Context, data []byte) error {
	if err := t.pipe.SendMessage(ctx, data, ""); err != nil {
		return err
	}
	t.bytesAcked.Add(int64(len(data)))
	return nil
}

func (t *siloTransport) Receive(ctx context.Context) ([]byte, error) {
	data, err := t.pipe.ReceiveMessage(ctx)
	if err == nil {
		t.bytesDelivered.Add(int64(len(data)))
	}
	return data, err
}

func (t *siloTransport) MaxMessageSize() int   { return t.pipe.MaxMessageSize() }
func (t *siloTransport) BytesDelivered() int64 { return t.bytesDelivered.Load() }
func (t *siloTransport) BytesAcked() int64     { return t.bytesAcked.Load() }

func (t *siloTransport) Authenticated() bool {
	if a, ok := t.pipe.(interface{ Authenticated() bool }); ok {
		return a.Authenticated()
	}
	return false
}

func (t *siloTransport) WalkLayers() []transport.Layer {
	if tw, ok := t.pipe.(transport.Telemetry); ok {
		return tw.WalkLayers()
	}
	return nil
}

var _ transport.Transport = (*siloTransport)(nil)
var _ transport.Telemetry = (*siloTransport)(nil)
