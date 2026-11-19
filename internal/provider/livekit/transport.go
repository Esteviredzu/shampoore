package livekit

import (
	"context"
	"sync/atomic"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

type pipeTransport struct {
	pipe transport.Pipe

	bytesSent      atomic.Int64
	bytesDelivered atomic.Int64
}

func NewTransport(p transport.Pipe) transport.Transport {
	return &pipeTransport{pipe: p}
}

func (t *pipeTransport) Start(ctx context.Context) error { return t.pipe.Start(ctx) }
func (t *pipeTransport) Stop(ctx context.Context) error  { return t.pipe.Stop(ctx) }

func (t *pipeTransport) Send(ctx context.Context, data []byte) error {
	if err := t.pipe.SendMessage(ctx, data, ""); err != nil {
		return err
	}
	t.bytesSent.Add(int64(len(data)))
	return nil
}

func (t *pipeTransport) Receive(ctx context.Context) ([]byte, error) {
	data, err := t.pipe.ReceiveMessage(ctx)
	if err == nil {
		t.bytesDelivered.Add(int64(len(data)))
	}
	return data, err
}

func (t *pipeTransport) MaxMessageSize() int   { return t.pipe.MaxMessageSize() }
func (t *pipeTransport) BytesDelivered() int64 { return t.bytesDelivered.Load() }
func (t *pipeTransport) BytesAcked() int64     { return t.bytesSent.Load() }

func (t *pipeTransport) WalkLayers() []transport.Layer {
	if tw, ok := t.pipe.(transport.Telemetry); ok {
		return tw.WalkLayers()
	}
	return nil
}

var _ transport.Transport = (*pipeTransport)(nil)
var _ transport.Telemetry = (*pipeTransport)(nil)
