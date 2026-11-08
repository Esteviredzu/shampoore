package livekit_datachannel

import (
	"context"
	"sync/atomic"

	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

type directTransport struct {
	pipe transport.Pipe

	bytesSent      atomic.Int64
	bytesDelivered atomic.Int64
}

func newDirectTransport(p transport.Pipe) *directTransport {
	return &directTransport{pipe: p}
}

func (t *directTransport) Start(ctx context.Context) error { return t.pipe.Start(ctx) }
func (t *directTransport) Stop(ctx context.Context) error  { return t.pipe.Stop(ctx) }

func (t *directTransport) Send(ctx context.Context, data []byte) error {
	if err := t.pipe.SendMessage(ctx, data, ""); err != nil {
		return err
	}
	t.bytesSent.Add(int64(len(data)))
	return nil
}

func (t *directTransport) Receive(ctx context.Context) ([]byte, error) {
	data, err := t.pipe.ReceiveMessage(ctx)
	if err == nil {
		t.bytesDelivered.Add(int64(len(data)))
	}
	return data, err
}

func (t *directTransport) MaxMessageSize() int    { return t.pipe.MaxMessageSize() }
func (t *directTransport) BytesDelivered() int64  { return t.bytesDelivered.Load() }
func (t *directTransport) BytesAcked() int64      { return t.bytesSent.Load() }

func (t *directTransport) WalkLayers() []transport.Layer {
	if tw, ok := t.pipe.(transport.Telemetry); ok {
		return tw.WalkLayers()
	}
	return nil
}

var _ transport.Transport = (*directTransport)(nil)
var _ transport.Telemetry = (*directTransport)(nil)