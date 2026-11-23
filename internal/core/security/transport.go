package security

import (
	"context"
	"errors"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type transportPipe struct {
	t transport.Transport
}

func (p *transportPipe) Start(ctx context.Context) error { return p.t.Start(ctx) }
func (p *transportPipe) Stop(ctx context.Context) error  { return p.t.Stop(ctx) }

func (p *transportPipe) SendMessage(ctx context.Context, data []byte, _ string) error {
	return p.t.Send(ctx, data)
}

func (p *transportPipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	return p.t.Receive(ctx)
}

func (p *transportPipe) DeleteMessage(context.Context, string) error { return nil }
func (p *transportPipe) SessionID() string                           { return "" }
func (p *transportPipe) MaxMessageSize() int                         { return p.t.MaxMessageSize() }

func (p *transportPipe) WalkLayers() []transport.Layer {
	if tw, ok := p.t.(transport.Telemetry); ok {
		return tw.WalkLayers()
	}
	return nil
}

type secureTransport struct {
	pipe  transport.Pipe
	inner transport.Transport
}

func (t *secureTransport) Start(ctx context.Context) error { return t.pipe.Start(ctx) }
func (t *secureTransport) Stop(ctx context.Context) error  { return t.pipe.Stop(ctx) }

func (t *secureTransport) Send(ctx context.Context, data []byte) error {
	return t.pipe.SendMessage(ctx, data, "")
}

func (t *secureTransport) Receive(ctx context.Context) ([]byte, error) {
	return t.pipe.ReceiveMessage(ctx)
}

func (t *secureTransport) MaxMessageSize() int   { return t.pipe.MaxMessageSize() }
func (t *secureTransport) BytesDelivered() int64 { return t.inner.BytesDelivered() }
func (t *secureTransport) BytesAcked() int64     { return t.inner.BytesAcked() }

func (t *secureTransport) Authenticated() bool { return true }

func (t *secureTransport) WalkLayers() []transport.Layer {
	if tw, ok := t.pipe.(transport.Telemetry); ok {
		return tw.WalkLayers()
	}
	return nil
}

func WrapTransport(inner transport.Transport, password, role string, logger logging.ContextLogger) (transport.Transport, error) {
	if inner == nil {
		return nil, errors.New("secure-pipe: inner transport is nil")
	}
	pipe, err := NewSecurePipeWithRole(&transportPipe{t: inner}, password, role, logger)
	if err != nil {
		return nil, err
	}
	return &secureTransport{pipe: pipe, inner: inner}, nil
}

func IsAuthenticated(tr transport.Transport) bool {
	a, ok := tr.(interface{ Authenticated() bool })
	return ok && a.Authenticated()
}

func EnsureSecureTransport(inner transport.Transport, password, role string, logger logging.ContextLogger) (transport.Transport, error) {
	if inner == nil || password == "" || IsAuthenticated(inner) {
		return inner, nil
	}
	return WrapTransport(inner, password, role, logger)
}

var _ transport.Transport = (*secureTransport)(nil)
var _ transport.Telemetry = (*secureTransport)(nil)
var _ transport.Pipe = (*transportPipe)(nil)
