package security

import (
	"context"
	"errors"

	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

type SecurePipe struct {
	inner        transport.Pipe
	key          []byte
	logger       logging.ContextLogger
	maxPlaintext int
	ctr          transport.Counters
}

func NewSecurePipe(inner transport.Pipe, password string, log logging.ContextLogger) (*SecurePipe, error) {
	key, err := DeriveKey(password)
	if err != nil {
		return nil, err
	}
	maxPlain := inner.MaxMessageSize() - NonceSize - TagSize
	if maxPlain < 1 {
		return nil, errors.New("secure-pipe: inner pipe too small")
	}
	return &SecurePipe{
		inner:        inner,
		key:          key,
		logger:       log,
		maxPlaintext: maxPlain,
	}, nil
}

func (p *SecurePipe) Start(ctx context.Context) error { return p.inner.Start(ctx) }
func (p *SecurePipe) Stop(ctx context.Context) error  { return p.inner.Stop(ctx) }
func (p *SecurePipe) SessionID() string               { return p.inner.SessionID() }
func (p *SecurePipe) MaxMessageSize() int             { return p.maxPlaintext }

func (p *SecurePipe) SendMessage(ctx context.Context, data []byte, key string) error {
	if len(data) > p.maxPlaintext {
		return errors.New("secure-pipe: plaintext too large")
	}
	blob, err := Encrypt(p.key, data)
	if err != nil {
		return err
	}
	if err := p.inner.SendMessage(ctx, blob, key); err != nil {
		return err
	}
	p.ctr.UsefulSent.Add(int64(len(data)))
	p.ctr.NetworkSent.Add(int64(len(blob)))
	p.ctr.MessagesSent.Add(1)
	p.ctr.FramesSent.Add(1)
	return nil
}

func (p *SecurePipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	for {
		blob, err := p.inner.ReceiveMessage(ctx)
		if err != nil {
			return nil, err
		}
		plain, err := Decrypt(p.key, blob)
		if err != nil {
			p.logger.Warn("secure-pipe: dropping undecryptable message: ", err)
			continue
		}
		p.ctr.UsefulRecv.Add(int64(len(plain)))
		p.ctr.NetworkRecv.Add(int64(len(blob)))
		p.ctr.MessagesRecv.Add(1)
		p.ctr.FramesRecv.Add(1)
		return plain, nil
	}
}

func (p *SecurePipe) DeleteMessage(ctx context.Context, key string) error {
	return p.inner.DeleteMessage(ctx, key)
}

func (p *SecurePipe) WalkLayers() []transport.Layer {
	out := []transport.Layer{{
		Kind: "security", Type: "xchacha",
		Snapshot: p.ctr.Snapshot(),
	}}
	if tw, ok := p.inner.(transport.Telemetry); ok {
		out = append(out, tw.WalkLayers()...)
	}
	return out
}

var _ transport.Telemetry = (*SecurePipe)(nil)