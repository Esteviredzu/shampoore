package multi

import (
	"context"
	"errors"

	"github.com/Esteviredzu/shampoore-core/internal/core/security"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

type secureTransport struct {
	inner  transport.Transport
	key    []byte
	logger logging.ContextLogger
	max    int
}

func newSecureTransport(inner transport.Transport, password string, logger logging.ContextLogger) (*secureTransport, error) {
	key, err := security.DeriveKey(password)
	if err != nil {
		return nil, err
	}
	max := inner.MaxMessageSize() - security.NonceSize - security.TagSize
	if max < 1 {
		return nil, errors.New("multi/secure: inner too small")
	}
	return &secureTransport{inner: inner, key: key, logger: logger, max: max}, nil
}

func (s *secureTransport) Start(ctx context.Context) error { return s.inner.Start(ctx) }
func (s *secureTransport) Stop(ctx context.Context) error  { return s.inner.Stop(ctx) }
func (s *secureTransport) MaxMessageSize() int             { return s.max }
func (s *secureTransport) BytesDelivered() int64           { return s.inner.BytesDelivered() }
func (s *secureTransport) BytesAcked() int64               { return s.inner.BytesAcked() }

func (s *secureTransport) Send(ctx context.Context, data []byte) error {
	if len(data) > s.max {
		return errors.New("multi/secure: too large")
	}
	enc, err := security.Encrypt(s.key, data)
	if err != nil {
		return err
	}
	return s.inner.Send(ctx, enc)
}

func (s *secureTransport) Receive(ctx context.Context) ([]byte, error) {
	for {
		enc, err := s.inner.Receive(ctx)
		if err != nil {
			return nil, err
		}
		dec, err := security.Decrypt(s.key, enc)
		if err != nil {
			s.logger.Warn("multi/secure: decrypt fail: ", err)
			continue
		}
		return dec, nil
	}
}

var _ transport.Transport = (*secureTransport)(nil)