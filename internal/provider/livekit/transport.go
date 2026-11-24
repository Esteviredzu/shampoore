package livekit

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Esteviredzu/shampoore/internal/net/utlsdial"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const livekitAPITimeout = 15 * time.Second

func NewHTTPClient(timeout time.Duration) *http.Client {
	return NewHTTPClientForProfile(timeout, utlsdial.DefaultProfile)
}

func NewHTTPClientForProfile(timeout time.Duration, profile utlsdial.Profile) *http.Client {
	if timeout <= 0 {
		timeout = livekitAPITimeout
	}
	return &http.Client{
		Transport: utlsdial.NewHTTPTransport(profile),
		Timeout:   timeout,
	}
}

func NewSignalDialer() *websocket.Dialer {
	return NewSignalDialerForProfile(utlsdial.DefaultProfile)
}

func NewSignalDialerForProfile(profile utlsdial.Profile) *websocket.Dialer {
	return utlsdial.NewWebSocketDialer(profile)
}

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

func (t *pipeTransport) Authenticated() bool {
	if a, ok := t.pipe.(interface{ Authenticated() bool }); ok {
		return a.Authenticated()
	}
	return false
}

func (t *pipeTransport) WalkLayers() []transport.Layer {
	if tw, ok := t.pipe.(transport.Telemetry); ok {
		return tw.WalkLayers()
	}
	return nil
}

var _ transport.Transport = (*pipeTransport)(nil)
var _ transport.Telemetry = (*pipeTransport)(nil)
