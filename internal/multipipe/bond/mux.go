package bond

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/multiplexing"
	"github.com/Esteviredzu/shampoore-core/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

type transportTube struct {
	t transport.Transport
}

func (tt *transportTube) Send(ctx context.Context, data []byte) error {
	return tt.t.Send(ctx, data)
}
func (tt *transportTube) Receive(ctx context.Context) ([]byte, error) {
	return tt.t.Receive(ctx)
}
func (tt *transportTube) MaxMessageSize() int { return tt.t.MaxMessageSize() }

type connTransport struct {
	c          *BondConn
	maxPayload int
	numTubes   int
}

func (ct *connTransport) Start(ctx context.Context) error { return nil }
func (ct *connTransport) Stop(ctx context.Context) error  { return nil }
func (ct *connTransport) Send(ctx context.Context, data []byte) error {
	_, err := ct.c.Write(data)
	return err
}
func (ct *connTransport) Receive(ctx context.Context) ([]byte, error) {
	buf := make([]byte, ct.maxPayload)
	n, err := ct.c.ReadContext(ctx, buf)
	if err != nil {
		return nil, err
	}
	out := make([]byte, n)
	copy(out, buf[:n])
	return out, nil
}

func (ct *connTransport) MaxMessageSize() int {
	return ct.maxPayload * ct.numTubes
}

func (ct *connTransport) BytesDelivered() int64 { return 0 }
func (ct *connTransport) BytesAcked() int64     { return 0 }

type Mux struct {
	transports []transport.Transport
	names      []string
	logger     logging.ContextLogger

	conn  *BondConn
	inner multiplexing.StreamMultiplexer

	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	started bool
}

func NewMux(
	transports []transport.Transport,
	names []string,
	logger logging.ContextLogger,
) (*Mux, error) {
	if len(transports) == 0 {
		return nil, fmt.Errorf("bond: no transports")
	}
	if len(names) != len(transports) {
		return nil, fmt.Errorf("bond: names/transports length mismatch")
	}
	return &Mux{
		transports: transports,
		names:      names,
		logger:     logger,
	}, nil
}

func (m *Mux) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return nil
	}
	m.ctx, m.cancel = context.WithCancel(ctx)

	for i, t := range m.transports {
		if err := t.Start(m.ctx); err != nil {
			for j := i - 1; j >= 0; j-- {
				_ = m.transports[j].Stop(context.Background())
			}
			m.cancel()
			return fmt.Errorf("bond: start transport %s: %w", m.names[i], err)
		}
	}

	tubes := make([]tubeLike, len(m.transports))
	for i, t := range m.transports {
		tubes[i] = &transportTube{t: t}
	}

	m.conn = NewBondConn(m.ctx, m.names, tubes, m.logger)

	ct := &connTransport{
		c:          m.conn,
		maxPayload: m.conn.MaxPayload(),
		numTubes:   len(m.transports),
	}
	aggregate := ct.MaxMessageSize()

	cfg := tunnel.Config{
		FlushThreshold: aggregate,
		FlushInterval:  20 * time.Millisecond,
	}
	m.inner = tunnel.New(ct, m.logger, cfg)
	if err := m.inner.Start(m.ctx); err != nil {
		_ = m.conn.Close()
		m.cancel()
		for _, t := range m.transports {
			_ = t.Stop(context.Background())
		}
		return fmt.Errorf("bond: tunnel start: %w", err)
	}

	m.started = true
	m.logger.Info("bond: started, subflows=", len(m.names),
		" per_tube_payload=", ct.maxPayload,
		" aggregate_payload=", aggregate,
		" flush_threshold=", aggregate)
	return nil
}

func (m *Mux) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		return nil
	}
	m.started = false
	if m.inner != nil {
		_ = m.inner.Stop(ctx)
	}
	if m.conn != nil {
		_ = m.conn.Close()
	}
	if m.cancel != nil {
		m.cancel()
	}
	for i := len(m.transports) - 1; i >= 0; i-- {
		_ = m.transports[i].Stop(ctx)
	}
	return nil
}

func (m *Mux) OpenStream(ctx context.Context, metadata map[string]any) (multiplexing.Stream, error) {
	m.mu.Lock()
	inner := m.inner
	m.mu.Unlock()
	if inner == nil {
		return nil, fmt.Errorf("bond: mux not started")
	}
	return inner.OpenStream(ctx, metadata)
}

func (m *Mux) AcceptStream(ctx context.Context) (multiplexing.Stream, error) {
	m.mu.Lock()
	inner := m.inner
	m.mu.Unlock()
	if inner == nil {
		return nil, fmt.Errorf("bond: mux not started")
	}
	return inner.AcceptStream(ctx)
}

func (m *Mux) WalkLayers() []transport.Layer {
	out := []transport.Layer{{
		Kind: "mux", Type: "bond",
		Snapshot: transport.Snapshot{Timestamp: time.Now()},
	}}
	for i, t := range m.transports {
		if tel, ok := t.(transport.Telemetry); ok {
			for _, l := range tel.WalkLayers() {
				if l.Name == "" {
					l.Name = m.names[i]
				}
				out = append(out, l)
			}
		}
	}
	return out
}

var _ multiplexing.StreamMultiplexer = (*Mux)(nil)
var _ transport.Telemetry = (*Mux)(nil)
