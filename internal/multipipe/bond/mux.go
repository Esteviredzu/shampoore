package bond

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
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

const recreateDelay = 200 * time.Millisecond

type connManager struct {
	mu      sync.Mutex
	ctx     context.Context
	names   []string
	tubes   []tubeLike
	logger  logging.ContextLogger
	conn    *BondConn
	created time.Time
	closed  bool
	payload int

	retired []uint32

	resetPending bool
}

func newConnManager(
	ctx context.Context,
	names []string,
	tubes []tubeLike,
	logger logging.ContextLogger,
) *connManager {
	return &connManager{
		ctx:     ctx,
		names:   names,
		tubes:   tubes,
		logger:  logger,
		payload: aggregateMaxPayload(tubes),
	}
}

func (cm *connManager) MaxPayload() int { return cm.payload }

func (cm *connManager) acquire() *BondConn {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.closed {
		return cm.conn
	}
	if cm.conn != nil && cm.conn.terminalErr() == nil && !cm.conn.isClosed() {
		return cm.conn
	}
	if cm.ctx.Err() != nil {

		return cm.conn
	}
	if cm.conn != nil {
		_ = cm.conn.Close()
		cm.retireSessionLocked(cm.conn.session)
		cm.conn = nil
	}

	replacing := !cm.created.IsZero()
	if replacing {
		if wait := recreateDelay - time.Since(cm.created); wait > 0 {
			select {
			case <-cm.ctx.Done():
			case <-time.After(wait):
			}
			if cm.ctx.Err() != nil {
				return nil
			}
		}
	}
	cm.conn = newBondConnWithRetired(cm.ctx, cm.names, cm.tubes, cm.logger, cm.retired)
	cm.created = time.Now()
	if replacing {

		cm.resetPending = true
	}
	if cm.logger != nil {
		if replacing {
			cm.logger.Warn("bond: connection recreated over live subflows (session=", cm.conn.session, ")")
		} else {
			cm.logger.Debug("bond: connection started (session=", cm.conn.session, ")")
		}
	}
	return cm.conn
}

func (cm *connManager) retireSessionLocked(session uint32) {
	if session == 0 {
		return
	}
	for _, id := range cm.retired {
		if id == session {
			return
		}
	}
	cm.retired = append(cm.retired, session)
	if len(cm.retired) > maxRetiredSessions {
		cm.retired = cm.retired[len(cm.retired)-maxRetiredSessions:]
	}
}

func (cm *connManager) takeReset() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	pending := cm.resetPending
	cm.resetPending = false
	return pending
}

func (cm *connManager) Close() error {
	cm.mu.Lock()
	cm.closed = true
	conn := cm.conn
	cm.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

type connTransport struct {
	c          *BondConn
	session    *connManager
	maxPayload int
	numTubes   int
}

func (ct *connTransport) Start(context.Context) error { return nil }
func (ct *connTransport) Stop(context.Context) error  { return nil }

func (ct *connTransport) current() *BondConn {
	if ct.session != nil {
		return ct.session.acquire()
	}
	return ct.c
}

func (ct *connTransport) Send(ctx context.Context, data []byte) error {
	conn := ct.current()
	if conn == nil {
		return fmt.Errorf("%w: bond: no connection", transport.ErrTransportReset)
	}
	_, err := conn.Write(data)
	return err
}

func (ct *connTransport) Receive(ctx context.Context) ([]byte, error) {
	conn := ct.current()
	if conn == nil {
		return nil, fmt.Errorf("%w: bond: no connection", transport.ErrTransportReset)
	}
	if ct.session != nil && ct.session.takeReset() {
		return nil, fmt.Errorf("%w: bond: connection recreated", transport.ErrTransportReset)
	}
	buf := make([]byte, ct.maxPayload)
	n, err := conn.ReadContext(ctx, buf)
	if err != nil {
		if ct.session != nil && errors.Is(err, transport.ErrTransportReset) {

			_ = ct.session.acquire()
			ct.session.takeReset()
		}
		return nil, err
	}
	out := make([]byte, n)
	copy(out, buf[:n])
	return out, nil
}

func (ct *connTransport) MaxMessageSize() int {
	return ct.maxPayload
}

func (ct *connTransport) BytesDelivered() int64 { return 0 }
func (ct *connTransport) BytesAcked() int64     { return 0 }

type Mux struct {
	transports []transport.Transport
	names      []string
	logger     logging.ContextLogger

	session *connManager
	inner   multiplexing.StreamMultiplexer

	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	started    bool
	userID     string
	exitNodeID string
	password   string
	active     []transport.Transport
}

func (m *Mux) SetIdentity(userID, exitNodeID string) {
	m.mu.Lock()
	m.userID = userID
	m.exitNodeID = exitNodeID
	m.mu.Unlock()
}

func (m *Mux) SetEncryptionPassword(password string) {
	m.mu.Lock()
	m.password = password
	m.mu.Unlock()
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

func (m *Mux) NotifyNetworkChanged() {
	for _, t := range m.transports {
		if n, ok := t.(interface{ NotifyNetworkChanged() }); ok {
			n.NotifyNetworkChanged()
		}
	}
}

func (m *Mux) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return nil
	}
	m.ctx, m.cancel = context.WithCancel(ctx)

	secured := make([]transport.Transport, len(m.transports))
	for i, t := range m.transports {
		wrapped, err := security.EnsureSecureTransport(t, m.password, security.RoleClient, m.logger)
		if err != nil {
			m.cancel()
			return fmt.Errorf("bond: secure transport %s: %w", m.names[i], err)
		}
		secured[i] = wrapped
	}

	started := make([]bool, len(secured))
	startErrs := make([]error, len(secured))
	var startWg sync.WaitGroup
	for i, t := range secured {
		startWg.Add(1)
		go func(i int, t transport.Transport) {
			defer startWg.Done()
			begin := time.Now()
			if err := t.Start(m.ctx); err != nil {
				startErrs[i] = fmt.Errorf("bond: start transport %s: %w", m.names[i], err)
				return
			}
			started[i] = true
			if m.logger != nil {
				m.logger.Debug("bond: subflow ", m.names[i], " started in ", time.Since(begin))
			}
		}(i, t)
	}
	startWg.Wait()
	for _, err := range startErrs {
		if err == nil {
			continue
		}
		for j := len(secured) - 1; j >= 0; j-- {
			if started[j] {
				_ = secured[j].Stop(context.Background())
			}
		}
		m.cancel()
		return err
	}
	m.active = secured

	tubes := make([]tubeLike, len(secured))
	for i, t := range secured {
		tubes[i] = &transportTube{t: t}
	}

	m.session = newConnManager(m.ctx, m.names, tubes, m.logger)

	ct := &connTransport{
		session:    m.session,
		maxPayload: m.session.MaxPayload(),
		numTubes:   len(secured),
	}
	aggregate := ct.MaxMessageSize()

	cfg := tunnel.Config{
		FlushThreshold: aggregate,
		FlushInterval:  20 * time.Millisecond,
		UserID:         m.userID,
		ExitNodeID:     m.exitNodeID,
	}
	m.inner = tunnel.New(ct, m.logger, cfg)
	if err := m.inner.Start(m.ctx); err != nil {
		_ = m.session.Close()
		m.cancel()
		for _, t := range secured {
			_ = t.Stop(context.Background())
		}
		m.active = nil
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
	if m.session != nil {
		_ = m.session.Close()
	}
	if m.cancel != nil {
		m.cancel()
	}
	active := m.active
	if len(active) == 0 {
		active = m.transports
	}
	m.active = nil
	for i := len(active) - 1; i >= 0; i-- {
		_ = active[i].Stop(ctx)
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
	m.mu.Lock()
	transports := m.transports
	if len(m.active) == len(m.names) {
		transports = m.active
	}
	names := m.names
	m.mu.Unlock()

	out := []transport.Layer{{
		Kind: "mux", Type: "bond",
		Snapshot: transport.Snapshot{Timestamp: time.Now()},
	}}
	for i, t := range transports {
		if tel, ok := t.(transport.Telemetry); ok {
			for _, l := range tel.WalkLayers() {
				if l.Name == "" && i < len(names) {
					l.Name = names[i]
				}
				out = append(out, l)
			}
		}
	}
	return out
}

var _ multiplexing.StreamMultiplexer = (*Mux)(nil)
var _ transport.Telemetry = (*Mux)(nil)
