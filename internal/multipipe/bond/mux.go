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

func wrapBondTube(tr transport.Transport, password, role string, logger logging.ContextLogger) (transport.Transport, error) {
	if tr == nil || password == "" {
		return tr, nil
	}
	return security.WrapTransport(tr, password, role, logger)
}

var payloadBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 65536)
		return &b
	},
}

func getPayloadBuf(minSize int) *[]byte {
	bp := payloadBufPool.Get().(*[]byte)
	if cap(*bp) < minSize {
		*bp = make([]byte, minSize)
	}
	*bp = (*bp)[:minSize]
	return bp
}

func putPayloadBuf(bp *[]byte) {
	if bp != nil && cap(*bp) <= 128*1024 {
		payloadBufPool.Put(bp)
	}
}

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

type startResult struct {
	idx int
	err error
}

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

func (cm *connManager) MaxPayload() int {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.payload
}

func (cm *connManager) tubeCount() int {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return len(cm.names)
}

func (cm *connManager) addTube(name string, tube tubeLike) {
	if tube == nil {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.closed {
		return
	}
	found := false
	for i, n := range cm.names {
		if n == name {
			cm.tubes[i] = tube
			found = true
			break
		}
	}
	if !found {
		cm.names = append(cm.names, name)
		cm.tubes = append(cm.tubes, tube)
	}
	cm.payload = aggregateMaxPayload(cm.tubes)
	if cm.conn != nil {
		cm.conn.addSubflow(name, tube)
	}
}

func (cm *connManager) replaceTube(name string, tube tubeLike) {
	cm.addTube(name, tube)
}

func (cm *connManager) dropTube(name string) {
	cm.mu.Lock()
	conn := cm.conn
	closed := cm.closed
	cm.mu.Unlock()
	if closed || conn == nil {
		return
	}
	conn.dropSubflow(name)
}

func (cm *connManager) acquire() *BondConn {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.closed || cm.ctx.Err() != nil {
		return nil
	}
	if cm.conn != nil && cm.conn.terminalErr() == nil && !cm.conn.isClosed() {
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
				return nil
			case <-time.After(wait):
			}
			if cm.ctx.Err() != nil {
				return nil
			}
		}
	}
	if len(cm.tubes) > 0 {
		cm.payload = aggregateMaxPayload(cm.tubes)
	}
	cm.conn = newBondConnWithRetired(cm.ctx, cm.names, cm.tubes, cm.logger, cm.retired)
	if cm.payload > 0 {

		if cur := cm.conn.MaxPayload(); cur <= 0 || cm.payload < cur {
			cm.conn.maxPayload.Store(int64(cm.payload))
		}
	}
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
	_, err := conn.WriteContext(ctx, data)
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
	buf := getPayloadBuf(ct.maxPayload)
	defer putPayloadBuf(buf)
	n, err := conn.ReadContext(ctx, *buf)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		if ct.session != nil {
			_ = ct.session.acquire()
			ct.session.takeReset()
		}
		if !errors.Is(err, transport.ErrTransportReset) {
			err = fmt.Errorf("%w: %v", transport.ErrTransportReset, err)
		}
		return nil, err
	}
	out := make([]byte, n)
	copy(out, (*buf)[:n])
	return out, nil
}

func (ct *connTransport) MaxMessageSize() int {
	if ct.session != nil {
		return ct.session.MaxPayload()
	}
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

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex

	doneMu     sync.Mutex
	done       chan struct{}
	doneClosed bool

	runID     uint64
	started   bool
	starting  bool
	startDone chan struct{}

	bg sync.WaitGroup

	userID      string
	exitNodeID  string
	password    string
	active      []transport.Transport
	activeNames []string
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
		done:       make(chan struct{}),
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
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	if m.started || m.starting {
		m.mu.Unlock()
		return nil
	}
	m.starting = true
	m.runID++
	runID := m.runID
	done := make(chan struct{})
	m.startDone = done
	m.ctx, m.cancel = context.WithCancel(ctx)
	runCtx := m.ctx
	runCancel := m.cancel
	m.doneMu.Lock()
	if m.done == nil || m.doneClosed {
		m.done = make(chan struct{})
		m.doneClosed = false
	}
	m.doneMu.Unlock()
	logger := m.logger
	password := m.password
	userID := m.userID
	exitNodeID := m.exitNodeID
	transports := append([]transport.Transport(nil), m.transports...)
	names := append([]string(nil), m.names...)
	m.mu.Unlock()

	finish := func(err error) error {
		m.mu.Lock()
		if m.runID == runID {
			m.starting = false
			if m.startDone == done {
				m.startDone = nil
			}
			if err != nil {
				m.started = false
			}
		}
		m.mu.Unlock()
		close(done)
		return err
	}

	stopEvery := func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, t := range transports {
			if t != nil {
				_ = t.Stop(stopCtx)
			}
		}
	}

	secured := make([]transport.Transport, len(transports))
	for i, t := range transports {
		wrapped, err := wrapBondTube(t, password, security.RoleClient, logger)
		if err != nil {
			runCancel()
			stopEvery()
			return finish(fmt.Errorf("bond: secure transport %s: %w", names[i], err))
		}
		secured[i] = wrapped
	}

	results := make(chan startResult, len(secured))
	for i := range secured {
		i := i
		go func() {
			begin := time.Now()
			err := secured[i].Start(runCtx)
			if err == nil && logger != nil {
				logger.Debug("bond: subflow ", names[i], " started in ", time.Since(begin))
			}
			results <- startResult{idx: i, err: err}
		}()
	}

	readyNames := make([]string, 0, len(secured))
	readyTubes := make([]tubeLike, 0, len(secured))
	readyTransports := make([]transport.Transport, 0, len(secured))
	pending := len(secured)
	var firstErr error

wait:
	for pending > 0 {
		select {
		case <-runCtx.Done():
			break wait
		case res := <-results:
			pending--
			if res.err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("bond: start transport %s: %w", names[res.idx], res.err)
				}
				if logger != nil {
					logger.Warn("bond: subflow ", names[res.idx], " failed to start: ", res.err)
				}
				stopTransport(secured[res.idx])
				continue
			}
			readyNames = append(readyNames, names[res.idx])
			readyTubes = append(readyTubes, &transportTube{t: secured[res.idx]})
			readyTransports = append(readyTransports, secured[res.idx])
			break wait
		}
	}

	if len(readyNames) == 0 {
		runCancel()
		stopEvery()
		if firstErr == nil {
			if err := runCtx.Err(); err != nil {
				firstErr = fmt.Errorf("%w: bond: start cancelled", err)
			} else {
				firstErr = fmt.Errorf("bond: no subflow could be started")
			}
		}
		return finish(firstErr)
	}

	session := newConnManager(runCtx, readyNames, readyTubes, logger)
	ct := &connTransport{
		session:    session,
		maxPayload: session.MaxPayload(),
		numTubes:   len(secured),
	}
	aggregate := ct.MaxMessageSize()

	cfg := tunnel.Config{
		FlushThreshold: aggregate,
		FlushInterval:  20 * time.Millisecond,
		UserID:         userID,
		ExitNodeID:     exitNodeID,
	}
	inner := tunnel.New(ct, logger, cfg)
	if err := inner.Start(runCtx); err != nil {
		_ = session.Close()
		runCancel()
		stopEvery()
		return finish(fmt.Errorf("bond: tunnel start: %w", err))
	}

	m.mu.Lock()

	if m.runID != runID || m.ctx == nil || m.ctx.Err() != nil {
		m.mu.Unlock()
		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		_ = inner.Stop(stopCtx)
		cancelStop()
		_ = session.Close()
		stopEvery()
		return finish(fmt.Errorf("%w: bond: mux stopped during start", context.Canceled))
	}
	m.session = session
	m.inner = inner
	m.active = append(m.active, readyTransports...)
	m.activeNames = append(m.activeNames, readyNames...)
	m.started = true
	m.mu.Unlock()

	if logger != nil {
		logger.Info("bond: started with ", len(readyNames), "/", len(names), " subflows",
			" per_tube_payload=", ct.maxPayload,
			" aggregate_payload=", aggregate,
			" flush_threshold=", aggregate)
	}

	go m.watchInner(runID, runCtx, inner)

	if pending > 0 {
		m.bg.Add(1)
		go func() {
			defer m.bg.Done()
			m.adoptLateStarts(runID, runCtx, results, pending, secured, names)
		}()
	}
	return finish(nil)
}

func (m *Mux) adoptLateStarts(runID uint64, runCtx context.Context, results <-chan startResult, pending int, secured []transport.Transport, names []string) {
	for i := 0; i < pending; i++ {
		var res startResult
		select {
		case res = <-results:
		case <-runCtx.Done():
			return
		}
		if res.err != nil {
			if m.logger != nil {
				m.logger.Warn("bond: subflow ", names[res.idx], " failed to start: ", res.err)
			}
			stopTransport(secured[res.idx])
			continue
		}
		m.mu.Lock()
		started := m.started && m.runID == runID
		session := m.session
		if started {
			m.active = append(m.active, secured[res.idx])
			m.activeNames = append(m.activeNames, names[res.idx])
		}
		m.mu.Unlock()
		if !started || session == nil {
			stopTransport(secured[res.idx])
			continue
		}
		session.addTube(names[res.idx], &transportTube{t: secured[res.idx]})
		if m.logger != nil {
			m.logger.Info("bond: late subflow ", names[res.idx], " joined the active pool")
		}
	}
}

func (m *Mux) watchInner(runID uint64, runCtx context.Context, inner *tunnel.TunnelMultiplexer) {
	if inner == nil {
		return
	}
	done := inner.Done()
	if done == nil {
		return
	}
	select {
	case <-done:
		m.mu.Lock()
		current := m.started && m.runID == runID && m.inner == inner
		m.mu.Unlock()
		if !current {
			return
		}
		if m.logger != nil {
			m.logger.Warn("bond: inner tunnel session ended; failing the bond multiplexer")
		}
		m.closeDone()
	case <-runCtx.Done():
	}
}

func (m *Mux) Done() <-chan struct{} {
	m.doneMu.Lock()
	defer m.doneMu.Unlock()
	return m.done
}

func (m *Mux) closeDone() {
	m.doneMu.Lock()
	if !m.doneClosed && m.done != nil {
		m.doneClosed = true
		close(m.done)
	}
	m.doneMu.Unlock()
}

func (m *Mux) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	if !m.started && !m.starting {
		m.mu.Unlock()
		return nil
	}
	starting := m.starting
	startDone := m.startDone
	cancel := m.cancel
	runID := m.runID
	inner := m.inner
	session := m.session
	active := append([]transport.Transport(nil), m.active...)
	activeNames := append([]string(nil), m.activeNames...)
	m.inner = nil
	m.session = nil
	m.active = nil
	m.activeNames = nil
	m.started = false
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if starting && startDone != nil {
		select {
		case <-startDone:
		case <-ctx.Done():
			return ctx.Err()
		}

		m.mu.Lock()
		if inner == nil {
			inner = m.inner
		}
		if session == nil {
			session = m.session
		}
		if len(active) == 0 && len(m.active) > 0 {
			active = append(active, m.active...)
			activeNames = append(activeNames, m.activeNames...)
		}
		m.inner = nil
		m.session = nil
		m.active = nil
		m.activeNames = nil
		m.started = false
		m.mu.Unlock()
	}
	m.bg.Wait()

	if inner != nil {
		_ = inner.Stop(ctx)
	}
	if session != nil {
		_ = session.Close()
	}

	seen := make(map[string]struct{}, len(active)+len(m.transports))
	stopOne := func(name string, t transport.Transport) {
		if t == nil {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		_ = t.Stop(ctx)
	}
	for i := len(active) - 1; i >= 0; i-- {
		name := ""
		if i < len(activeNames) {
			name = activeNames[i]
		}
		stopOne(name, active[i])
	}
	for i := len(m.transports) - 1; i >= 0; i-- {
		name := ""
		if i < len(m.names) {
			name = m.names[i]
		}
		stopOne(name, m.transports[i])
	}

	m.mu.Lock()
	if m.runID == runID && !m.starting {
		m.ctx = nil
		m.cancel = nil
	}
	m.mu.Unlock()
	m.closeDone()
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
