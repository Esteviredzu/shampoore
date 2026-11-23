package bond

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/transport"
)

type testLogger struct{}

func (testLogger) Trace(...any)                         {}
func (testLogger) Debug(...any)                         {}
func (testLogger) Info(...any)                          {}
func (testLogger) Warn(...any)                          {}
func (testLogger) Error(...any)                         {}
func (testLogger) Fatal(...any)                         {}
func (testLogger) Panic(...any)                         {}
func (testLogger) TraceContext(context.Context, ...any) {}
func (testLogger) DebugContext(context.Context, ...any) {}
func (testLogger) InfoContext(context.Context, ...any)  {}
func (testLogger) WarnContext(context.Context, ...any)  {}
func (testLogger) ErrorContext(context.Context, ...any) {}
func (testLogger) FatalContext(context.Context, ...any) {}
func (testLogger) PanicContext(context.Context, ...any) {}

type tubeItem struct {
	data []byte
	err  error
}

type fakeTube struct {
	sendErr error
	recvCh  chan tubeItem
	maxMsg  int

	peer chan<- tubeItem

	dropSet bool
	dropSeq uint32
	dropped bool

	nackGate chan struct{}

	mu   sync.Mutex
	sent [][]byte
}

func newFakeTube() *fakeTube {
	return &fakeTube{recvCh: make(chan tubeItem, 256), maxMsg: 65536}
}

func (f *fakeTube) Send(ctx context.Context, data []byte) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	if gate := f.nackGate; gate != nil && isNackFrame(data) {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	cp := append([]byte(nil), data...)
	f.mu.Lock()
	f.sent = append(f.sent, cp)
	peer := f.peer
	if peer != nil && f.dropSet && !f.dropped {
		if frame, err := decodeFrame(cp); err == nil && !frame.isControl() && frame.seq == f.dropSeq {
			f.dropped = true
			f.mu.Unlock()
			return nil
		}
	}
	f.mu.Unlock()
	if peer == nil {
		return nil
	}
	select {
	case peer <- tubeItem{data: cp}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeTube) sentFrames() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.sent))
	for i, frame := range f.sent {
		out[i] = append([]byte(nil), frame...)
	}
	return out
}

func (f *fakeTube) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case item := <-f.recvCh:
		if item.err != nil {
			return nil, item.err
		}
		return item.data, nil
	}
}

func (f *fakeTube) MaxMessageSize() int { return f.maxMsg }

const testSessionID = 0x5EED

func testBondFrame(seq uint32, payload string) []byte {
	return bondFrameFrom(testSessionID, 0, seq, payload)
}

func bondFrameFrom(session, ack, seq uint32, payload string) []byte {
	buf, err := encodeFrame(seq, ack, 0, session, []byte(payload))
	if err != nil {
		panic(err)
	}
	return buf
}

type readResult struct {
	data []byte
	err  error
}

func readWithTimeout(t *testing.T, bc *BondConn, timeout time.Duration) ([]byte, error) {
	t.Helper()
	done := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 128)
		n, err := bc.Read(buf)
		out := make([]byte, n)
		copy(out, buf[:n])
		done <- readResult{data: out, err: err}
	}()
	select {
	case res := <-done:
		return res.data, res.err
	case <-time.After(timeout):
		t.Fatalf("Read did not return within %s: stream is wedged", timeout)
		return nil, nil
	}
}

func TestReorderOverflowFailsRead(t *testing.T) {
	tube := newFakeTube()
	bc := newBondConn(
		context.Background(),
		[]string{"a"},
		[]tubeLike{tube},
		testLogger{},
		connOptions{maxReorder: 4, gapTimeout: 5 * time.Second},
	)
	defer func() { _ = bc.Close() }()

	for seq := uint32(1); seq <= 8; seq++ {
		tube.recvCh <- tubeItem{data: testBondFrame(seq, "x")}
	}

	_, err := readWithTimeout(t, bc, 2*time.Second)
	if !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("expected ErrTransportReset on reorder overflow, got %v", err)
	}
	if !strings.Contains(err.Error(), "reorder buffer overflow") {
		t.Fatalf("expected a reorder overflow error, got %v", err)
	}
}

func TestGapTimeoutUnblocksRead(t *testing.T) {
	tube := newFakeTube()
	bc := newBondConn(
		context.Background(),
		[]string{"a"},
		[]tubeLike{tube},
		testLogger{},
		connOptions{maxReorder: 64, gapTimeout: 50 * time.Millisecond},
	)
	defer func() { _ = bc.Close() }()

	tube.recvCh <- tubeItem{data: testBondFrame(1, "y")}

	_, err := readWithTimeout(t, bc, 2*time.Second)
	if !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("expected ErrTransportReset on a lost chunk, got %v", err)
	}
	if !strings.Contains(err.Error(), "lost after") {
		t.Fatalf("expected a gap timeout error, got %v", err)
	}
}

func TestSendChunkFailureResetsSession(t *testing.T) {
	errBoom := errors.New("boom")
	pathA := newFakeTube()
	pathA.sendErr = errBoom
	pathB := newFakeTube()
	pathB.sendErr = errBoom

	bc := newBondConn(
		context.Background(),
		[]string{"a", "b"},
		[]tubeLike{pathA, pathB},
		testLogger{},
		connOptions{maxReorder: 64, gapTimeout: 5 * time.Second},
	)
	defer func() { _ = bc.Close() }()

	if _, err := bc.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := readWithTimeout(t, bc, 2*time.Second)
	if !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("expected ErrTransportReset when no subflow can deliver, got %v", err)
	}
	if !strings.Contains(err.Error(), "lost") {
		t.Fatalf("expected a lost-chunk error, got %v", err)
	}
}

func TestEmptyMessageIsIgnored(t *testing.T) {
	tube := newFakeTube()
	bc := newBondConn(
		context.Background(),
		[]string{"a"},
		[]tubeLike{tube},
		testLogger{},
		connOptions{maxReorder: 64, gapTimeout: 5 * time.Second},
	)
	defer func() { _ = bc.Close() }()

	tube.recvCh <- tubeItem{data: []byte{}}
	tube.recvCh <- tubeItem{data: testBondFrame(0, "ok")}

	data, err := readWithTimeout(t, bc, 2*time.Second)
	if err != nil {
		t.Fatalf("read after an empty message: %v", err)
	}
	if string(data) != "ok" {
		t.Fatalf("unexpected payload %q", data)
	}
}

func TestTransportResetKeepsSession(t *testing.T) {
	tube := newFakeTube()
	bc := newBondConn(
		context.Background(),
		[]string{"a"},
		[]tubeLike{tube},
		testLogger{},
		connOptions{maxReorder: 64, gapTimeout: 5 * time.Second},
	)
	defer func() { _ = bc.Close() }()

	tube.recvCh <- tubeItem{err: fmt.Errorf("%w: reconnected", transport.ErrTransportReset)}
	tube.recvCh <- tubeItem{data: testBondFrame(0, "ok")}

	data, err := readWithTimeout(t, bc, 2*time.Second)
	if err != nil {
		t.Fatalf("read after transport reset: %v", err)
	}
	if string(data) != "ok" {
		t.Fatalf("unexpected payload %q", data)
	}
}

func TestReadContextReturnsTerminalErrorAfterFail(t *testing.T) {
	tube := newFakeTube()
	bc := newBondConn(
		context.Background(),
		[]string{"a"},
		[]tubeLike{tube},
		testLogger{},
		connOptions{maxReorder: 64, gapTimeout: 50 * time.Millisecond},
	)

	tube.recvCh <- tubeItem{data: testBondFrame(1, "y")}

	if _, err := readWithTimeout(t, bc, 2*time.Second); !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("expected ErrTransportReset on a lost chunk, got %v", err)
	}

	for i := 0; i < 3; i++ {
		start := time.Now()
		if _, err := bc.Read(make([]byte, 8)); !errors.Is(err, transport.ErrTransportReset) {
			t.Fatalf("read %d error = %v, want ErrTransportReset", i, err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("read %d took %s: dead bond conn still blocks", i, elapsed)
		}
	}

	closed := make(chan struct{})
	go func() {
		_ = bc.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return: worker goroutines are wedged")
	}
}

func TestAllSubflowsStoppedReceivingFailsRead(t *testing.T) {
	tube := newFakeTube()
	bc := newBondConn(
		context.Background(),
		[]string{"a"},
		[]tubeLike{tube},
		testLogger{},
		connOptions{maxReorder: 64, gapTimeout: 5 * time.Second},
	)
	defer func() { _ = bc.Close() }()

	tube.recvCh <- tubeItem{err: errors.New("transport gone")}

	_, err := readWithTimeout(t, bc, 2*time.Second)
	if !errors.Is(err, transport.ErrTransportReset) {
		t.Fatalf("expected ErrTransportReset when all subflows stopped, got %v", err)
	}
}

func chunkPayload(seq uint32) string {
	return fmt.Sprintf("chunk-%02d", seq)
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("not observed within %s: %s", timeout, what)
}

func countSeq(frames [][]byte, want uint32) int {
	n := 0
	for _, frame := range frames {
		decoded, err := decodeFrame(frame)
		if err != nil || decoded.isControl() {
			continue
		}
		if decoded.seq == want {
			n++
		}
	}
	return n
}

func isNackFrame(data []byte) bool {
	frame, err := decodeFrame(data)
	if err != nil || !frame.isControl() {
		return false
	}
	kind, _, ok := decodeControl(frame.payload)
	return ok && kind == controlNack
}

func dataFrameCount(frames [][]byte) int {
	n := 0
	for _, frame := range frames {
		decoded, err := decodeFrame(frame)
		if err != nil || decoded.isControl() {
			continue
		}
		n++
	}
	return n
}

func countControl(frames [][]byte, want byte) int {
	n := 0
	for _, frame := range frames {
		decoded, err := decodeFrame(frame)
		if err != nil || !decoded.isControl() {
			continue
		}
		if kind, _, ok := decodeControl(decoded.payload); ok && kind == want {
			n++
		}
	}
	return n
}

func TestSlowFirstChunkKeepsSessionAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fast := newFakeTube()
	slow := newFakeTube()
	logger := &captureLogger{}
	bc := newBondConn(
		ctx,
		[]string{"2-raw_ws", "1-silo"},
		[]tubeLike{fast, slow},
		logger,
		defaultConnOptions(),
	)
	defer func() {
		cancel()
		_ = bc.Close()
	}()

	const chunks = 21
	for seq := uint32(1); seq < chunks; seq++ {
		fast.recvCh <- tubeItem{data: testBondFrame(seq, chunkPayload(seq))}
	}
	go func() {
		select {
		case <-time.After(6 * time.Second):
			slow.recvCh <- tubeItem{data: testBondFrame(0, chunkPayload(0))}
		case <-ctx.Done():
		}
	}()

	for seq := uint32(0); seq < chunks; seq++ {
		data, err := readWithTimeout(t, bc, 12*time.Second)
		if err != nil {
			if errors.Is(err, transport.ErrTransportReset) {
				t.Fatalf("session reset while seq=%d was still in flight: %v (logs: %s)",
					seq, err, logger.dump())
			}
			t.Fatalf("read chunk %d: %v", seq, err)
		}
		if want := chunkPayload(seq); string(data) != want {
			t.Fatalf("chunk %d = %q, want %q", seq, data, want)
		}
	}
	if err := bc.terminalErr(); err != nil {
		t.Fatalf("session was reset: %v", err)
	}
	if msg, found := logger.contains("session reset"); found {
		t.Fatalf("bond reset the session: %s\n%s", msg, logger.dump())
	}
}

func TestFirstChunkFansOutAcrossAllSubflows(t *testing.T) {
	cold := newFakeTube()
	warm := newFakeTube()
	bc := newBondConn(
		context.Background(),
		[]string{"1-silo", "2-raw_ws"},
		[]tubeLike{cold, warm},
		testLogger{},
		defaultConnOptions(),
	)
	defer func() { _ = bc.Close() }()

	if _, err := bc.Write([]byte("first")); err != nil {
		t.Fatalf("write first chunk: %v", err)
	}
	waitFor(t, 2*time.Second, "seq=0 on both tubes", func() bool {
		return countSeq(cold.sentFrames(), 0) == 1 && countSeq(warm.sentFrames(), 0) == 1
	})
	for name, path := range map[string]*fakeTube{"1-silo": cold, "2-raw_ws": warm} {
		for _, frame := range path.sentFrames() {
			decoded, err := decodeFrame(frame)
			if err != nil {
				t.Fatalf("%s sent a malformed frame: %v", name, err)
			}
			if decoded.isControl() || decoded.seq != 0 {
				continue
			}
			if string(decoded.payload) != "first" {
				t.Fatalf("%s carried seq=0 payload %q, want %q", name, decoded.payload, "first")
			}
		}
	}

	for _, payload := range []string{"second", "third"} {
		if _, err := bc.Write([]byte(payload)); err != nil {
			t.Fatalf("write %q: %v", payload, err)
		}
	}
	waitFor(t, 2*time.Second, "seq=1 and seq=2 delivered exactly once", func() bool {
		seqOne := countSeq(cold.sentFrames(), 1) + countSeq(warm.sentFrames(), 1)
		seqTwo := countSeq(cold.sentFrames(), 2) + countSeq(warm.sentFrames(), 2)
		return seqOne == 1 && seqTwo == 1
	})
}

func TestNackRetransmitsLostChunk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	senderA := newFakeTube()
	senderB := newFakeTube()
	receiverA := newFakeTube()
	receiverB := newFakeTube()

	senderA.peer = receiverA.recvCh
	receiverA.peer = senderA.recvCh
	senderB.peer = receiverB.recvCh
	receiverB.peer = senderB.recvCh

	senderA.dropSet = true
	senderA.dropSeq = 2

	logger := &captureLogger{}
	sender := newBondConn(ctx, []string{"0-silo", "1-raw_ws"},
		[]tubeLike{senderA, senderB}, logger, defaultConnOptions())
	defer func() { _ = sender.Close() }()

	sender.sched.MarkAlive(sender.sched.subflows[0])

	receiver := newBondConn(ctx, []string{"0-silo", "1-raw_ws"},
		[]tubeLike{receiverA, receiverB}, logger,
		connOptions{maxReorder: 256, gapTimeout: 2 * time.Second})
	defer func() { _ = receiver.Close() }()

	const chunks = 51
	start := time.Now()
	for seq := uint32(0); seq < chunks; seq++ {
		if _, err := sender.Write([]byte{byte(seq)}); err != nil {
			t.Fatalf("write seq=%d: %v", seq, err)
		}
	}

	type readOutcome struct {
		data []byte
		err  error
	}
	done := make(chan readOutcome, 1)
	go func() {
		got := make([]byte, chunks)
		_, err := io.ReadFull(receiver, got)
		done <- readOutcome{data: got, err: err}
	}()

	var outcome readOutcome
	select {
	case outcome = <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("receiver stalled on the lost chunk; logs:\n%s", logger.dump())
	}
	if outcome.err != nil {
		t.Fatalf("receiver failed: %v; logs:\n%s", outcome.err, logger.dump())
	}
	for seq := uint32(0); seq < chunks; seq++ {
		if outcome.data[seq] != byte(seq) {
			t.Fatalf("chunk %d = %d, want %d", seq, outcome.data[seq], byte(seq))
		}
	}
	elapsed := time.Since(start)
	if elapsed >= 500*time.Millisecond {
		t.Fatalf("gap recovery took %s, want < 500ms", elapsed)
	}
	if err := receiver.terminalErr(); err != nil {
		t.Fatalf("receiver reset the session instead of nacking: %v", err)
	}
	if err := sender.terminalErr(); err != nil {
		t.Fatalf("sender reset the session instead of retransmitting: %v", err)
	}
	if msg, found := logger.contains("lost after"); found {
		t.Fatalf("gap timeout fired despite the nack recovery: %s", msg)
	}
	nacks := countControl(receiverA.sentFrames(), controlNack) + countControl(receiverB.sentFrames(), controlNack)
	if nacks == 0 {
		t.Fatal("receiver never asked for the missing chunk")
	}
	if retransmits := countSeq(senderA.sentFrames(), 2) + countSeq(senderB.sentFrames(), 2); retransmits < 2 {
		t.Fatalf("seq=2 was sent %d time(s), want a retransmission", retransmits)
	}
}

func readDataAfterReset(t *testing.T, bc *BondConn, timeout time.Duration) ([]byte, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		data, err := readWithTimeout(t, bc, time.Until(deadline))
		if errors.Is(err, transport.ErrTransportReset) {
			continue
		}
		return data, err
	}
}

func TestPeerSessionChangeAcceptsFreshStream(t *testing.T) {
	tube := newFakeTube()
	bc := newBondConn(
		context.Background(),
		[]string{"a"},
		[]tubeLike{tube},
		testLogger{},
		connOptions{maxReorder: 64, gapTimeout: 5 * time.Second},
	)
	defer func() { _ = bc.Close() }()

	tube.recvCh <- tubeItem{data: bondFrameFrom(1, 0, 0, "old-0")}
	tube.recvCh <- tubeItem{data: bondFrameFrom(1, 0, 1, "old-1")}
	if data, err := readWithTimeout(t, bc, 2*time.Second); err != nil || string(data) != "old-0" {
		t.Fatalf("first chunk = %q, %v", data, err)
	}
	if data, err := readWithTimeout(t, bc, 2*time.Second); err != nil || string(data) != "old-1" {
		t.Fatalf("second chunk = %q, %v", data, err)
	}

	tube.recvCh <- tubeItem{data: bondFrameFrom(2, 0, 0, "new-0")}
	data, err := readDataAfterReset(t, bc, 2*time.Second)
	if err != nil {
		t.Fatalf("read after the peer restart: %v", err)
	}
	if string(data) != "new-0" {
		t.Fatalf("chunk after restart = %q, want new-0", data)
	}

	tube.recvCh <- tubeItem{data: bondFrameFrom(1, 0, 2, "stale")}
	tube.recvCh <- tubeItem{data: bondFrameFrom(2, 0, 1, "new-1")}
	data, err = readDataAfterReset(t, bc, 2*time.Second)
	if err != nil {
		t.Fatalf("read after a stale frame: %v", err)
	}
	if string(data) != "new-1" {
		t.Fatalf("chunk = %q, want new-1", data)
	}
	if err := bc.terminalErr(); err != nil {
		t.Fatalf("a peer restart must be a soft reset, got terminal error: %v", err)
	}
}

func TestSchedulerPrefersConfirmedSubflow(t *testing.T) {
	s := newScheduler([]string{"1-silo", "2-raw_ws"}, []tubeLike{newFakeTube(), newFakeTube()})
	cold, warm := s.subflows[0], s.subflows[1]

	used := map[string]bool{}
	for i := 0; i < 4; i++ {
		sf := s.pickNonFull(nil, true)
		if sf == nil {
			t.Fatal("pick returned nil while both tubes are usable")
		}
		used[sf.name] = true
	}
	if len(used) != 2 {
		t.Fatalf("cold start never reached both tubes: %v", used)
	}

	s.MarkAlive(warm)
	for i := 0; i < 8; i++ {
		if got := s.pickNonFull(nil, true); got != warm {
			t.Fatalf("start-up pick %d went to %s, want the confirmed tube", i, got.name)
		}
	}

	used = map[string]bool{}
	for i := 0; i < 8; i++ {
		sf := s.pickNonFull(nil, false)
		if sf == nil {
			t.Fatal("pick returned nil after the start-up window")
		}
		used[sf.name] = true
	}
	if !used[cold.name] || !used[warm.name] {
		t.Fatalf("round-robin did not use both tubes: %v", used)
	}

	for i := 0; i < cap(warm.queue); i++ {
		warm.queue <- queuedChunk{seq: uint32(i)}
	}
	if got := s.pickNonFull(nil, true); got != cold {
		t.Fatalf("pick with a full confirmed tube went to %v, want the cold tube", got)
	}
	for len(warm.queue) > 0 {
		<-warm.queue
	}

	if got := s.pickAlive(map[*subflow]struct{}{cold: {}}); got != warm {
		t.Fatalf("failover picked %v, want the confirmed tube", got)
	}
}

func TestFullSendWindowBlocksWriteWithoutEvicting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tube := newFakeTube()
	bc := newBondConn(
		ctx,
		[]string{"a"},
		[]tubeLike{tube},
		testLogger{},
		connOptions{maxReorder: 64, gapTimeout: 30 * time.Second, maxSentChunks: 4},
	)
	defer func() { _ = bc.Close() }()

	for seq := 0; seq < 4; seq++ {
		if _, err := bc.Write([]byte{byte(seq)}); err != nil {
			t.Fatalf("write seq=%d: %v", seq, err)
		}
	}
	if got := bc.unackedCount(); got != 4 {
		t.Fatalf("unacked = %d, want the full window of 4", got)
	}

	written := make(chan error, 1)
	go func() {
		_, err := bc.Write([]byte{4})
		written <- err
	}()
	select {
	case err := <-written:
		t.Fatalf("Write returned %v with a full window: an unacked chunk was evicted", err)
	case <-time.After(150 * time.Millisecond):
	}

	tube.recvCh <- tubeItem{data: encodeControlFrame(controlPing, testSessionID, 2, bc.session, nil)}
	select {
	case err := <-written:
		if err != nil {
			t.Fatalf("write after the ack: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Write stayed blocked after the peer acknowledged two chunks")
	}

	if got := bc.unackedCount(); got != 3 {
		t.Fatalf("unacked = %d after the ack, want 3 (seq 2, 3 and the new one)", got)
	}
	waitFor(t, 2*time.Second, "every sequence reached the wire exactly once", func() bool {
		frames := tube.sentFrames()
		for seq := uint32(0); seq <= 4; seq++ {
			if countSeq(frames, seq) != 1 {
				return false
			}
		}
		return true
	})
}

func TestLargeDownloadSurvivesDelayedNack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	senderA := newFakeTube()
	senderB := newFakeTube()
	receiverA := newFakeTube()
	receiverB := newFakeTube()

	senderA.peer = receiverA.recvCh
	receiverA.peer = senderA.recvCh
	senderB.peer = receiverB.recvCh
	receiverB.peer = senderB.recvCh

	senderA.dropSet, senderA.dropSeq = true, 14
	senderB.dropSet, senderB.dropSeq = true, 14

	nackGate := make(chan struct{})
	receiverA.nackGate = nackGate
	receiverB.nackGate = nackGate

	logger := &captureLogger{}
	sender := newBondConn(ctx, []string{"0-silo", "1-raw_ws"},
		[]tubeLike{senderA, senderB}, logger, defaultConnOptions())
	defer func() { _ = sender.Close() }()

	receiver := newBondConn(ctx, []string{"0-silo", "1-raw_ws"},
		[]tubeLike{receiverA, receiverB}, logger,
		connOptions{maxReorder: 4096, gapTimeout: 5 * time.Second})
	defer func() { _ = receiver.Close() }()

	const chunks = 1200
	payload := func(seq int) []byte { return []byte{byte(seq >> 8), byte(seq)} }

	type outcome struct {
		data []byte
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		got := make([]byte, 2*chunks)
		_, err := io.ReadFull(receiver, got)
		done <- outcome{data: got, err: err}
	}()

	for seq := 0; seq < chunks; seq++ {
		if _, err := sender.Write(payload(seq)); err != nil {
			t.Fatalf("write seq=%d: %v", seq, err)
		}
	}

	if _, _, ok := sender.sentChunk(14); !ok {
		t.Fatalf("seq=14 was evicted from the sent window after %d chunks", chunks)
	}
	if got := sender.unackedCount(); got <= 512 {
		t.Fatalf("unacked = %d, want more than the old 512-chunk limit", got)
	}

	waitFor(t, 10*time.Second, "1000 chunks reached the wire", func() bool {
		return dataFrameCount(senderA.sentFrames())+dataFrameCount(senderB.sentFrames()) >= 1000
	})

	close(nackGate)

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("receiver failed after %d chunks: %v; logs:\n%s",
				chunks, got.err, logger.dump())
		}
		for seq := 0; seq < chunks; seq++ {
			if got.data[2*seq] != payload(seq)[0] || got.data[2*seq+1] != payload(seq)[1] {
				t.Fatalf("chunk seq=%d was corrupted in the assembled stream", seq)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("receiver stalled on the lost chunk; logs:\n%s", logger.dump())
	}

	if err := sender.terminalErr(); err != nil {
		t.Fatalf("sender reset the session instead of retransmitting: %v", err)
	}
	if err := receiver.terminalErr(); err != nil {
		t.Fatalf("receiver reset the session instead of retransmitting: %v", err)
	}
	if msg, found := logger.contains("lost after"); found {
		t.Fatalf("gap timeout fired despite the recovered chunk: %s", msg)
	}
	if retransmits := countSeq(senderA.sentFrames(), 14) + countSeq(senderB.sentFrames(), 14); retransmits < 2 {
		t.Fatalf("seq=14 was sent %d time(s), want a retransmission from the sent window", retransmits)
	}
}

func TestPeerSessionResetRestartsBothStreams(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tubeA := newFakeTube()
	tubeB := newFakeTube()
	receiverA := newFakeTube()
	receiverB := newFakeTube()

	tubeA.peer = receiverA.recvCh
	receiverA.peer = tubeA.recvCh
	tubeB.peer = receiverB.recvCh
	receiverB.peer = tubeB.recvCh

	names := []string{"0-silo", "1-raw_ws"}
	sender := newBondConn(ctx, names, []tubeLike{tubeA, tubeB}, testLogger{}, defaultConnOptions())

	logger := &captureLogger{}
	receiver := newBondConn(ctx, names, []tubeLike{receiverA, receiverB}, logger,
		connOptions{maxReorder: 256, gapTimeout: 5 * time.Second})
	defer func() { _ = receiver.Close() }()

	if _, err := sender.Write([]byte("hello")); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if data, err := readWithTimeout(t, receiver, 2*time.Second); err != nil || string(data) != "hello" {
		t.Fatalf("receiver got %q, %v", data, err)
	}

	if _, err := receiver.Write([]byte("before")); err != nil {
		t.Fatalf("write before: %v", err)
	}
	if data, err := readWithTimeout(t, sender, 2*time.Second); err != nil || string(data) != "before" {
		t.Fatalf("sender got %q, %v", data, err)
	}

	oldSession := sender.session
	if err := sender.Close(); err != nil {
		t.Fatalf("close sender: %v", err)
	}
	sender = newBondConnWithRetired(ctx, names, []tubeLike{tubeA, tubeB}, testLogger{}, []uint32{oldSession})
	defer func() { _ = sender.Close() }()

	if _, err := sender.Write([]byte("restarted")); err != nil {
		t.Fatalf("write restarted: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	sawReset, gotRestarted := false, false
	for !(sawReset && gotRestarted) {
		data, err := readWithTimeout(t, receiver, time.Until(deadline))
		switch {
		case errors.Is(err, transport.ErrTransportReset):
			sawReset = true
		case err != nil:
			t.Fatalf("read after the peer restart: %v; logs:\n%s", err, logger.dump())
		case string(data) == "restarted":
			gotRestarted = true
		default:
			t.Fatalf("unexpected chunk %q after the peer restart", data)
		}
	}
	if err := receiver.terminalErr(); err != nil {
		t.Fatalf("a peer restart must be a soft reset, got terminal error: %v", err)
	}

	if _, err := receiver.Write([]byte("reply")); err != nil {
		t.Fatalf("write reply: %v", err)
	}
	if data, err := readWithTimeout(t, sender, 5*time.Second); err != nil || string(data) != "reply" {
		t.Fatalf("fresh sender got %q, %v", data, err)
	}

	replied := false
	frames := append(receiverA.sentFrames(), receiverB.sentFrames()...)
	for _, frame := range frames {
		decoded, err := decodeFrame(frame)
		if err != nil || decoded.isControl() {
			continue
		}
		if decoded.seq == 0 && string(decoded.payload) == "reply" {
			replied = true
		}
	}
	if !replied {
		t.Fatal("outgoing stream did not restart at seq 0 after the peer reset")
	}
}

func TestSchedulerMarkDeadSurvivesConcurrentMarkAlive(t *testing.T) {
	s := newScheduler([]string{"a"}, []tubeLike{newFakeTube()})
	sf := s.subflows[0]

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.MarkDead(sf, errors.New("boom"))
		}()
		go func() {
			defer wg.Done()
			s.MarkAlive(sf)
		}()
	}
	wg.Wait()
}
