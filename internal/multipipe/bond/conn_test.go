package bond

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
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
}

func newFakeTube() *fakeTube {
	return &fakeTube{recvCh: make(chan tubeItem, 64), maxMsg: 65536}
}

func (f *fakeTube) Send(ctx context.Context, data []byte) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	return nil
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

func bondFrame(seq uint32, payload string) []byte {
	data := []byte(payload)
	buf := make([]byte, frameHeaderSize+len(data))
	binary.BigEndian.PutUint32(buf[0:4], seq)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(data)))
	copy(buf[frameHeaderSize:], data)
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
		tube.recvCh <- tubeItem{data: bondFrame(seq, "x")}
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

	tube.recvCh <- tubeItem{data: bondFrame(1, "y")}

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
	tube.recvCh <- tubeItem{data: bondFrame(0, "ok")}

	data, err := readWithTimeout(t, bc, 2*time.Second)
	if err != nil {
		t.Fatalf("read after transport reset: %v", err)
	}
	if string(data) != "ok" {
		t.Fatalf("unexpected payload %q", data)
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
