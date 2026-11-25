package bond

import (
	"context"

	"io"
	"testing"
	"time"
)

type eofTube struct {
	delay time.Duration
}

func (t *eofTube) Send(context.Context, []byte) error {
	return io.EOF
}

func (t *eofTube) Receive(ctx context.Context) ([]byte, error) {
	if t.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(t.delay):
		}
	}
	return nil, io.EOF
}

func (t *eofTube) MaxMessageSize() int { return 12000 }

func TestBondRunsOnLiveTubesWhileOthersFailWithEOF(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fastA1, fastB1 := newMemPair()
	fastA2, fastB2 := newMemPair()
	names := []string{"0-livekit_datachannel", "1-silo", "2-jellyfin", "3-raw_ws"}

	sender := NewBondConn(ctx, names, []tubeLike{
		&eofTube{delay: 100 * time.Millisecond},
		&eofTube{delay: 100 * time.Millisecond},
		fastA1, fastA2,
	}, testLogger{})
	defer func() { _ = sender.Close() }()

	receiver := NewBondConn(ctx, names, []tubeLike{
		&eofTube{delay: 100 * time.Millisecond},
		&eofTube{delay: 100 * time.Millisecond},
		fastB1, fastB2,
	}, testLogger{})
	defer func() { _ = receiver.Close() }()

	read := func(deadline time.Duration, want []byte) {
		t.Helper()
		readCtx, readCancel := context.WithTimeout(ctx, deadline)
		defer readCancel()
		got := make([]byte, len(want))
		if _, err := io.ReadFull(&ctxReader{ctx: readCtx, bc: receiver}, got); err != nil {
			t.Fatalf("read %d bytes within %s: %v", len(want), deadline, err)
		}
		if string(got) != string(want) {
			t.Fatalf("payload = %q, want %q", got, want)
		}
	}

	start := time.Now()
	if _, err := sender.Write([]byte("handshake")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	read(time.Second, []byte("handshake"))
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("first round trip took %s: traffic waited for the broken tubes", elapsed)
	}

	const rounds = 200
	payload := []byte("dns-query-payload")
	for i := 0; i < rounds; i++ {
		if _, err := sender.Write(payload); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		read(2*time.Second, payload)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Fatalf("%d round trips took %s", rounds, elapsed)
	}

	for name, conn := range map[string]*BondConn{"sender": sender, "receiver": receiver} {
		if err := conn.terminalErr(); err != nil {
			t.Fatalf("%s session was reset by the EOF tubes: %v", name, err)
		}
		select {
		case <-conn.failed:
			t.Fatalf("%s session is in the failed state", name)
		default:
		}
	}

	for _, snap := range sender.sched.Snapshot() {
		if snap.Name == "2-jellyfin" || snap.Name == "3-raw_ws" {
			if snap.SentBytes == 0 {
				t.Fatalf("healthy tube %s carried no traffic", snap.Name)
			}
		}
	}
}

type ctxReader struct {
	ctx context.Context
	bc  *BondConn
}

func (r *ctxReader) Read(p []byte) (int, error) {
	return r.bc.ReadContext(r.ctx, p)
}

type slowTube struct {
	inner tubeLike
	delay time.Duration
}

func (t *slowTube) Send(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(t.delay):
	}
	return t.inner.Send(ctx, data)
}

func (t *slowTube) Receive(ctx context.Context) ([]byte, error) {
	return t.inner.Receive(ctx)
}

func (t *slowTube) MaxMessageSize() int { return t.inner.MaxMessageSize() }

func TestBondStartupDoesNotWaitForSlowTubes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fastA1, fastB1 := newMemPair()
	fastA2, fastB2 := newMemPair()
	slowA1, slowB1 := newMemPair()
	slowA2, slowB2 := newMemPair()

	const slowDelay = 500 * time.Millisecond
	names := []string{"0-slow-a", "1-slow-b", "2-jellyfin", "3-raw_ws"}

	sender := NewBondConn(ctx, names, []tubeLike{
		&slowTube{inner: slowA1, delay: slowDelay},
		&slowTube{inner: slowA2, delay: slowDelay},
		fastA1, fastA2,
	}, testLogger{})
	defer func() { _ = sender.Close() }()

	receiver := NewBondConn(ctx, names, []tubeLike{
		&slowTube{inner: slowB1, delay: slowDelay},
		&slowTube{inner: slowB2, delay: slowDelay},
		fastB1, fastB2,
	}, testLogger{})
	defer func() { _ = receiver.Close() }()

	start := time.Now()
	if _, err := sender.Write([]byte("hello")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	got := make([]byte, len("hello"))
	_, err := io.ReadFull(&ctxReader{ctx: readCtx, bc: receiver}, got)
	readCancel()
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("payload = %q, want hello", got)
	}
	if elapsed := time.Since(start); elapsed >= slowDelay {
		t.Fatalf("first round trip took %s: the start-up chunk waited for a slow tube", elapsed)
	}

	for i := 0; i < 16; i++ {
		payload := []byte("open-ack")
		if _, err := sender.Write(payload); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		roundCtx, roundCancel := context.WithTimeout(ctx, 2*time.Second)
		reply := make([]byte, len(payload))
		_, readErr := io.ReadFull(&ctxReader{ctx: roundCtx, bc: receiver}, reply)
		roundCancel()
		if readErr != nil {
			t.Fatalf("read %d: %v", i, readErr)
		}
		if string(reply) != string(payload) {
			t.Fatalf("round %d payload = %q", i, reply)
		}
	}
	if elapsed := time.Since(start); elapsed >= 2*slowDelay {
		t.Fatalf("the startup window took %s, want well under one slow round trip", elapsed)
	}
}
