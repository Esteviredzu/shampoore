package raw_ws

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestBatchedSendAcknowledgedByWriter(t *testing.T) {
	received := make(chan []byte, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			received <- data
		}
	}))
	defer srv.Close()

	tr := newTestTransport(srv.URL, true)
	cancel := startTransport(t, tr)
	defer cancel()
	defer stopWithTimeout(t, tr)

	deadline := time.Now().Add(2 * time.Second)
	for {
		tr.connMu.Lock()
		active := tr.active
		tr.connMu.Unlock()
		if active != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("transport did not connect")
		}
		time.Sleep(5 * time.Millisecond)
	}

	start := time.Now()
	if err := tr.Send(context.Background(), []byte("ping")); err != nil {
		t.Fatalf("send: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Send took %s: acknowledgement is waiting for the batch timer", elapsed)
	}

	select {
	case data := <-received:
		if len(data) == 0 {
			t.Fatal("server received an empty frame")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive the batched frame")
	}
}

func TestSessionTeardownResolvesPendingSends(t *testing.T) {
	tr := newTransport(testConfig("ws://127.0.0.1:1", true), testLogger{})

	sess := &wsSession{
		done:  make(chan struct{}),
		queue: make(chan outboundMsg, 4),
	}
	msg := outboundMsg{data: []byte("payload"), done: make(chan error, 1)}
	if err := sess.enqueue(msg, time.Second); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	sess.markDone()
	tr.failPending(sess, errSessionClosed)

	select {
	case err := <-msg.done:
		if !errors.Is(err, errSessionClosed) {
			t.Fatalf("pending send error = %v, want errSessionClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending send was never resolved: Send would block forever")
	}

	if err := sess.enqueue(outboundMsg{done: make(chan error, 1)}, time.Millisecond); !errors.Is(err, errSessionClosed) {
		t.Fatalf("enqueue after teardown = %v, want errSessionClosed", err)
	}
}

func TestSendLoopReportsWriteFailure(t *testing.T) {
	tr := newTransport(testConfig("ws://127.0.0.1:1", true), testLogger{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess := &wsSession{
		ctx:    ctx,
		done:   make(chan struct{}),
		queue:  make(chan outboundMsg, 4),
		errCh:  make(chan error, 4),
		cancel: cancel,
	}
	go tr.sendLoop(ctx, sess, sess.errCh)

	msg := outboundMsg{data: []byte("payload"), done: make(chan error, 1)}
	if err := sess.enqueue(msg, time.Second); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case err := <-msg.done:
		if err == nil {
			t.Fatal("batched send reported success although the write failed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sendLoop never reported the write result")
	}
}
