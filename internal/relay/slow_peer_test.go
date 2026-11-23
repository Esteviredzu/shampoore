package relay

import (
	"bytes"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSlowPeerDoesNotBlockRoom(t *testing.T) {
	s := New("")
	s.pingInterval = time.Hour
	srv := httptest.NewServer(s)
	defer srv.Close()

	slow, _, err := dialRelay(t, srv, "room", "")
	if err != nil {
		t.Fatalf("dial slow: %v", err)
	}
	_ = slow

	fast, _, err := dialRelay(t, srv, "room", "")
	if err != nil {
		t.Fatalf("dial fast: %v", err)
	}
	sender, _, err := dialRelay(t, srv, "room", "")
	if err != nil {
		t.Fatalf("dial sender: %v", err)
	}

	waitForRoomSize(t, s, "room", 3, 2*time.Second)

	var fastCount atomic.Int64
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		for {
			if _, _, err := fast.ReadMessage(); err != nil {
				return
			}
			fastCount.Add(1)
		}
	}()
	t.Cleanup(func() { _ = fast.Close() })

	payload := bytes.Repeat([]byte("x"), 32<<10)
	const count = 400

	start := time.Now()
	for i := 0; i < count; i++ {
		if err := sender.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("broadcast stalled the sender for %s", elapsed)
	}

	deadline := time.Now().Add(5 * time.Second)
	for fastCount.Load() < 50 {
		if time.Now().After(deadline) {
			t.Fatalf("fast peer received %d messages, want at least 50", fastCount.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	deadline = time.Now().Add(5 * time.Second)
	for s.RoomSize("room") > 2 {
		if time.Now().After(deadline) {
			t.Fatalf("slow peer was not dropped, room size=%d", s.RoomSize("room"))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSlowPeerQueueOverflowClosesClient(t *testing.T) {

	c := &client{
		send: make(chan clientMessage, 1),
		done: make(chan struct{}),
	}
	if !c.enqueue(clientMessage{typ: websocket.BinaryMessage, data: []byte("x")}) {
		t.Fatal("enqueue failed on an empty queue")
	}
	if c.enqueue(clientMessage{typ: websocket.BinaryMessage, data: []byte("y")}) {
		t.Fatal("enqueue accepted a message beyond the queue capacity")
	}

	c.close()
	if c.enqueue(clientMessage{typ: websocket.BinaryMessage}) {
		t.Fatal("enqueue accepted a message on a closed client")
	}
}
