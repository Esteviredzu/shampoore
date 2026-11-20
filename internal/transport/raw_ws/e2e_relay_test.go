package raw_ws

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/relay"
)

func TestTransportStreamsThroughRelayStream(t *testing.T) {
	rs := relay.New("secret")
	srv := httptest.NewServer(rs)
	defer srv.Close()

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	a := newTestTransport(srv.URL, false)
	b := newTestTransport(srv.URL, false)
	defer func() { _ = a.Stop(context.Background()) }()
	defer func() { _ = b.Stop(context.Background()) }()
	if err := a.Start(ctxA); err != nil {
		t.Fatalf("start a: %v", err)
	}
	if err := b.Start(ctxB); err != nil {
		t.Fatalf("start b: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if rs.RoomSize("room-1") == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("clients did not join the tunnel room (size=%d)", rs.RoomSize("room-1"))
		}
		time.Sleep(20 * time.Millisecond)
	}

	deadline = time.Now().Add(10 * time.Second)
	for {
		err := a.Send(ctxA, []byte("hello-through-relay"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("send never succeeded: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	recvCtx, recvCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer recvCancel()
	data, err := b.Receive(recvCtx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if string(data) != "hello-through-relay" {
		t.Fatalf("payload = %q", data)
	}
}
