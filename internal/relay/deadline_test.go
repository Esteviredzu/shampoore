package relay

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestTunnelReapsPeerWithoutPongs(t *testing.T) {
	s := New("")
	s.pongWait = 100 * time.Millisecond
	s.pingInterval = 20 * time.Millisecond
	srv := httptest.NewServer(s)
	defer srv.Close()

	if _, _, err := dialRelay(t, srv, "dead-room", ""); err != nil {
		t.Fatalf("dial: %v", err)
	}

	waitForRoomSize(t, s, "dead-room", 1, time.Second)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.RoomSize("dead-room") == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("half-open peer was not reaped from the room")
}

func waitForRoomSize(t *testing.T, s *Server, room string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.RoomSize(room) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("room %q size = %d, want %d", room, s.RoomSize(room), want)
}
