package relay

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func dialRelay(t *testing.T, srv *httptest.Server, room, password string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/" + room
	header := http.Header{}
	if password != "" {
		header.Set("X-Room-Password", password)
	}
	conn, resp, err := websocket.DefaultDialer.Dial(url, header)
	if err != nil {
		return nil, resp, err
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, resp, nil
}

func TestRelayPasswordAndBroadcast(t *testing.T) {
	srv := httptest.NewServer(New("secret"))
	defer srv.Close()

	if _, resp, err := dialRelay(t, srv, "room", "wrong"); err == nil {
		t.Fatal("wrong password was accepted")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong password status = %v", resp)
	}

	a, _, err := dialRelay(t, srv, "room", "secret")
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := dialRelay(t, srv, "room", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	_ = b.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := b.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("message = %q", data)
	}
}

func TestRelayRoomsAreIsolated(t *testing.T) {
	srv := httptest.NewServer(New(""))
	defer srv.Close()
	a1, _, err := dialRelay(t, srv, "room-a", "")
	if err != nil {
		t.Fatal(err)
	}
	a2, _, err := dialRelay(t, srv, "room-a", "")
	if err != nil {
		t.Fatal(err)
	}
	b1, _, err := dialRelay(t, srv, "room-b", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := a1.WriteMessage(websocket.BinaryMessage, []byte("only-a")); err != nil {
		t.Fatal(err)
	}
	_ = a2.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, data, err := a2.ReadMessage(); err != nil || string(data) != "only-a" {
		t.Fatalf("room-a message = %q, %v", data, err)
	}
	_ = b1.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, _, err := b1.ReadMessage(); err == nil {
		t.Fatal("message leaked into another room")
	}
}
