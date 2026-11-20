package utlsdial

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	utls "github.com/refraction-networking/utls"
)

func newTLSServer(t *testing.T, handler http.Handler, alpn ...string) *httptest.Server {
	t.Helper()
	insecureSkipVerify.Store(true)
	t.Cleanup(func() { insecureSkipVerify.Store(false) })

	srv := httptest.NewUnstartedServer(handler)
	if len(alpn) > 0 {
		srv.TLS = &tls.Config{NextProtos: alpn}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func newCaddyTLSServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	insecureSkipVerify.Store(true)
	t.Cleanup(func() { insecureSkipVerify.Store(false) })

	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{
		NextProtos: []string{"h2", "http/1.1"},
		MinVersion: tls.VersionTLS13,
	}
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func serverAddr(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	addr := srv.Listener.Addr().String()
	if _, _, err := net.SplitHostPort(addr); err != nil {
		t.Fatalf("unexpected listener addr %q: %v", addr, err)
	}
	return addr
}

type connectionStater interface {
	ConnectionState() tls.ConnectionState
}

func TestDialContextHandshakeAndALPN(t *testing.T) {
	srv := newTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}), "http/1.1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := DialContext(ctx, "tcp", serverAddr(t, srv), "", WebSocketALPN, ProfileChrome)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stater, ok := conn.(connectionStater)
	if !ok {
		t.Fatal("connection does not expose ConnectionState")
	}
	if got := stater.ConnectionState().NegotiatedProtocol; got != "http/1.1" {
		t.Fatalf("negotiated protocol = %q, want http/1.1", got)
	}

	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestNewHTTPTransportGET(t *testing.T) {
	srv := newTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		_, _ = io.WriteString(w, "hello")
	}), "h2", "http/1.1")

	tr := NewHTTPTransport(ProfileChrome)
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	resp, err := client.Get(srv.URL + "/api")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "hello" {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
	if resp.TLS == nil || resp.TLS.NegotiatedProtocol == "" {
		t.Fatalf("transport did not expose negotiated ALPN: %+v", resp.TLS)
	}
}

func TestNewWebSocketDialerUpgrade(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := newTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			mt, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(mt, data); err != nil {
				return
			}
		}
	}), "http/1.1")

	url := "wss" + strings.TrimPrefix(srv.URL, "https") + "/stream"
	dialer := NewWebSocketDialer(ProfileFirefox)
	dialer.HandshakeTimeout = 5 * time.Second
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "ping" {
		t.Fatalf("echo = %q", data)
	}
}

func TestNewWebSocketDialerChromeAgainstCaddyALPN(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := newCaddyTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			t.Error("websocket handler got no TLS state")
			http.Error(w, "tls required", http.StatusBadRequest)
			return
		}
		if got := r.TLS.NegotiatedProtocol; got != "http/1.1" {
			t.Errorf("negotiated ALPN = %q, want http/1.1", got)
		}
		if got := r.TLS.Version; got != tls.VersionTLS13 {
			t.Errorf("negotiated TLS version = %#x, want TLS 1.3", got)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			mt, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(mt, data); err != nil {
				return
			}
		}
	}))

	url := "wss" + strings.TrimPrefix(srv.URL, "https") + "/stream"
	dialer := NewWebSocketDialer(ProfileChrome)
	dialer.HandshakeTimeout = 5 * time.Second
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "unexpected EOF") {
			t.Fatalf("dial ws: unexpected EOF (ALPS/ALPN mismatch on TLS 1.3): %v", err)
		}
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "ping" {
		t.Fatalf("echo = %q", data)
	}
}

func TestProfileMapping(t *testing.T) {
	cases := map[Profile]utls.ClientHelloID{
		ProfileChrome:       utls.HelloChrome_Auto,
		"":                  utls.HelloChrome_Auto,
		"unknown":           utls.HelloChrome_Auto,
		ProfileFirefox:      utls.HelloFirefox_Auto,
		ProfileSafari:       utls.HelloSafari_Auto,
		Profile(" SAFARI "): utls.HelloSafari_Auto,
	}
	for profile, want := range cases {
		if got := clientHelloID(profile); got != want {
			t.Fatalf("clientHelloID(%q) = %v, want %v", profile, got, want)
		}
	}
	if ua := UserAgent(ProfileChrome); !strings.Contains(ua, "Chrome/") {
		t.Fatalf("chrome UA = %q", ua)
	}
	if ua := UserAgent(ProfileFirefox); !strings.Contains(ua, "Firefox/") {
		t.Fatalf("firefox UA = %q", ua)
	}
	if ua := UserAgent("bogus"); !strings.Contains(ua, "Chrome/") {
		t.Fatalf("fallback UA = %q", ua)
	}
}

func newPipeUConn(t *testing.T, profile Profile, alpn []string) *utls.UConn {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	cfg := &utls.Config{
		ServerName: "example.test",
		NextProtos: append([]string(nil), alpn...),
		MinVersion: utls.VersionTLS12,
	}
	uconn, err := newUConn(client, cfg, profile, alpn)
	if err != nil {
		t.Fatalf("newUConn: %v", err)
	}
	return uconn
}

func hasApplicationSettings(spec *utls.ClientHelloSpec) bool {
	for _, ext := range spec.Extensions {
		switch ext.(type) {
		case *utls.ApplicationSettingsExtension, *utls.ApplicationSettingsExtensionNew:
			return true
		}
	}
	return false
}

func TestNewUConnNarrowsPresetToHTTP1(t *testing.T) {
	uconn := newPipeUConn(t, ProfileChrome, WebSocketALPN)

	if uconn.ClientHelloID != utls.HelloCustom {
		t.Fatalf("client hello id = %v, want utls.HelloCustom", uconn.ClientHelloID)
	}
	if err := uconn.BuildHandshakeState(); err != nil {
		t.Fatalf("build handshake state: %v", err)
	}
	got := uconn.HandshakeState.Hello.AlpnProtocols
	if len(got) != 1 || got[0] != "http/1.1" {
		t.Fatalf("client hello ALPN = %v, want [http/1.1]", got)
	}
}

func TestNewUConnKeepsHTTP2Preset(t *testing.T) {
	uconn := newPipeUConn(t, ProfileChrome, HTTPTransportALPN)

	if uconn.ClientHelloID != utls.HelloChrome_Auto {
		t.Fatalf("client hello id = %v, want utls.HelloChrome_Auto", uconn.ClientHelloID)
	}
	if err := uconn.BuildHandshakeState(); err != nil {
		t.Fatalf("build handshake state: %v", err)
	}
	got := uconn.HandshakeState.Hello.AlpnProtocols
	if len(got) != 2 || got[0] != "h2" || got[1] != "http/1.1" {
		t.Fatalf("client hello ALPN = %v, want [h2 http/1.1]", got)
	}
}

func TestDropApplicationSettings(t *testing.T) {
	spec, err := utls.UTLSIdToSpec(utls.HelloChrome_Auto)
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	if !hasApplicationSettings(&spec) {
		t.Fatal("chrome preset does not contain an ALPS extension")
	}
	if !dropApplicationSettings(&spec) {
		t.Fatal("dropApplicationSettings reported no change")
	}
	if hasApplicationSettings(&spec) {
		t.Fatal("ALPS extension survived")
	}
	if alpnProtocols(&spec) == nil {
		t.Fatal("ALPN extension disappeared")
	}
	if dropApplicationSettings(&spec) {
		t.Fatal("second dropApplicationSettings call should be a no-op")
	}
}

func TestIsHTTP1Only(t *testing.T) {
	cases := map[string]struct {
		alpn []string
		want bool
	}{
		"ws":          {[]string{"http/1.1"}, true},
		"h2+http1.1":  {[]string{"h2", "http/1.1"}, false},
		"h2":          {[]string{"h2"}, false},
		"empty":       {nil, false},
		"http1 twice": {[]string{"http/1.1", "http/1.1"}, true},
	}
	for name, tc := range cases {
		if got := isHTTP1Only(tc.alpn); got != tc.want {
			t.Fatalf("%s: isHTTP1Only(%v) = %v, want %v", name, tc.alpn, got, tc.want)
		}
	}
}

func TestSetALPNReplacesPresetExtension(t *testing.T) {
	spec, err := utls.UTLSIdToSpec(utls.HelloChrome_Auto)
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	if !setALPN(&spec, WebSocketALPN) {
		t.Fatal("chrome preset has no ALPN extension")
	}
	found := false
	for _, ext := range spec.Extensions {
		if a, ok := ext.(*utls.ALPNExtension); ok {
			found = true
			if len(a.AlpnProtocols) != 1 || a.AlpnProtocols[0] != "http/1.1" {
				t.Fatalf("alpn = %v", a.AlpnProtocols)
			}
		}
	}
	if !found {
		t.Fatal("ALPN extension disappeared")
	}
}

func TestHostFromAddr(t *testing.T) {
	for addr, want := range map[string]string{
		"example.test:443": "example.test",
		"[::1]:8787":       "::1",
		"example.test":     "example.test",
	} {
		if got := HostFromAddr(addr); got != want {
			t.Fatalf("HostFromAddr(%q) = %q, want %q", addr, got, want)
		}
	}
}
