package utlsdial

import (
	"bufio"
	"bytes"
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

func TestNewHTTP1TransportForcesHTTP1PUT(t *testing.T) {
	const putSize = 300 * 1024
	payload := bytes.Repeat([]byte("shampoore-silo"), putSize/len("shampoore-silo"))
	payload = append(payload, []byte("tail")...)

	srv := newCaddyTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			t.Error("PUT handler got no TLS state")
			http.Error(w, "tls required", http.StatusBadRequest)
			return
		}
		if got := r.TLS.NegotiatedProtocol; got != "http/1.1" {
			t.Errorf("negotiated ALPN = %q, want http/1.1", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read PUT body: %v", err)
			http.Error(w, "read", http.StatusBadRequest)
			return
		}
		if !bytes.Equal(body, payload) {
			t.Errorf("PUT body = %d bytes, want %d", len(body), len(payload))
			http.Error(w, "body mismatch", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	tr := NewHTTP1Transport(ProfileChrome)
	defer tr.CloseIdleConnections()
	if tr.ForceAttemptHTTP2 {
		t.Fatal("HTTP/1.1 transport still forces HTTP/2")
	}
	if tr.Protocols == nil || tr.Protocols.HTTP2() || !tr.Protocols.HTTP1() {
		t.Fatalf("unexpected protocols: %+v", tr.Protocols)
	}

	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/bucket/key", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.ProtoMajor != 1 {
		t.Fatalf("response protocol = %s, want HTTP/1.x", resp.Proto)
	}
	if resp.TLS == nil || resp.TLS.NegotiatedProtocol != "http/1.1" {
		t.Fatalf("unexpected negotiated protocol: %+v", resp.TLS)
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
		"default":           utls.HelloChrome_Auto,
		"unknown":           utls.HelloChrome_Auto,
		ProfileFirefox:      utls.HelloFirefox_Auto,
		ProfileSafari:       utls.HelloSafari_Auto,
		ProfileIOS:          utls.HelloIOS_Auto,
		ProfileEdge:         utls.HelloEdge_Auto,
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
	if ua := UserAgent(ProfileIOS); !strings.Contains(ua, "iPhone") {
		t.Fatalf("ios UA = %q", ua)
	}
	if ua := UserAgent(ProfileEdge); !strings.Contains(ua, "Edg/") {
		t.Fatalf("edge UA = %q", ua)
	}
	if ua := UserAgent("bogus"); !strings.Contains(ua, "Chrome/") {
		t.Fatalf("fallback UA = %q", ua)
	}
	if ua := UserAgent(ProfileRandom); ua == "" {
		t.Fatal("random UA is empty")
	}
}

func TestNormalizeProfile(t *testing.T) {
	cases := map[Profile]Profile{
		"":             ProfileChrome,
		"default":      ProfileChrome,
		"unknown":      ProfileChrome,
		" CHROME ":     ProfileChrome,
		"firefox":      ProfileFirefox,
		" SAFARI ":     ProfileSafari,
		"iOS":          ProfileIOS,
		"Edge":         ProfileEdge,
		" random ":     ProfileRandom,
		"f1refox-typo": ProfileChrome,
	}
	for raw, want := range cases {
		if got := NormalizeProfile(raw); got != want {
			t.Fatalf("NormalizeProfile(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestSupportedProfiles(t *testing.T) {
	want := []string{"chrome", "random", "firefox", "safari", "ios", "edge"}
	got := SupportedProfiles()
	if len(got) != len(want) {
		t.Fatalf("SupportedProfiles() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SupportedProfiles()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	got[0] = "mutated"
	if SupportedProfiles()[0] != "chrome" {
		t.Fatal("SupportedProfiles() must return a copy")
	}
}

func TestRandomHelloPool(t *testing.T) {
	if len(randomHelloPool) == 0 {
		t.Fatal("random hello pool is empty")
	}
	required := []utls.ClientHelloID{
		utls.HelloChrome_Auto,
		utls.HelloFirefox_Auto,
		utls.HelloSafari_Auto,
		utls.HelloIOS_Auto,
		utls.HelloEdge_Auto,
	}
	for _, want := range required {
		found := false
		for _, entry := range randomHelloPool {
			if entry.hello == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("random pool misses %v", want)
		}
	}

	seen := map[utls.ClientHelloID]struct{}{}
	for i := 0; i < 128; i++ {
		hello, family := resolveHello(ProfileRandom)
		if family == "" || family == ProfileRandom {
			t.Fatalf("resolveHello(random) family = %q", family)
		}
		found := false
		for _, entry := range randomHelloPool {
			if entry.hello == hello && entry.profile == family {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("resolveHello(random) returned %v/%q outside the pool", hello, family)
		}
		seen[hello] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("random profile produced a single hello: %v", seen)
	}
}

func TestRandomHelloPoolSupportsTLS13(t *testing.T) {
	for _, entry := range randomHelloPool {
		spec, err := utls.UTLSIdToSpec(entry.hello)
		if err != nil {
			t.Fatalf("UTLSIdToSpec(%v): %v", entry.hello, err)
		}
		offers := false
		for _, ext := range spec.Extensions {
			versions, ok := ext.(*utls.SupportedVersionsExtension)
			if !ok {
				continue
			}
			for _, v := range versions.Versions {
				if v == tls.VersionTLS13 {
					offers = true
					break
				}
			}
		}
		if !offers {
			t.Fatalf("profile %q (%v) does not offer TLS 1.3", entry.profile, entry.hello)
		}
	}
}

func TestDialContextRandomProfile(t *testing.T) {
	srv := newTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}), "http/1.1")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 4; i++ {
		conn, err := DialContext(ctx, "tcp", serverAddr(t, srv), "", WebSocketALPN, ProfileRandom)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		_ = conn.Close()
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

func TestDialerUsesFastHappyEyeballsFallback(t *testing.T) {
	d := newDialer()
	if d.FallbackDelay != happyEyeballsDelay {
		t.Fatalf("FallbackDelay = %s, want %s", d.FallbackDelay, happyEyeballsDelay)
	}
	if happyEyeballsDelay <= 0 || happyEyeballsDelay > 250*time.Millisecond {
		t.Fatalf("happy eyeballs delay %s is outside the 0..250ms budget", happyEyeballsDelay)
	}
	if d.Timeout != dialTimeout {
		t.Fatalf("connect timeout = %s, want %s", d.Timeout, dialTimeout)
	}
	if d.Control == nil {
		t.Fatal("socket protector hook is not wired into the dialer")
	}
	if d.KeepAlive != keepAlivePeriod {
		t.Fatalf("keepalive = %s, want %s", d.KeepAlive, keepAlivePeriod)
	}
}

func TestTransportDialersUseTheHappyEyeballsDialer(t *testing.T) {
	httpTr := NewHTTPTransport(ProfileChrome)
	if httpTr.DialContext == nil || httpTr.DialTLSContext == nil {
		t.Fatal("HTTP transport is missing its dial functions")
	}
	h1Tr := NewHTTP1Transport(ProfileChrome)
	if h1Tr.DialContext == nil || h1Tr.DialTLSContext == nil {
		t.Fatal("HTTP/1.1 transport is missing its dial functions")
	}
	ws := NewWebSocketDialer(ProfileChrome)
	if ws.NetDialContext == nil || ws.NetDialTLSContext == nil {
		t.Fatal("websocket dialer is missing its dial functions")
	}
}
