package utlsdial

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	utls "github.com/refraction-networking/utls"

	"github.com/Esteviredzu/shampoore/internal/netdial"
)

const (
	dialTimeout         = 30 * time.Second
	tlsHandshakeTimeout = 10 * time.Second
	wsHandshakeTimeout  = 10 * time.Second
	idleConnTimeout     = 90 * time.Second
	keepAlivePeriod     = 30 * time.Second

	happyEyeballsDelay = 250 * time.Millisecond
)

func newDialer() *net.Dialer {
	return &net.Dialer{
		Timeout:       dialTimeout,
		KeepAlive:     keepAlivePeriod,
		FallbackDelay: happyEyeballsDelay,
		Control:       netdial.Control,
	}
}

type Profile string

const (
	ProfileChrome  Profile = "chrome"
	ProfileFirefox Profile = "firefox"
	ProfileSafari  Profile = "safari"
	ProfileIOS     Profile = "ios"
	ProfileEdge    Profile = "edge"

	ProfileRandom Profile = "random"

	DefaultProfile = ProfileChrome
)

var supportedProfiles = []string{
	string(ProfileChrome),
	string(ProfileRandom),
	string(ProfileFirefox),
	string(ProfileSafari),
	string(ProfileIOS),
	string(ProfileEdge),
}

func SupportedProfiles() []string {
	return append([]string(nil), supportedProfiles...)
}

var HTTPTransportALPN = []string{"h2", "http/1.1"}

var HTTP1ALPN = []string{"http/1.1"}

var WebSocketALPN = []string{"http/1.1"}

var userAgents = map[Profile]string{
	ProfileChrome:  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	ProfileFirefox: "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
	ProfileSafari:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
	ProfileIOS:     "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
	ProfileEdge:    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
}

var insecureSkipVerify atomic.Bool

func NormalizeProfile(p Profile) Profile {
	switch Profile(strings.ToLower(strings.TrimSpace(string(p)))) {
	case ProfileFirefox:
		return ProfileFirefox
	case ProfileSafari:
		return ProfileSafari
	case ProfileIOS:
		return ProfileIOS
	case ProfileEdge:
		return ProfileEdge
	case ProfileRandom:
		return ProfileRandom
	case ProfileChrome, "default":
		return ProfileChrome
	default:
		return DefaultProfile
	}
}

type helloProfile struct {
	profile Profile
	hello   utls.ClientHelloID
}

var randomHelloPool = []helloProfile{
	{ProfileChrome, utls.HelloChrome_Auto},
	{ProfileFirefox, utls.HelloFirefox_Auto},
	{ProfileSafari, utls.HelloSafari_Auto},
	{ProfileIOS, utls.HelloIOS_Auto},
	{ProfileEdge, utls.HelloEdge_Auto},
	{ProfileChrome, utls.HelloChrome_133},
	{ProfileChrome, utls.HelloChrome_131},
	{ProfileChrome, utls.HelloChrome_120},
	{ProfileFirefox, utls.HelloFirefox_120},
	{ProfileFirefox, utls.HelloFirefox_105},
	{ProfileSafari, utls.HelloSafari_16_0},
	{ProfileIOS, utls.HelloIOS_14},
}

func pickRandomHello() helloProfile {
	if len(randomHelloPool) == 0 {
		return helloProfile{profile: DefaultProfile, hello: utls.HelloChrome_Auto}
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return randomHelloPool[0]
	}
	idx := int(binary.BigEndian.Uint64(b[:]) % uint64(len(randomHelloPool)))
	return randomHelloPool[idx]
}

func resolveHello(p Profile) (utls.ClientHelloID, Profile) {
	switch NormalizeProfile(p) {
	case ProfileFirefox:
		return utls.HelloFirefox_Auto, ProfileFirefox
	case ProfileSafari:
		return utls.HelloSafari_Auto, ProfileSafari
	case ProfileIOS:
		return utls.HelloIOS_Auto, ProfileIOS
	case ProfileEdge:
		return utls.HelloEdge_Auto, ProfileEdge
	case ProfileRandom:
		chosen := pickRandomHello()
		return chosen.hello, chosen.profile
	default:
		return utls.HelloChrome_Auto, ProfileChrome
	}
}

func clientHelloID(p Profile) utls.ClientHelloID {
	hello, _ := resolveHello(p)
	return hello
}

func UserAgent(p Profile) string {
	p = NormalizeProfile(p)
	if p == ProfileRandom {
		p = pickRandomHello().profile
	}
	if ua, ok := userAgents[p]; ok {
		return ua
	}
	return userAgents[DefaultProfile]
}

func DialContext(ctx context.Context, network, addr, serverName string, alpn []string, profile Profile) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, errors.New("utlsdial: empty address")
	}
	if network == "" {
		network = "tcp"
	}
	if serverName == "" {
		serverName = HostFromAddr(addr)
	}

	dialer := newDialer()
	raw, err := dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("utlsdial: dial %s: %w", addr, err)
	}

	cfg := &utls.Config{
		ServerName:         serverName,
		NextProtos:         append([]string(nil), alpn...),
		MinVersion:         utls.VersionTLS12,
		InsecureSkipVerify: insecureSkipVerify.Load(),
	}
	uconn, err := newUConn(raw, cfg, profile, alpn)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("utlsdial: client hello: %w", err)
	}

	handshakeCtx, cancel := context.WithTimeout(ctx, tlsHandshakeTimeout)
	defer cancel()
	if err := uconn.HandshakeContext(handshakeCtx); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("utlsdial: tls handshake %s: %w", serverName, err)
	}
	return &tlsConn{UConn: uconn}, nil
}

func NewHTTPTransport(profile Profile) *http.Transport {
	profile = NormalizeProfile(profile)
	tr := &http.Transport{
		Proxy:       http.ProxyFromEnvironment,
		DialContext: newDialer().DialContext,
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return DialContext(ctx, network, addr, HostFromAddr(addr), HTTPTransportALPN, profile)
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ExpectContinueTimeout: time.Second,
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	tr.Protocols = protocols
	return tr
}

func NewHTTP1Transport(profile Profile) *http.Transport {
	profile = NormalizeProfile(profile)
	tr := &http.Transport{
		Proxy:       http.ProxyFromEnvironment,
		DialContext: newDialer().DialContext,
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return DialContext(ctx, network, addr, HostFromAddr(addr), HTTP1ALPN, profile)
		},
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ExpectContinueTimeout: time.Second,
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(false)
	tr.Protocols = protocols
	return tr
}

func NewWebSocketDialer(profile Profile) *websocket.Dialer {
	profile = NormalizeProfile(profile)
	d := &websocket.Dialer{
		HandshakeTimeout: wsHandshakeTimeout,
		NetDialContext:   newDialer().DialContext,
	}
	d.NetDialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return DialContext(ctx, network, addr, HostFromAddr(addr), WebSocketALPN, profile)
	}
	return d
}

func HostFromAddr(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(strings.TrimSpace(addr), "[]")
}

func newUConn(raw net.Conn, cfg *utls.Config, profile Profile, alpn []string) (*utls.UConn, error) {
	hello := clientHelloID(profile)
	if len(alpn) == 0 {
		return utls.UClient(raw, cfg, hello), nil
	}
	spec, err := utls.UTLSIdToSpec(hello)
	if err != nil {
		return nil, err
	}
	presetALPN := alpnProtocols(&spec)
	if !setALPN(&spec, alpn) {
		spec.Extensions = append(spec.Extensions, &utls.ALPNExtension{
			AlpnProtocols: append([]string(nil), alpn...),
		})
		presetALPN = nil
	}

	droppedALPS := false
	if isHTTP1Only(alpn) {
		droppedALPS = dropApplicationSettings(&spec)
	}
	if !droppedALPS && equalProtocols(presetALPN, alpn) {
		return utls.UClient(raw, cfg, hello), nil
	}

	uconn := utls.UClient(raw, cfg, utls.HelloCustom)
	if err := uconn.ApplyPreset(&spec); err != nil {
		return nil, err
	}
	return uconn, nil
}

func setALPN(spec *utls.ClientHelloSpec, alpn []string) bool {
	for _, ext := range spec.Extensions {
		if a, ok := ext.(*utls.ALPNExtension); ok {
			a.AlpnProtocols = append([]string(nil), alpn...)
			return true
		}
	}
	return false
}

func alpnProtocols(spec *utls.ClientHelloSpec) []string {
	for _, ext := range spec.Extensions {
		if a, ok := ext.(*utls.ALPNExtension); ok {
			return append([]string(nil), a.AlpnProtocols...)
		}
	}
	return nil
}

func equalProtocols(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func isHTTP1Only(alpn []string) bool {
	if len(alpn) == 0 {
		return false
	}
	for _, proto := range alpn {
		if proto != "http/1.1" {
			return false
		}
	}
	return true
}

func dropApplicationSettings(spec *utls.ClientHelloSpec) bool {
	kept := spec.Extensions[:0]
	dropped := false
	for _, ext := range spec.Extensions {
		switch ext.(type) {
		case *utls.ApplicationSettingsExtension, *utls.ApplicationSettingsExtensionNew:
			dropped = true
			continue
		}
		kept = append(kept, ext)
	}
	spec.Extensions = kept
	return dropped
}

type tlsConn struct {
	*utls.UConn
}

func (c *tlsConn) ConnectionState() tls.ConnectionState {
	s := c.UConn.ConnectionState()
	return tls.ConnectionState{
		Version:                     s.Version,
		HandshakeComplete:           s.HandshakeComplete,
		DidResume:                   s.DidResume,
		CipherSuite:                 s.CipherSuite,
		NegotiatedProtocol:          s.NegotiatedProtocol,
		NegotiatedProtocolIsMutual:  s.NegotiatedProtocolIsMutual,
		ServerName:                  s.ServerName,
		PeerCertificates:            s.PeerCertificates,
		VerifiedChains:              s.VerifiedChains,
		SignedCertificateTimestamps: s.SignedCertificateTimestamps,
		OCSPResponse:                s.OCSPResponse,
		TLSUnique:                   s.TLSUnique,
		ECHAccepted:                 s.ECHAccepted,
	}
}
