package jellyfin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/capability"
	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/net/utlsdial"
	"github.com/Esteviredzu/shampoore/internal/netdial"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	defaultListen      = "0.0.0.0:8097"
	defaultUpstreamURL = "http://jellyfin:8096"
)

type Config struct {
	URL         string `json:"url,omitempty"`
	Secret      string `json:"secret"`
	Namespace   string `json:"namespace,omitempty"`
	UpstreamURL string `json:"upstream_url,omitempty"`
	Listen      string `json:"listen,omitempty"`
	UTLSProfile string `json:"utls_profile,omitempty"`
	Mode        string `json:"mode,omitempty"`
	Role        string `json:"role,omitempty"`

	UserID            string          `json:"user_id,omitempty"`
	Multiuser         bool            `json:"multiuser,omitempty"`
	Users             []auth.UserSpec `json:"users,omitempty"`
	AllowInsecureHTTP bool            `json:"allow_insecure_http,omitempty"`
}

const (
	modePair   = "pair"
	modeDirect = "direct"
)

func (c Config) withDefaults() Config {
	c.URL = strings.TrimSpace(c.URL)
	c.Secret = strings.TrimSpace(c.Secret)
	c.Namespace = strings.TrimSpace(c.Namespace)
	c.UpstreamURL = strings.TrimSpace(c.UpstreamURL)
	c.Listen = strings.TrimSpace(c.Listen)
	c.UTLSProfile = strings.TrimSpace(c.UTLSProfile)
	c.Mode = strings.ToLower(strings.TrimSpace(c.Mode))
	c.Role = strings.ToLower(strings.TrimSpace(c.Role))
	c.UserID = strings.TrimSpace(c.UserID)
	if c.Namespace == "" {
		c.Namespace = "default"
	}
	if c.Mode == "" {
		c.Mode = modePair
	}
	return c
}

func (c Config) Validate() error {
	c = c.withDefaults()
	if c.Secret == "" {
		return errors.New("jellyfin: secret is required")
	}
	switch c.Mode {
	case modePair, modeDirect:
	default:
		return fmt.Errorf("jellyfin: unsupported mode %q", c.Mode)
	}
	switch c.Role {
	case "", "client", "exit":
	default:
		return fmt.Errorf("jellyfin: unsupported role %q", c.Role)
	}
	if c.UserID != "" {
		if _, err := auth.NormalizeUserID(c.UserID); err != nil {
			return fmt.Errorf("jellyfin: %w", err)
		}
	}
	if c.URL != "" {
		if err := validateHTTPURL(c.URL, c.AllowInsecureHTTP); err != nil {
			return err
		}
	}
	if c.UpstreamURL != "" {
		if err := validateHTTPURL(c.UpstreamURL, true); err != nil {
			return fmt.Errorf("jellyfin: upstream_url: %w", err)
		}
	}
	if c.Listen != "" {
		if _, _, err := net.SplitHostPort(c.Listen); err != nil {
			return fmt.Errorf("jellyfin: listen: %w", err)
		}
	}
	return nil
}

func (c Config) validateForRole(role string) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if role == "server" && c.Mode == modeDirect {
		return nil
	}
	if strings.TrimSpace(c.URL) == "" {
		return errors.New("jellyfin: url is required")
	}
	if role == "server" && c.Role != "" && c.Role != "exit" {
		return fmt.Errorf("jellyfin: server role must be exit, got %q", c.Role)
	}
	return nil
}

func validateHTTPURL(raw string, allowInsecure bool) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid url %q: %w", raw, err)
	}
	if u.Host == "" {
		return fmt.Errorf("invalid url %q: host is required", raw)
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return nil
	case "http":
		if !allowInsecure {
			return fmt.Errorf("insecure http:// is disabled for %q", raw)
		}
		return nil
	default:
		return fmt.Errorf("unsupported url scheme %q", u.Scheme)
	}
}

func init() {
	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "jellyfin",
		DisplayName: "Jellyfin",
		Description: "Jellyfin-shaped HTTP/2 media stream transport",
		Fields: []app.Field{
			{
				Key:         "url",
				Label:       "Public URL",
				Type:        app.FieldTypeText,
				Required:    true,
				Placeholder: "https://media.example.com",
			},
			{
				Key:      "secret",
				Label:    "Session Secret",
				Type:     app.FieldTypePassword,
				Required: true,
				Hint:     "Shared HMAC secret used to sign X-Emby-Token values (relay broker, clients and exits)",
			},
			{
				Key:         "upstream_url",
				Label:       "Real Jellyfin URL",
				Type:        app.FieldTypeText,
				Required:    false,
				Placeholder: defaultUpstreamURL,
				Hint:        "Direct mode only: proxy requests without a valid token here (empty = built-in stub)",
			},
			{
				Key:         "listen",
				Label:       "Listen Address",
				Type:        app.FieldTypeText,
				Required:    false,
				Placeholder: defaultListen,
				Hint:        "Direct mode only: plain HTTP listener behind Caddy; pair mode uses the relay broker",
			},
			{
				Key:         "utls_profile",
				Label:       "uTLS Profile",
				Type:        app.FieldTypeText,
				Required:    false,
				Advanced:    true,
				Options:     utlsdial.SupportedProfiles(),
				Placeholder: "chrome",
				Hint:        "chrome (default), random, firefox, safari, ios, edge",
			},
			{
				Key:      "user_id",
				Label:    "User ID",
				Type:     app.FieldTypeText,
				Required: false,
				Hint:     "Multiuser mode: personal tunnel identity",
			},
			{
				Key:      "multiuser",
				Label:    "Multiuser server",
				Type:     app.FieldTypeBool,
				Required: false,
				Hint:     "Server only: authorize every request through the user registry",
			},
		},
		Defaults: app.Values{
			"mux_flush_ms":        20,
			"mux_flush_threshold": 12000,
		},
		Caps: app.Capabilities{
			Relay:     relayProvider{},
			Multiuser: app.MultiuserConfig{},
			Secrets: app.SecretProviderFunc(func() []capability.SecretSpec {
				return []capability.SecretSpec{{
					Key:       "jellyfin_secret",
					Label:     "Jellyfin Session Secret",
					Generator: capability.SecretGenerator{Kind: "hex", Bytes: 32},
					Editable:  true,
				}}
			}),
		},
		DecodeURL: decodeURL,
		EncodeURL: encodeURL,
		Validate:  validate,
		Build:     build,

		Aliases:                 []string{"jellyfin"},
		BuildSubscriptionConfig: buildSubscriptionConfig,
		BuildServerConfig:       buildServerConfig,
	})
	if err != nil {
		panic(err)
	}
}

func decodeURL(q url.Values) (json.RawMessage, error) {
	cfg := Config{
		URL:         strings.TrimSpace(q.Get("url")),
		Secret:      strings.TrimSpace(q.Get("secret")),
		Namespace:   strings.TrimSpace(q.Get("ns")),
		UTLSProfile: strings.TrimSpace(q.Get("utls_profile")),
		UserID:      strings.TrimSpace(q.Get("user_id")),
	}
	cfg.Multiuser, _ = strconv.ParseBool(q.Get("multiuser"))
	cfg.AllowInsecureHTTP, _ = strconv.ParseBool(q.Get("allow_insecure_http"))
	if cfg.URL == "" || cfg.Secret == "" {
		return nil, errors.New("jellyfin: url and secret are required")
	}
	return json.Marshal(cfg)
}

func encodeURL(raw json.RawMessage, q url.Values) error {
	if len(raw) == 0 {
		return errors.New("jellyfin: empty transport config")
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	cfg = cfg.withDefaults()
	if cfg.URL != "" {
		q.Set("url", cfg.URL)
	}
	if cfg.Secret != "" {
		q.Set("secret", cfg.Secret)
	}
	if cfg.Namespace != "" && cfg.Namespace != "default" {
		q.Set("ns", cfg.Namespace)
	}
	if cfg.UTLSProfile != "" {
		q.Set("utls_profile", cfg.UTLSProfile)
	}
	if cfg.UserID != "" {
		q.Set("user_id", cfg.UserID)
	}
	if cfg.Multiuser {
		q.Set("multiuser", "1")
	}
	if cfg.AllowInsecureHTTP {
		q.Set("allow_insecure_http", "1")
	}
	return nil
}

func build(
	ctx context.Context,
	s *app.Settings,
	role string,
	logger logging.ContextLogger,
	eff app.Values,
) (*app.BuildResult, error) {
	cfg := Config{}
	if data := s.TransportData(); len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	explicitMode := strings.TrimSpace(cfg.Mode) != ""
	cfg = cfg.withDefaults()
	if role == "server" && !explicitMode && cfg.Listen != "" {
		cfg.Mode = modeDirect
	}
	if cfg.UserID == "" && (role != "server" || !cfg.Multiuser) {
		cfg.UserID = strings.TrimSpace(s.UserID)
	}
	if cfg.UserID != "" {
		normalized, err := auth.NormalizeUserID(cfg.UserID)
		if err != nil {
			return nil, fmt.Errorf("jellyfin: %w", err)
		}
		cfg.UserID = normalized
	}
	if err := cfg.validateForRole(role); err != nil {
		return nil, err
	}

	bypass := bypassHosts(ctx, cfg.URL)
	if role == "server" {
		registry, err := buildUserRegistry(cfg.Users)
		if err != nil {
			return nil, err
		}
		if cfg.Mode == modeDirect {
			srv, err := newServer(cfg, logger, registry, s.EncryptionPassword)
			if err != nil {
				return nil, err
			}
			srv.SetExitNodeID(s.ExitNodeID)
			logger.Info("jellyfin: direct server ready (listen=", cfg.Listen,
				" upstream=", cfg.UpstreamURL, " users=", len(cfg.Users), ")")
			return &app.BuildResult{
				SessionServer: srv,
				UserRegistry:  registry,
				BypassHosts:   bypass,
			}, nil
		}
		if cfg.Role == "" {
			cfg.Role = "exit"
		}
		muxCfg := tunnel.Config{
			FlushThreshold: eff.Int("mux_flush_threshold", defaultFlushThreshold),
			FlushInterval:  time.Duration(eff.Int("mux_flush_ms", int(defaultFlushInterval/time.Millisecond))) * time.Millisecond,
			Registry:       registry,
		}
		srv := newMultiServer(cfg, logger, registry, s.EncryptionPassword, muxCfg)
		srv.SetExitNodeID(s.ExitNodeID)
		logger.Info("jellyfin: exit server ready (url=", cfg.URL,
			" users=", len(cfg.Users), " role=", cfg.Role, ")")
		return &app.BuildResult{
			SessionServer: srv,
			UserRegistry:  registry,
			BypassHosts:   bypass,
		}, nil
	}

	if cfg.Role == "" {
		cfg.Role = "client"
	}

	client, err := newClient(cfg, s.ExitNodeID, nil, logger)
	if err != nil {
		return nil, err
	}
	var tr transport.Transport = client
	if strings.TrimSpace(s.EncryptionPassword) != "" {
		secured, err := security.WrapTransport(client, s.EncryptionPassword, security.RoleClient, logger)
		if err != nil {
			return nil, fmt.Errorf("jellyfin: wrap encryption: %w", err)
		}
		tr = notifyTransport{Transport: secured, notify: client.NotifyNetworkChanged}
		logger.Info("jellyfin: XChaCha20-Poly1305 encryption enabled")
	}
	logger.Info("jellyfin: client ready (url=", cfg.URL, " user=", cfg.UserID,
		" utls=", cfg.UTLSProfile, ")")
	return &app.BuildResult{Transport: tr, BypassHosts: bypass}, nil
}

func validate(raw json.RawMessage) error {
	cfg := Config{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
	}
	return cfg.Validate()
}

func bypassHosts(ctx context.Context, rawURL string) []string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return nil
	}
	host := u.Hostname()
	out := []string{host}
	out = append(out, netdial.ResolveIPv4(ctx, host)...)
	return out
}

func routeConfig(p app.RouteParams, server bool) Config {
	cfg := Config{
		URL:       app.NormalizeHTTPSURL(p.Domain),
		Secret:    strings.TrimSpace(p.Secret("jellyfin_secret")),
		Namespace: p.Namespace(),
	}
	if strings.HasPrefix(strings.ToLower(cfg.URL), "http://") {
		cfg.AllowInsecureHTTP = true
	}
	if server {
		cfg.Mode = modePair
		cfg.Role = "exit"
		cfg.Multiuser = true
		cfg.Users = make([]auth.UserSpec, 0)
	} else {
		cfg.Mode = modePair
		cfg.Role = "client"
		cfg.UserID = p.UserID
	}
	return cfg
}

func buildSubscriptionConfig(p app.RouteParams) (app.Settings, error) {
	return jellyfinSettings(routeConfig(p, false), p, false)
}

func buildServerConfig(p app.RouteParams) (app.Settings, error) {
	return jellyfinSettings(routeConfig(p, true), p, true)
}

func jellyfinSettings(cfg Config, p app.RouteParams, server bool) (app.Settings, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return app.Settings{}, err
	}
	if !server && cfg.UserID == "" {
		return app.Settings{}, errors.New("jellyfin: user_id is required for subscriptions")
	}
	raw, err := app.MarshalTransportOptions(cfg, server)
	if err != nil {
		return app.Settings{}, err
	}
	out := app.Settings{
		Name:      p.RouteName,
		Type:      "jellyfin",
		Transport: raw,
	}
	if !server {
		out.UserID = p.UserID
	}
	out.ExitNodeID = p.ExitNodeID
	out.EncryptionPassword = p.Secret("encryption_password")
	return out, nil
}
