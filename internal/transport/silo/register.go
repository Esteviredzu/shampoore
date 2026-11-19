package silo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/core/security"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/netdial"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

func init() {
	defaults := app.Values{
		"mux_flush_ms":        20,
		"mux_flush_threshold": 12000,
	}

	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "silo",
		DisplayName: "Silo S3",
		Description: "Tunnel over Silo object storage",
		Fields: []app.Field{
			{Key: "endpoint", Label: "S3 Endpoint", Type: app.FieldTypeText, Required: true, Placeholder: "s3.kakaobaobab.casa"},
			{Key: "bucket", Label: "Bucket Name", Type: app.FieldTypeText, Required: true},
			{Key: "access_key", Label: "Access Key", Type: app.FieldTypeText, Required: true},
			{Key: "secret_key", Label: "Secret Key", Type: app.FieldTypePassword, Required: true},
			{Key: "user_id", Label: "User ID", Type: app.FieldTypeText, Required: false, Hint: "Isolate objects under users/<user_id>/"},
			{Key: "multiuser", Label: "Multiuser server", Type: app.FieldTypeBool, Required: false, Hint: "Server only: serve every users/<user_id>/ prefix"},
		},
		Defaults:  defaults,
		EncodeURL: encodeURL,
		DecodeURL: decodeURL,
		Validate:  validate,
		Build:     build,

		Aliases:                 []string{"silo"},
		BuildSubscriptionConfig: buildSubscriptionConfig,
		BuildServerConfig:       buildServerConfig,
		RelayUpstream:           "silo:9000",
		RelaySecrets:            []string{"minio_root_user", "minio_root_password", "s3_bucket"},
	})
	if err != nil {
		panic(err)
	}
}

func wrapEncryption(inner transport.Pipe, password, role string, logger logging.ContextLogger) (transport.Pipe, error) {
	if password == "" {
		return inner, nil
	}
	return security.WrapPipeWithRole(inner, password, role, logger)
}

func build(
	ctx context.Context,
	s *app.Settings,
	role string,
	logger logging.ContextLogger,
	eff app.Values,
) (*app.BuildResult, error) {
	cfg := DefaultConfig()
	if data := s.TransportData(); len(data) > 0 {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	}
	if cfg.UserID == "" {
		cfg.UserID = s.UserID
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	password := s.EncryptionPassword
	if cfg.EncryptionPassword != "" {
		if password != "" && password != cfg.EncryptionPassword {
			return nil, errors.New("silo: settings and transport encryption passwords differ")
		}
		password = cfg.EncryptionPassword
	}

	host := cfg.Endpoint
	if h, _, err := net.SplitHostPort(cfg.Endpoint); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	bypass := []string{}
	if host != "" {
		bypass = append(bypass, host)
		bypass = append(bypass, netdial.ResolveIPv4(ctx, host)...)
	}

	if role == "server" && cfg.Multiuser {
		registry, err := buildUserRegistry(cfg.Users)
		if err != nil {
			return nil, err
		}
		store, err := newMinioStore(&cfg)
		if err != nil {
			return nil, err
		}
		srv := newMultiServer(cfg, logger, registry, password, store, app.Values{
			"mux_flush_ms":        eff.Int("mux_flush_ms", 20),
			"mux_flush_threshold": eff.Int("mux_flush_threshold", 12000),
		})
		logger.Info("silo: multiuser server ready (endpoint=", cfg.Endpoint,
			", bucket=", cfg.Bucket, ", users=", len(cfg.Users), ")")
		return &app.BuildResult{
			SessionServer: srv,
			UserRegistry:  registry,
			BypassHosts:   bypass,
		}, nil
	}

	pipe, err := NewPipe(&cfg, role, logger)
	if err != nil {
		return nil, err
	}

	var secured transport.Pipe = pipe
	if password != "" {
		wrapped, err := wrapEncryption(pipe, password, role, logger)
		if err != nil {
			return nil, fmt.Errorf("silo: wrap encryption: %w", err)
		}
		secured = wrapped
		logger.Info("silo: XChaCha20-Poly1305 encryption enabled")
	}

	if err := secured.Start(ctx); err != nil {
		return nil, err
	}

	logger.Info("silo: ready (endpoint=", cfg.Endpoint, ", bucket=", cfg.Bucket,
		", role=", role, ", user=", cfg.UserID, ")")

	return &app.BuildResult{
		Transport:   newTransport(secured),
		BypassHosts: bypass,
	}, nil
}

func decodeURL(q url.Values) (json.RawMessage, error) {
	cfg := DefaultConfig()
	cfg.Endpoint = q.Get("endpoint")
	cfg.Bucket = q.Get("bucket")
	cfg.AccessKey = q.Get("access_key")
	cfg.SecretKey = q.Get("secret_key")
	cfg.EncryptionPassword = q.Get("encryption_password")
	cfg.UserID = q.Get("user_id")
	cfg.Multiuser, _ = strconv.ParseBool(q.Get("multiuser"))
	return json.Marshal(cfg)
}

func encodeURL(raw json.RawMessage, q url.Values) error {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	q.Set("endpoint", cfg.Endpoint)
	q.Set("bucket", cfg.Bucket)
	q.Set("access_key", cfg.AccessKey)
	q.Set("secret_key", cfg.SecretKey)
	if cfg.EncryptionPassword != "" {
		q.Set("encryption_password", cfg.EncryptionPassword)
	}
	if cfg.UserID != "" {
		q.Set("user_id", cfg.UserID)
	}
	if cfg.Multiuser {
		q.Set("multiuser", "1")
	}
	return nil
}

func validate(raw json.RawMessage) error {
	cfg := DefaultConfig()
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return err
		}
	}
	return cfg.Validate()
}

func routeTunnelID(p app.RouteParams) string {
	if p.RouteID > 0 {
		return "route-" + strconv.FormatInt(p.RouteID, 10)
	}
	if name := strings.TrimSpace(p.RouteName); name != "" {
		return name
	}
	return "route"
}

func routeSiloConfig(p app.RouteParams, server bool) Config {
	cfg := DefaultConfig()
	cfg.Endpoint = app.HostOnly(p.Domain)
	cfg.AccessKey = strings.TrimSpace(p.Secret("s3_access_key"))
	cfg.SecretKey = strings.TrimSpace(p.Secret("s3_secret_key"))
	if cfg.AccessKey == "" {
		cfg.AccessKey = p.Secret("minio_root_user")
	}
	if cfg.SecretKey == "" {
		cfg.SecretKey = p.Secret("minio_root_password")
	}
	cfg.Bucket = p.Secret("s3_bucket")
	cfg.UseSSL = !strings.HasPrefix(strings.ToLower(strings.TrimSpace(p.Domain)), "http://")
	cfg.TunnelID = routeTunnelID(p)
	cfg.EncryptionPassword = p.Secret("encryption_password")
	if server {
		cfg.Multiuser = true
		cfg.Users = make([]auth.UserSpec, 0)
	} else {
		cfg.UserID = p.UserID
	}
	return cfg
}

func buildSubscriptionConfig(p app.RouteParams) (app.Settings, error) {
	return siloSettings(routeSiloConfig(p, false), p, false)
}

func buildServerConfig(p app.RouteParams) (app.Settings, error) {
	return siloSettings(routeSiloConfig(p, true), p, true)
}

func siloSettings(cfg Config, p app.RouteParams, server bool) (app.Settings, error) {
	if err := cfg.Validate(); err != nil {
		return app.Settings{}, err
	}
	raw, err := app.MarshalTransportOptions(cfg, server)
	if err != nil {
		return app.Settings{}, err
	}
	out := app.Settings{
		Name:      p.RouteName,
		Type:      "silo",
		Transport: raw,
	}
	if !server {
		out.UserID = p.UserID
	}
	out.ExitNodeID = p.ExitNodeID
	out.EncryptionPassword = p.Secret("encryption_password")
	return out, nil
}
