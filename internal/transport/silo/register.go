package silo

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"
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
		},
		Defaults:  defaults,
		EncodeURL: encodeURL,
		DecodeURL: decodeURL,
		Build:     build,
	})
	if err != nil {
		panic(err)
	}
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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	pipe, err := NewPipe(&cfg, role, logger)
	if err != nil {
		return nil, err
	}

	if err := pipe.Start(ctx); err != nil {
		return nil, err
	}

	logger.Info("silo: ready (endpoint=", cfg.Endpoint, ", bucket=", cfg.Bucket, ", role=", role, ")")

	return &app.BuildResult{
		Transport: newTransport(pipe),
	}, nil
}

func decodeURL(q url.Values) (json.RawMessage, error) {
	cfg := DefaultConfig()
	cfg.Endpoint = q.Get("endpoint")
	cfg.Bucket = q.Get("bucket")
	cfg.AccessKey = q.Get("access_key")
	cfg.SecretKey = q.Get("secret_key")
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
	return nil
}
