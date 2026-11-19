package yandex

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/core/reliability"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/netdial"
)

func init() {
	defaults := app.Values{
		"mux_flush_ms":        20,
		"mux_flush_threshold": 12000,

		"reliable_window": 64,
		"reliable_chunk":  9000,
	}

	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "yandex",
		DisplayName: "Yandex Board",
		Description: "Yandex Board as a channel",
		Fields: []app.Field{
			{
				Key:      "hash",
				Label:    "Board Hash",
				Type:     app.FieldTypeText,
				Required: true,
				Hint:     "32 hex chars from the board URL",
			},
		},
		Defaults:  defaults,
		DecodeURL: decodeURL,
		EncodeURL: encodeURL,
		Validate:  validate,
		Build:     buildTransport,
	})
	if err != nil {
		panic(err)
	}
}

func buildTransport(
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

	creds, err := Bootstrap(ctx, cfg.Hash)
	if err != nil {
		return nil, err
	}

	logger.Info("yandex: bootstrap ok; hash=", creds.Presentation)
	logger.Info("yandex: participant=", creds.Participant)

	boardCfg := DefaultBoardConfig()
	boardCfg.QueueSize = cfg.BoardQueue
	boardCfg.HeartbeatDisabled = cfg.HeartbeatDisabled

	client := NewBoardClient(logger, creds, boardCfg)
	if err := client.Connect(ctx); err != nil {
		return nil, err
	}

	client.StartPeriodicHeartbeat(ctx)

	pipeCfg := DefaultPipeConfig()
	pipeCfg.QueueSize = cfg.PipeQueue
	pipeCfg.SendBatch = cfg.SendBatch
	pipeCfg.SendFlushMs = cfg.SendFlushMs
	pipeCfg.DeleteEnabled = cfg.EffectiveDeleteEnabled()
	pipeCfg.DeleteBatch = cfg.DeleteBatch
	pipeCfg.DeleteFlushMs = cfg.DeleteFlushMs
	pipeCfg.StatsEnabled = !cfg.StatsDisabled

	pipe := NewYandexPipe(client, logger, pipeCfg)

	session, err := reliability.NewReliableSession(pipe, logger, reliability.Config{
		Window:             eff.Int("reliable_window", 64),
		ChunkSize:          eff.Int("reliable_chunk", 9000),
		EncryptionPassword: s.EncryptionPassword,
		Role:               role,
	})
	if err != nil {
		return nil, err
	}

	bypassHosts := []string{
		"boards.yandex.ru",
		"socket63.boards.yandex.ru",
	}
	bypass := append([]string{}, bypassHosts...)
	for _, host := range bypassHosts {
		bypass = append(bypass, netdial.ResolveIPv4(ctx, host)...)
	}

	var sides []app.SideService
	if role == "server" && cfg.EffectiveWorkerEnabled() {
		workerCfg := WorkerConfig{
			Threshold:    time.Duration(cfg.WorkerThresholdSec) * time.Second,
			ScanInterval: time.Duration(cfg.WorkerScanSec) * time.Second,
			BatchSize:    cfg.WorkerBatch,
			BatchPause:   time.Duration(cfg.WorkerBatchPauseMs) * time.Millisecond,
		}
		sides = append(sides, NewCleanupWorker(creds, logger, workerCfg))
	}

	return &app.BuildResult{
		Transport:   session,
		BypassHosts: bypass,
		Side:        sides,
	}, nil
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
