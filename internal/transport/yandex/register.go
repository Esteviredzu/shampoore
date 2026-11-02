package yandex

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/app"
	"github.com/Esteviredzu/shampoore-core/internal/core/reliability"
	"github.com/Esteviredzu/shampoore-core/internal/logging"
)

func init() {
	defaults := app.Values{
		"mux_flush_ms":        20,
		"mux_flush_threshold": 12000,

		"reliable_window": 64,
		"reliable_chunk":  4000,
	}

	err := app.RegisterDescriptor(app.Descriptor{
		Kind:        "yandex",
		DisplayName: "Yandex Board",
		Defaults:    defaults,
		DecodeURL:   decodeURL,
		EncodeURL:   encodeURL,
		Build:       buildTransport,
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
		ChunkSize:          eff.Int("reliable_chunk", 4000),
		EncryptionPassword: s.EncryptionPassword,
	})
	if err != nil {
		return nil, err
	}

	bypass := []string{
		"boards.yandex.ru",
		"socket63.boards.yandex.ru",
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