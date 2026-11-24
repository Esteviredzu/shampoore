package wsshape

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
)

const (
	FileName = "ws-shape.json"

	SupportedVersion = 1

	MaxFileBytes = 256 << 10
)

type Frame struct {
	HeaderBytes     int `json:"header_bytes"`
	FrameVersion    int `json:"frame_version"`
	MaxFrameBytes   int `json:"max_frame_bytes"`
	MaxMessageBytes int `json:"max_message_bytes"`
}

type Batch struct {
	WindowMs      int `json:"window_ms"`
	JitterMs      int `json:"jitter_ms"`
	MaxBytes      int `json:"max_bytes"`
	QueueCapacity int `json:"queue_capacity"`
	AckTimeoutMs  int `json:"ack_timeout_ms"`
}

type Padding struct {
	Enabled         *bool `json:"enabled,omitempty"`
	MinTotalBytes   int   `json:"min_total_bytes"`
	MaxExtraBytes   int   `json:"max_extra_bytes"`
	SmallExtraBytes int   `json:"small_extra_bytes"`
}

func (p Padding) On() bool {
	return p.Enabled == nil || *p.Enabled
}

func (p *Padding) SetEnabled(on bool) {
	p.Enabled = &on
}

type Pulse struct {
	PingIntervalMs int `json:"ping_interval_ms"`
	PingTimeoutMs  int `json:"ping_timeout_ms"`
	ReadIdleMs     int `json:"read_idle_ms"`
	WriteTimeoutMs int `json:"write_timeout_ms"`
}

type Lifecycle struct {
	WarmupMinMs     int `json:"warmup_min_ms"`
	WarmupMaxMs     int `json:"warmup_max_ms"`
	RotationMinMs   int `json:"rotation_min_ms"`
	RotationMaxMs   int `json:"rotation_max_ms"`
	ReconnectBaseMs int `json:"reconnect_base_ms"`
	ReconnectMaxMs  int `json:"reconnect_max_ms"`
	RotateIdleMs    int `json:"rotate_idle_ms"`
}

type Decoy struct {
	TickIntervalMs int `json:"tick_interval_ms"`
	TickJitterMs   int `json:"tick_jitter_ms"`
	BurstFrames    int `json:"burst_frames"`
	IdleIntervalMs int `json:"idle_interval_ms"`
	UplinkMinBytes int `json:"uplink_min_bytes"`
	UplinkMaxBytes int `json:"uplink_max_bytes"`
}

type Rate struct {
	MaxRateKBps int `json:"max_rate_kbps"`
	BurstBytes  int `json:"burst_bytes"`
}

type Config struct {
	Version     int       `json:"version"`
	Description string    `json:"description,omitempty"`
	Frame       Frame     `json:"frame"`
	Batch       Batch     `json:"batch"`
	Padding     Padding   `json:"padding"`
	Pulse       Pulse     `json:"pulse"`
	Lifecycle   Lifecycle `json:"lifecycle"`
	Decoy       Decoy     `json:"decoy"`
	Rate        Rate      `json:"rate"`
}

func Default() Config {
	return Config{
		Version:     SupportedVersion,
		Description: "Traffic shape shared by the raw_ws tunnel and the decoy website.",
		Frame: Frame{
			HeaderBytes:     12,
			FrameVersion:    1,
			MaxFrameBytes:   16384,
			MaxMessageBytes: 4 << 20,
		},
		Batch: Batch{
			WindowMs:      5,
			JitterMs:      10,
			MaxBytes:      32768,
			QueueCapacity: 4096,
			AckTimeoutMs:  15000,
		},
		Padding: Padding{
			MinTotalBytes:   64,
			MaxExtraBytes:   320,
			SmallExtraBytes: 32,
		},
		Pulse: Pulse{
			PingIntervalMs: 5000,
			PingTimeoutMs:  5000,
			ReadIdleMs:     15000,
			WriteTimeoutMs: 10000,
		},
		Lifecycle: Lifecycle{
			WarmupMinMs:     60,
			WarmupMaxMs:     250,
			RotationMinMs:   15 * 60 * 1000,
			RotationMaxMs:   35 * 60 * 1000,
			ReconnectBaseMs: 2000,
			ReconnectMaxMs:  30000,
			RotateIdleMs:    2000,
		},
		Decoy: Decoy{
			TickIntervalMs: 300,
			TickJitterMs:   300,
			BurstFrames:    2,
			IdleIntervalMs: 5000,
			UplinkMinBytes: 64,
			UplinkMaxBytes: 1024,
		},
		Rate: Rate{
			MaxRateKBps: 0,
			BurstBytes:  65536,
		},
	}
}

func Normalize(c Config) Config {
	d := Default()
	if c.Version <= 0 {
		c.Version = d.Version
	}
	c.Frame = normalizeFrame(c.Frame, d.Frame)
	c.Batch = normalizeBatch(c.Batch, d.Batch)
	c.Padding = normalizePadding(c.Padding, d.Padding)
	c.Pulse = normalizePulse(c.Pulse, d.Pulse)
	c.Lifecycle = normalizeLifecycle(c.Lifecycle, d.Lifecycle)
	c.Decoy = normalizeDecoy(c.Decoy, d.Decoy)
	c.Rate.BurstBytes = clampMin(c.Rate.BurstBytes, d.Rate.BurstBytes, 1)
	if c.Rate.MaxRateKBps < 0 {
		c.Rate.MaxRateKBps = 0
	}
	return c
}

func normalizeFrame(c, d Frame) Frame {
	c.HeaderBytes = clampMin(c.HeaderBytes, d.HeaderBytes, 1)
	c.FrameVersion = clampMin(c.FrameVersion, d.FrameVersion, 1)
	c.MaxFrameBytes = clampMin(c.MaxFrameBytes, d.MaxFrameBytes, c.HeaderBytes)
	c.MaxMessageBytes = clampMin(c.MaxMessageBytes, d.MaxMessageBytes, c.MaxFrameBytes)
	return c
}

func normalizeBatch(c, d Batch) Batch {
	c.WindowMs = clampMin(c.WindowMs, d.WindowMs, 1)
	c.JitterMs = clampMin(c.JitterMs, d.JitterMs, 0)
	c.MaxBytes = clampMin(c.MaxBytes, d.MaxBytes, 1)
	c.QueueCapacity = clampMin(c.QueueCapacity, d.QueueCapacity, 1)
	c.AckTimeoutMs = clampMin(c.AckTimeoutMs, d.AckTimeoutMs, 1)
	return c
}

func normalizePadding(c, d Padding) Padding {
	c.MinTotalBytes = clampMin(c.MinTotalBytes, d.MinTotalBytes, 0)
	c.MaxExtraBytes = clampMin(c.MaxExtraBytes, d.MaxExtraBytes, 0)
	c.SmallExtraBytes = clampMin(c.SmallExtraBytes, d.SmallExtraBytes, 0)
	return c
}

func normalizePulse(c, d Pulse) Pulse {
	c.PingIntervalMs = clampMin(c.PingIntervalMs, d.PingIntervalMs, 1)
	c.PingTimeoutMs = clampMin(c.PingTimeoutMs, d.PingTimeoutMs, 1)
	c.ReadIdleMs = clampMin(c.ReadIdleMs, d.ReadIdleMs, 1)
	c.WriteTimeoutMs = clampMin(c.WriteTimeoutMs, d.WriteTimeoutMs, 1)
	return c
}

func normalizeLifecycle(c, d Lifecycle) Lifecycle {
	c.WarmupMinMs = clampMin(c.WarmupMinMs, d.WarmupMinMs, 0)
	c.WarmupMaxMs = clampMin(c.WarmupMaxMs, d.WarmupMaxMs, c.WarmupMinMs)
	c.RotationMinMs = clampMin(c.RotationMinMs, d.RotationMinMs, 1)
	c.RotationMaxMs = clampMin(c.RotationMaxMs, d.RotationMaxMs, c.RotationMinMs)
	c.ReconnectBaseMs = clampMin(c.ReconnectBaseMs, d.ReconnectBaseMs, 1)
	c.ReconnectMaxMs = clampMin(c.ReconnectMaxMs, d.ReconnectMaxMs, c.ReconnectBaseMs)
	c.RotateIdleMs = clampMin(c.RotateIdleMs, d.RotateIdleMs, 1)
	return c
}

func normalizeDecoy(c, d Decoy) Decoy {
	c.TickIntervalMs = clampMin(c.TickIntervalMs, d.TickIntervalMs, 1)
	c.TickJitterMs = clampMin(c.TickJitterMs, d.TickJitterMs, 0)
	c.BurstFrames = clampMin(c.BurstFrames, d.BurstFrames, 1)
	c.IdleIntervalMs = clampMin(c.IdleIntervalMs, d.IdleIntervalMs, 1)
	c.UplinkMinBytes = clampMin(c.UplinkMinBytes, d.UplinkMinBytes, 1)
	c.UplinkMaxBytes = clampMin(c.UplinkMaxBytes, d.UplinkMaxBytes, c.UplinkMinBytes)
	return c
}

func clampMin(value, fallback, min int) int {
	if value == 0 {
		return fallback
	}
	if value < min {
		return min
	}
	return value
}

func Load(path string) (Config, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Default(), fmt.Errorf("wsshape: stat %s: %w", path, err)
	}
	if info.IsDir() {
		return Default(), fmt.Errorf("wsshape: %s is a directory", path)
	}
	if info.Size() > MaxFileBytes {
		return Default(), fmt.Errorf("wsshape: %s exceeds %d bytes", path, MaxFileBytes)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Default(), fmt.Errorf("wsshape: read %s: %w", path, err)
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return Default(), fmt.Errorf("wsshape: parse %s: %w", path, err)
	}
	if c.Version > SupportedVersion {
		return Default(), fmt.Errorf("wsshape: %s: unsupported version %d (max %d)", path, c.Version, SupportedVersion)
	}
	return Normalize(c), nil
}

func LoadDir(dir string) (Config, bool, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return Default(), false, nil
	}
	path := Path(dir)
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Default(), false, nil
		}
		return Default(), false, err
	}
	cfg, err := Load(path)
	if err != nil {
		return Default(), true, err
	}
	return cfg, true, nil
}

func Path(dir string) string {
	return filepath.Join(strings.TrimSpace(dir), FileName)
}

func PadLen(p Padding, payloadLen int) int {
	if !p.On() {
		return 0
	}
	if payloadLen < 0 {
		payloadLen = 0
	}
	if p.MaxExtraBytes > 0 {
		target := p.MinTotalBytes + rand.IntN(p.MaxExtraBytes)
		if payloadLen < target {
			return target - payloadLen
		}
	}
	if p.SmallExtraBytes > 0 {
		return rand.IntN(p.SmallExtraBytes)
	}
	return 0
}

func PadPayload(payload []byte, p Padding) []byte {
	pad := PadLen(p, len(payload))
	if pad <= 0 {
		return payload
	}
	out := make([]byte, len(payload)+pad)
	copy(out, payload)
	for i := len(payload); i < len(out); i++ {
		out[i] = ' '
	}
	return out
}
