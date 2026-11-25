package wsshape

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultMatchesRawWSReference(t *testing.T) {
	d := Default()
	checks := []struct {
		name string
		got  int
		want int
	}{
		{"frame.header_bytes", d.Frame.HeaderBytes, 12},
		{"frame.frame_version", d.Frame.FrameVersion, 1},
		{"frame.max_frame_bytes", d.Frame.MaxFrameBytes, 16384},
		{"frame.max_message_bytes", d.Frame.MaxMessageBytes, 4 << 20},
		{"batch.window_ms", d.Batch.WindowMs, 5},
		{"batch.jitter_ms", d.Batch.JitterMs, 10},
		{"batch.max_bytes", d.Batch.MaxBytes, 32768},
		{"batch.queue_capacity", d.Batch.QueueCapacity, 4096},
		{"batch.ack_timeout_ms", d.Batch.AckTimeoutMs, 15000},
		{"padding.min_total_bytes", d.Padding.MinTotalBytes, 64},
		{"padding.max_extra_bytes", d.Padding.MaxExtraBytes, 320},
		{"padding.small_extra_bytes", d.Padding.SmallExtraBytes, 32},
		{"pulse.ping_interval_ms", d.Pulse.PingIntervalMs, 5000},
		{"pulse.ping_timeout_ms", d.Pulse.PingTimeoutMs, 5000},
		{"pulse.read_idle_ms", d.Pulse.ReadIdleMs, 15000},
		{"pulse.write_timeout_ms", d.Pulse.WriteTimeoutMs, 10000},
		{"lifecycle.warmup_min_ms", d.Lifecycle.WarmupMinMs, 60},
		{"lifecycle.warmup_max_ms", d.Lifecycle.WarmupMaxMs, 250},
		{"lifecycle.rotation_min_ms", d.Lifecycle.RotationMinMs, 15 * 60 * 1000},
		{"lifecycle.rotation_max_ms", d.Lifecycle.RotationMaxMs, 35 * 60 * 1000},
		{"lifecycle.reconnect_base_ms", d.Lifecycle.ReconnectBaseMs, 250},
		{"lifecycle.reconnect_max_ms", d.Lifecycle.ReconnectMaxMs, 700},
		{"lifecycle.rotate_idle_ms", d.Lifecycle.RotateIdleMs, 2000},
		{"decoy.tick_interval_ms", d.Decoy.TickIntervalMs, 300},
		{"decoy.tick_jitter_ms", d.Decoy.TickJitterMs, 300},
		{"decoy.idle_interval_ms", d.Decoy.IdleIntervalMs, 5000},
		{"rate.max_rate_kbps", d.Rate.MaxRateKBps, 0},
		{"rate.burst_bytes", d.Rate.BurstBytes, 65536},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
	if !d.Padding.On() {
		t.Error("default padding must be enabled")
	}
}

func TestNormalizeZeroValueUsesDefaults(t *testing.T) {
	got := Normalize(Config{})
	def := Default()
	if got.Frame != def.Frame || got.Batch != def.Batch || got.Padding != def.Padding ||
		got.Pulse != def.Pulse || got.Lifecycle != def.Lifecycle || got.Decoy != def.Decoy {
		t.Fatalf("Normalize(zero) = %+v, want %+v", got, def)
	}
	if got.Version != SupportedVersion {
		t.Fatalf("version = %d", got.Version)
	}
}

func TestLoadDirMissingUsesDefaults(t *testing.T) {
	cfg, found, err := LoadDir(t.TempDir())
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if found {
		t.Fatal("found = true for a directory without " + FileName)
	}
	if cfg.Batch.MaxBytes != Default().Batch.MaxBytes {
		t.Fatalf("missing file did not fall back to defaults: %+v", cfg.Batch)
	}
	if _, _, err := LoadDir(""); err != nil {
		t.Fatalf("empty dir: %v", err)
	}
}

func writeShape(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDirReadsProfile(t *testing.T) {
	dir := t.TempDir()
	writeShape(t, dir, `{
		"version": 1,
		"batch": {"window_ms": 2},
		"padding": {"min_total_bytes": 512},
		"pulse": {"ping_interval_ms": 1500},
		"rate": {"max_rate_kbps": 64}
	}`)
	cfg, found, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if !found {
		t.Fatal("profile was not found")
	}
	if cfg.Batch.WindowMs != 2 || cfg.Batch.MaxBytes != Default().Batch.MaxBytes {
		t.Fatalf("batch = %+v", cfg.Batch)
	}
	if cfg.Padding.MinTotalBytes != 512 || !cfg.Padding.On() {
		t.Fatalf("padding = %+v", cfg.Padding)
	}
	if cfg.Pulse.PingIntervalMs != 1500 || cfg.Pulse.ReadIdleMs != Default().Pulse.ReadIdleMs {
		t.Fatalf("pulse = %+v", cfg.Pulse)
	}
	if cfg.Rate.MaxRateKBps != 64 {
		t.Fatalf("rate = %+v", cfg.Rate)
	}
}

func TestLoadRejectsInvalidProfiles(t *testing.T) {
	dir := t.TempDir()
	writeShape(t, dir, `{"version": 99}`)
	if _, err := Load(Path(dir)); err == nil {
		t.Fatal("future version was accepted")
	}
	writeShape(t, dir, `{not json`)
	if _, err := Load(Path(dir)); err == nil {
		t.Fatal("broken JSON was accepted")
	}
	if _, err := Load(filepath.Join(dir, "absent.json")); err == nil {
		t.Fatal("missing file was accepted")
	}
}

func TestPadLenBounds(t *testing.T) {
	p := PadLen(Default().Padding, 10)
	if p < 54 || p > 373 {
		t.Fatalf("small payload pad %d outside the raw_ws reference range", p)
	}
	for i := 0; i < 500; i++ {
		pad := PadLen(Default().Padding, 4096)
		if pad < 0 || pad > 31 {
			t.Fatalf("large payload pad %d outside 0..31", pad)
		}
	}
	off := Default().Padding
	off.SetEnabled(false)
	if got := PadLen(off, 10); got != 0 {
		t.Fatalf("disabled padding returned %d", got)
	}
}

func TestPadPayloadKeepsValidJSON(t *testing.T) {
	payload := []byte(`{"type":"tick","ts":1}`)
	out := PadPayload(payload, Default().Padding)
	if len(out) < Default().Padding.MinTotalBytes || len(out) > Default().Padding.MinTotalBytes+Default().Padding.MaxExtraBytes {
		t.Fatalf("padded length %d outside the reference target range", len(out))
	}
	if !json.Valid(out) {
		t.Fatalf("padded payload is not valid JSON: %q", out)
	}
	for i := len(payload); i < len(out); i++ {
		if out[i] != ' ' {
			t.Fatalf("pad byte %d = %q, want space", i, out[i])
		}
	}
}

func TestNormalizeClampsRanges(t *testing.T) {
	got := Normalize(Config{
		Frame:     Frame{HeaderBytes: -5, MaxFrameBytes: -1, MaxMessageBytes: -1},
		Batch:     Batch{WindowMs: -1, MaxBytes: -3},
		Padding:   Padding{MinTotalBytes: -10, MaxExtraBytes: -20, SmallExtraBytes: -30},
		Pulse:     Pulse{PingIntervalMs: -1},
		Lifecycle: Lifecycle{WarmupMinMs: 500, WarmupMaxMs: 100, RotationMinMs: 900, RotationMaxMs: 100},
		Decoy:     Decoy{UplinkMinBytes: 900, UplinkMaxBytes: 100, BurstFrames: -2},
		Rate:      Rate{MaxRateKBps: -5, BurstBytes: -1},
	})
	if got.Frame.MaxMessageBytes < got.Frame.MaxFrameBytes {
		t.Fatalf("max_message_bytes %d < max_frame_bytes %d", got.Frame.MaxMessageBytes, got.Frame.MaxFrameBytes)
	}
	if got.Lifecycle.WarmupMaxMs < got.Lifecycle.WarmupMinMs {
		t.Fatalf("warmup range not clamped: %+v", got.Lifecycle)
	}
	if got.Lifecycle.RotationMaxMs < got.Lifecycle.RotationMinMs {
		t.Fatalf("rotation range not clamped: %+v", got.Lifecycle)
	}
	if got.Decoy.UplinkMaxBytes < got.Decoy.UplinkMinBytes {
		t.Fatalf("uplink range not clamped: %+v", got.Decoy)
	}
	if got.Rate.MaxRateKBps != 0 || got.Rate.BurstBytes <= 0 {
		t.Fatalf("rate not clamped: %+v", got.Rate)
	}
}
