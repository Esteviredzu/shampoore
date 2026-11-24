//go:build linux

package hostmetrics

import (
	"syscall"
	"testing"
)

func TestParseProcStat(t *testing.T) {
	data := []byte("cpu  100 0 100 800 50 0 0 0 0 0\ncpu0 1 2 3 4 5 6 7 8 9 10\n")
	total, idle, err := parseProcStat(data)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1050 {
		t.Fatalf("total = %d, want 1050", total)
	}
	if idle != 850 {
		t.Fatalf("idle = %d, want 850", idle)
	}
	if _, _, err := parseProcStat([]byte("nonsense")); err == nil {
		t.Fatal("invalid /proc/stat was accepted")
	}
}

func TestParseMeminfo(t *testing.T) {
	data := []byte("MemTotal:       1048576 kB\nMemFree:         100000 kB\nMemAvailable:    524288 kB\n")
	total, available := parseMeminfo(data)
	if total != 1<<30 {
		t.Fatalf("total = %d, want %d", total, uint64(1<<30))
	}
	if available != 512<<20 {
		t.Fatalf("available = %d, want %d", available, uint64(512<<20))
	}
}

func TestDiskUsage(t *testing.T) {
	used, total := diskUsage(syscall.Statfs_t{Bsize: 4096, Blocks: 100, Bavail: 40})
	if total != 409600 {
		t.Fatalf("total = %d", total)
	}
	if used != 245760 {
		t.Fatalf("used = %d", used)
	}
}

func TestSamplerCPUPercent(t *testing.T) {
	sampler := &Sampler{}
	if got := sampler.cpuPercentLocked(1000, 900); got != 0 {
		t.Fatalf("first sample = %v, want 0", got)
	}
	if got := sampler.cpuPercentLocked(1200, 950); got != 75 {
		t.Fatalf("second sample = %v, want 75", got)
	}
	if got := sampler.cpuPercentLocked(1200, 950); got != 0 {
		t.Fatalf("unchanged counters = %v, want 0", got)
	}
}
