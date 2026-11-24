package hostmetrics

import "sync"

type Metrics struct {
	CPUPercent     float64 `json:"cpu_percent"`
	MemUsedBytes   uint64  `json:"mem_used_bytes"`
	MemTotalBytes  uint64  `json:"mem_total_bytes"`
	DiskUsedBytes  uint64  `json:"disk_used_bytes"`
	DiskTotalBytes uint64  `json:"disk_total_bytes"`
}

type Sampler struct {
	mu        sync.Mutex
	prevTotal uint64
	prevIdle  uint64
	hasPrev   bool
	statPath  string
	memPath   string
	diskPath  string
}

func NewSampler() *Sampler {
	return &Sampler{statPath: "/proc/stat", memPath: "/proc/meminfo", diskPath: "/"}
}

func (s *Sampler) cpuPercentLocked(total, idle uint64) float64 {
	var percent float64
	if s.hasPrev && total > s.prevTotal {
		dt := float64(total - s.prevTotal)
		di := float64(idle - s.prevIdle)
		percent = (dt - di) / dt * 100
		if percent < 0 {
			percent = 0
		}
		if percent > 100 {
			percent = 100
		}
	}
	s.prevTotal = total
	s.prevIdle = idle
	s.hasPrev = true
	return percent
}
