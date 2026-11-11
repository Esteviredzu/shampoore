package panel

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SystemStats struct {
	CPUUsage float64 `json:"cpu_usage"`
	MemUsed  uint64  `json:"mem_used"`
	MemTotal uint64  `json:"mem_total"`
	Uptime   uint64  `json:"uptime"`
}

type sysSampler struct {
	mu       sync.RWMutex
	cpuUsage float64
	memUsed  uint64
	memTotal uint64
	uptime   uint64

	prevTotal uint64
	prevIdle  uint64
	hasPrev   bool
}

var (
	sysSamplerInst = &sysSampler{}
	sysSamplerOnce sync.Once
)

func readCPUTicks() (uint64, uint64) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0
	}
	nl := strings.IndexByte(string(data), '\n')
	if nl < 0 {
		nl = len(data)
	}
	fields := strings.Fields(string(data[:nl]))
	if len(fields) < 5 {
		return 0, 0
	}
	var total, idle uint64
	for i, f := range fields[1:] {
		v, _ := strconv.ParseUint(f, 10, 64)
		total += v
		if i == 3 || i == 4 {
			idle += v
		}
	}
	return total, idle
}

func (s *sysSampler) sample() {
	total, idle := readCPUTicks()

	var memTotal, memAvail uint64
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			val, _ := strconv.ParseUint(fields[1], 10, 64)
			switch {
			case strings.HasPrefix(line, "MemTotal:"):
				memTotal = val * 1024
			case strings.HasPrefix(line, "MemAvailable:"):
				memAvail = val * 1024
			}
		}
	}

	var up uint64
	if data, err := os.ReadFile("/proc/uptime"); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) > 0 {
			f, _ := strconv.ParseFloat(fields[0], 64)
			up = uint64(f)
		}
	}

	s.mu.Lock()
	if s.hasPrev && total > s.prevTotal {
		dt := float64(total - s.prevTotal)
		di := float64(idle - s.prevIdle)
		s.cpuUsage = (dt - di) / dt * 100.0
	}
	s.prevTotal = total
	s.prevIdle = idle
	s.hasPrev = true
	s.memTotal = memTotal
	if memTotal > memAvail {
		s.memUsed = memTotal - memAvail
	}
	s.uptime = up
	s.mu.Unlock()
}

func (s *sysSampler) run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.sample()
	}
}

func getSystemStats() SystemStats {
	sysSamplerOnce.Do(func() {
		sysSamplerInst.sample()
		time.Sleep(100 * time.Millisecond)
		sysSamplerInst.sample()
		go sysSamplerInst.run()
	})

	s := sysSamplerInst
	s.mu.RLock()
	defer s.mu.RUnlock()
	return SystemStats{
		CPUUsage: s.cpuUsage,
		MemUsed:  s.memUsed,
		MemTotal: s.memTotal,
		Uptime:   s.uptime,
	}
}
