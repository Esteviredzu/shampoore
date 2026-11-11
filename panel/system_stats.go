package panel

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type SystemStats struct {
	CPUUsage float64 `json:"cpu_usage"`
	MemUsed  uint64  `json:"mem_used"`
	MemTotal uint64  `json:"mem_total"`
	Uptime   uint64  `json:"uptime"`
}

func readCPU() (uint64, uint64) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) == 0 {
		return 0, 0
	}
	fields := strings.Fields(lines[0])
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

func getSystemStats() SystemStats {
	var s SystemStats

	if data, err := os.ReadFile("/proc/uptime"); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) > 0 {
			f, _ := strconv.ParseFloat(fields[0], 64)
			s.Uptime = uint64(f)
		}
	}

	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		lines := strings.Split(string(data), "\n")
		var total, avail uint64
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				val, _ := strconv.ParseUint(fields[1], 10, 64)
				if strings.HasPrefix(line, "MemTotal:") {
					total = val * 1024
				} else if strings.HasPrefix(line, "MemAvailable:") {
					avail = val * 1024
				}
			}
		}
		s.MemTotal = total
		s.MemUsed = total - avail
	}

	t1, idle1 := readCPU()
	time.Sleep(100 * time.Millisecond)
	t2, idle2 := readCPU()

	if t2 > t1 {
		dt := float64(t2 - t1)
		didle := float64(idle2 - idle1)
		s.CPUUsage = (dt - didle) / dt * 100.0
	}

	return s
}