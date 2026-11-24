//go:build linux

package hostmetrics

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func (s *Sampler) Sample() (Metrics, error) {
	statData, err := os.ReadFile(s.statPath)
	if err != nil {
		return Metrics{}, fmt.Errorf("hostmetrics: read %s: %w", s.statPath, err)
	}
	total, idle, err := parseProcStat(statData)
	if err != nil {
		return Metrics{}, err
	}
	metrics := Metrics{}
	s.mu.Lock()
	metrics.CPUPercent = s.cpuPercentLocked(total, idle)
	s.mu.Unlock()

	if memData, err := os.ReadFile(s.memPath); err == nil {
		totalMem, availMem := parseMeminfo(memData)
		metrics.MemTotalBytes = totalMem
		if totalMem > availMem {
			metrics.MemUsedBytes = totalMem - availMem
		}
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(s.diskPath, &stat); err == nil {
		metrics.DiskUsedBytes, metrics.DiskTotalBytes = diskUsage(stat)
	}
	return metrics, nil
}

func parseProcStat(data []byte) (total, idle uint64, err error) {
	line := string(data)
	if nl := strings.IndexByte(line, '\n'); nl >= 0 {
		line = line[:nl]
	}
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, 0, fmt.Errorf("hostmetrics: unexpected /proc/stat format")
	}
	values := make([]uint64, 0, len(fields)-1)
	for _, field := range fields[1:] {
		value, parseErr := strconv.ParseUint(field, 10, 64)
		if parseErr != nil {
			return 0, 0, fmt.Errorf("hostmetrics: parse /proc/stat: %w", parseErr)
		}
		values = append(values, value)
	}
	for _, value := range values {
		total += value
	}
	if len(values) > 3 {
		idle = values[3]
	}
	if len(values) > 4 {
		idle += values[4]
	}
	return total, idle, nil
}

func parseMeminfo(data []byte) (total, available uint64) {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			total = value * 1024
		case "MemAvailable:":
			available = value * 1024
		}
	}
	return total, available
}

func diskUsage(stat syscall.Statfs_t) (used, total uint64) {
	blockSize := uint64(stat.Bsize)
	total = stat.Blocks * blockSize
	free := stat.Bavail * blockSize
	if total > free {
		used = total - free
	}
	return used, total
}
