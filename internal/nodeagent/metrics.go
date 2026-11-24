package nodeagent

import (
	"log"
	"time"

	"github.com/Esteviredzu/shampoore/internal/hostmetrics"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type MetricsSource interface {
	Sample() (panelstore.NodeMetrics, error)
}

type hostMetricsSource struct {
	sampler *hostmetrics.Sampler
}

func NewHostMetrics() MetricsSource {
	return hostMetricsSource{sampler: hostmetrics.NewSampler()}
}

func (h hostMetricsSource) Sample() (panelstore.NodeMetrics, error) {
	m, err := h.sampler.Sample()
	if err != nil {
		return panelstore.NodeMetrics{}, err
	}
	return panelstore.NodeMetrics{
		CPUPercent:     m.CPUPercent,
		MemUsedBytes:   m.MemUsedBytes,
		MemTotalBytes:  m.MemTotalBytes,
		DiskUsedBytes:  m.DiskUsedBytes,
		DiskTotalBytes: m.DiskTotalBytes,
	}, nil
}

func (a *Agent) sampleMetrics() *panelstore.NodeMetrics {
	if a.metrics == nil {
		return nil
	}
	m, err := a.metrics.Sample()
	if err != nil {
		a.mu.Lock()
		shouldLog := a.lastMetricsErr.IsZero() || time.Since(a.lastMetricsErr) > time.Minute
		if shouldLog {
			a.lastMetricsErr = time.Now()
		}
		a.mu.Unlock()
		if shouldLog {
			log.Printf("node-agent: host metrics: %v", err)
		}
		return nil
	}
	return &m
}
