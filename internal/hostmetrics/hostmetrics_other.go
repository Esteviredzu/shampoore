//go:build !linux

package hostmetrics

import "fmt"

func (s *Sampler) Sample() (Metrics, error) {
	return Metrics{}, fmt.Errorf("hostmetrics: unsupported platform")
}
