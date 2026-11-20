package version

import "strings"

var Version = "v2.0.0"

func Current() string {
	trimmed := strings.TrimSpace(Version)
	if trimmed == "" {
		return "dev"
	}
	return trimmed
}
