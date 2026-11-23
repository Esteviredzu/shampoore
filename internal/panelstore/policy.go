package panelstore

import (
	"strings"

	"github.com/Esteviredzu/shampoore/internal/capability"
)

type TransportPolicy interface {
	Known(kind string) bool
	IsMulti(kind string) bool
	MultiKinds() []string
	MultiStrategies(kind string) []string
	AllowsDecoy(kind string) bool
	Secrets() []capability.SecretSpec
	EditableSecret(key string) bool
}

type Option func(*Store)

func WithTransportPolicy(policy TransportPolicy) Option {
	return func(s *Store) {
		if policy != nil {
			s.policy = policy
		}
	}
}

func (s *Store) transportPolicy() TransportPolicy {
	if s != nil && s.policy != nil {
		return s.policy
	}
	return LegacyPolicy{}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}
