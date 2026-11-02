package security

import (
	"github.com/Esteviredzu/shampoore-core/internal/logging"
	"github.com/Esteviredzu/shampoore-core/internal/transport"
)

func WrapPipe(inner transport.Pipe, password string, logger logging.ContextLogger) (transport.Pipe, error) {
	if password == "" {
		return inner, nil
	}
	return NewSecurePipe(inner, password, logger)
}