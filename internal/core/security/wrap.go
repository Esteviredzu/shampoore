package security

import (
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

func WrapPipe(inner transport.Pipe, password string, logger logging.ContextLogger) (transport.Pipe, error) {
	return WrapPipeWithRole(inner, password, "", logger)
}

func WrapPipeWithRole(inner transport.Pipe, password, role string, logger logging.ContextLogger) (transport.Pipe, error) {
	if password == "" {
		return inner, nil
	}
	return NewSecurePipeWithRole(inner, password, role, logger)
}
