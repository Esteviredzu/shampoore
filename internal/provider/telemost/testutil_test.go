package telemost

import "context"

type testLogger struct{}

func (testLogger) Trace(...any)                         {}
func (testLogger) Debug(...any)                         {}
func (testLogger) Info(...any)                          {}
func (testLogger) Warn(...any)                          {}
func (testLogger) Error(...any)                         {}
func (testLogger) Fatal(...any)                         {}
func (testLogger) Panic(...any)                         {}
func (testLogger) TraceContext(context.Context, ...any) {}
func (testLogger) DebugContext(context.Context, ...any) {}
func (testLogger) InfoContext(context.Context, ...any)  {}
func (testLogger) WarnContext(context.Context, ...any)  {}
func (testLogger) ErrorContext(context.Context, ...any) {}
func (testLogger) FatalContext(context.Context, ...any) {}
func (testLogger) PanicContext(context.Context, ...any) {}
