package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON structured logger with service metadata attached.
func New(service, environment string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: levelFromEnv(os.Getenv("LOG_LEVEL")),
	})
	return slog.New(handler).With(
		"service", service,
		"environment", environment,
	)
}

func levelFromEnv(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

