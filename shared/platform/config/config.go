package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Loader resolves environment variables with an optional prefix and supports
// file-backed secrets for mounted secret volumes.
type Loader struct {
	Prefix     string
	SecretsDir string
}

// New returns a loader that reads from the current process environment.
func New(prefix string) Loader {
	return Loader{
		Prefix:     strings.TrimSpace(prefix),
		SecretsDir: strings.TrimSpace(os.Getenv("RMS_SECRETS_DIR")),
	}
}

func (l Loader) key(name string) string {
	name = strings.TrimSpace(strings.ToUpper(name))
	if l.Prefix == "" {
		return name
	}
	return strings.TrimSuffix(strings.ToUpper(l.Prefix), "_") + "_" + name
}

func (l Loader) lookup(name string) string {
	key := l.key(name)
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	if l.SecretsDir != "" {
		if value := readSecret(filepath.Join(l.SecretsDir, key)); value != "" {
			return value
		}
	}
	return ""
}

// String returns the configured string or the provided fallback.
func (l Loader) String(name, fallback string) string {
	if value := l.lookup(name); value != "" {
		return value
	}
	return fallback
}

// Required returns the configured string or an error when it is missing.
func (l Loader) Required(name string) (string, error) {
	if value := l.lookup(name); value != "" {
		return value, nil
	}
	return "", fmt.Errorf("missing required config value %s", l.key(name))
}

// Int returns a parsed integer or the fallback when unset or invalid.
func (l Loader) Int(name string, fallback int) int {
	value := l.lookup(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// Bool returns a parsed boolean or the fallback when unset or invalid.
func (l Loader) Bool(name string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(l.lookup(name)))
	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	case "":
		return fallback
	default:
		return fallback
	}
}

// Duration returns a parsed duration or the fallback when unset or invalid.
func (l Loader) Duration(name string, fallback time.Duration) time.Duration {
	value := l.lookup(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// CSV returns a comma-separated environment variable as a slice.
func (l Loader) CSV(name string) []string {
	raw := l.lookup(name)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}

// Secret reads a mounted secret file when available and falls back to the env var.
func (l Loader) Secret(name string) (string, error) {
	if l.SecretsDir != "" {
		if value := readSecret(filepath.Join(l.SecretsDir, l.key(name))); value != "" {
			return value, nil
		}
	}
	return l.Required(name)
}

// Endpoint returns a host:port pair from either a fully qualified env value or
// a host/port pair built from the default values.
func (l Loader) Endpoint(name, defaultHost string, defaultPort int) string {
	if value := l.lookup(name); value != "" {
		return value
	}
	return fmt.Sprintf("%s:%d", defaultHost, defaultPort)
}

func readSecret(path string) string {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(bytes))
}

