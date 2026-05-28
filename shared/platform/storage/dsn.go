package storage

import "fmt"

// PostgresConfig captures the database-per-service connection details.
type PostgresConfig struct {
	Host            string
	Port            int
	Database        string
	Username        string
	Password        string
	SSLMode         string
	ApplicationName string
	Schema          string
}

// DSN renders a PostgreSQL connection string.
func (c PostgresConfig) DSN() string {
	port := c.Port
	if port == 0 {
		port = 5432
	}
	sslMode := c.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	schema := c.Schema
	if schema == "" {
		schema = "public"
	}
	appName := c.ApplicationName
	if appName == "" {
		appName = "rms"
	}
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s&application_name=%s&search_path=%s",
		c.Username, c.Password, c.Host, port, c.Database, sslMode, appName, schema,
	)
}

// RedisConfig captures the cache connection details.
type RedisConfig struct {
	Host     string
	Port     int
	Password string
	DB       int
}

// Address returns the host:port form used by Redis clients.
func (c RedisConfig) Address() string {
	port := c.Port
	if port == 0 {
		port = 6379
	}
	return fmt.Sprintf("%s:%d", c.Host, port)
}

