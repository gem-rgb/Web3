package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/example/rms/server/internal/redisx"
)

// Manager provides Redis-backed distributed locks for hot aggregates.
type Manager struct {
	client *redisx.Client
	prefix string
}

// Lease represents a held lock.
type Lease struct {
	manager *Manager
	key     string
	token   string
}

// New returns a new lock manager.
func New(client *redisx.Client, prefix string) *Manager {
	prefix = strings.Trim(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		prefix = "rms"
	}
	return &Manager{client: client, prefix: prefix}
}

func (m *Manager) enabled() bool {
	return m != nil && m.client != nil
}

func (m *Manager) key(parts ...string) string {
	cleaned := make([]string, 0, len(parts)+1)
	cleaned = append(cleaned, m.prefix)
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			cleaned = append(cleaned, strings.ReplaceAll(strings.ToLower(value), " ", "-"))
		}
	}
	return strings.Join(cleaned, ":")
}

// Acquire tries to claim a lock until the context ends or the lock becomes available.
func (m *Manager) Acquire(ctx context.Context, scope string, ttl time.Duration) (*Lease, error) {
	if !m.enabled() {
		return &Lease{}, nil
	}
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	for attempt := 0; attempt < 3; attempt++ {
		lease, ok, err := m.TryAcquire(ctx, scope, ttl)
		if err != nil {
			return nil, err
		}
		if ok {
			return lease, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return nil, errors.New("distributed lock unavailable")
}

// TryAcquire attempts to claim a lock once.
func (m *Manager) TryAcquire(ctx context.Context, scope string, ttl time.Duration) (*Lease, bool, error) {
	if !m.enabled() {
		return &Lease{}, true, nil
	}
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	token := randomToken()
	ok, err := m.client.SetNX(ctx, m.key("lock", scope), token, ttl)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return &Lease{manager: m, key: m.key("lock", scope), token: token}, true, nil
}

// Release frees the lock if it is still owned by the lease token.
func (l *Lease) Release(ctx context.Context) error {
	if l == nil || l.manager == nil || l.manager.client == nil || l.key == "" {
		return nil
	}
	lua := `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end return 0`
	reply, err := l.manager.client.Do(ctx, "EVAL", lua, 1, l.key, l.token)
	if err != nil {
		return err
	}
	switch value := reply.(type) {
	case int64:
		if value < 0 {
			return fmt.Errorf("unexpected redis lock release reply %d", value)
		}
	case string:
		if value == "-1" {
			return fmt.Errorf("unexpected redis lock release reply %q", value)
		}
	}
	return nil
}

func randomToken() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}
