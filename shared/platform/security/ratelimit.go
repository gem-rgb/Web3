package security

import (
	"sync"
	"time"
)

type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter is a per-key token bucket rate limiter.
type Limiter struct {
	mu       sync.Mutex
	rate     float64
	burst    float64
	buckets  map[string]*bucket
	clock    func() time.Time
}

// NewLimiter creates a new rate limiter with the supplied rate and burst.
func NewLimiter(rate float64, burst int) *Limiter {
	if rate <= 0 {
		rate = 1
	}
	if burst <= 0 {
		burst = 1
	}
	return &Limiter{
		rate:    rate,
		burst:   float64(burst),
		buckets: map[string]*bucket{},
		clock:   time.Now,
	}
}

// Allow determines whether the key may consume a token.
func (l *Limiter) Allow(key string) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst - 1, last: now}
		l.buckets[key] = b
		return true
	}

	elapsed := now.Sub(b.last).Seconds()
	b.tokens = min(l.burst, b.tokens+elapsed*l.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

