package retry

import (
	"context"
	"math/rand"
	"time"
)

// Policy controls retry behavior for transient operations.
type Policy struct {
	Attempts  int
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    time.Duration
}

// DefaultPolicy returns a conservative retry policy.
func DefaultPolicy() Policy {
	return Policy{
		Attempts:  3,
		BaseDelay: 10 * time.Millisecond,
		MaxDelay:  250 * time.Millisecond,
		Jitter:    5 * time.Millisecond,
	}
}

// Do runs fn until it succeeds, the context ends, or the retry budget is exhausted.
func Do(ctx context.Context, policy Policy, fn func() error) error {
	if policy.Attempts <= 0 {
		policy.Attempts = 1
	}
	if policy.BaseDelay <= 0 {
		policy.BaseDelay = 10 * time.Millisecond
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = 250 * time.Millisecond
	}
	var lastErr error
	delay := policy.BaseDelay
	for attempt := 0; attempt < policy.Attempts; attempt++ {
		if err := fn(); err != nil {
			lastErr = err
			if attempt == policy.Attempts-1 {
				break
			}
			jitter := time.Duration(0)
			if policy.Jitter > 0 {
				jitter = time.Duration(rand.Int63n(int64(policy.Jitter)))
			}
			wait := delay + jitter
			if wait > policy.MaxDelay {
				wait = policy.MaxDelay
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			delay *= 2
			if delay > policy.MaxDelay {
				delay = policy.MaxDelay
			}
			continue
		}
		return nil
	}
	return lastErr
}
