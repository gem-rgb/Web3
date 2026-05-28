package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/example/rms/server/internal/retry"
)

// Job is a unit of work executed by the worker pool.
type Job func(context.Context) error

// Pool is a bounded worker pool with retry support.
type Pool struct {
	jobs    chan Job
	workers int
	logger  *slog.Logger
	policy  retry.Policy

	wg sync.WaitGroup
}

// New returns a new worker pool.
func New(workers, queue int, logger *slog.Logger, policy retry.Policy) *Pool {
	if workers <= 0 {
		workers = 1
	}
	if queue <= 0 {
		queue = workers * 2
	}
	return &Pool{
		jobs:    make(chan Job, queue),
		workers: workers,
		logger:  logger,
		policy:  policy,
	}
}

// Start launches the worker goroutines.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func(workerID int) {
			defer p.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-p.jobs:
					if !ok {
						return
					}
					if job == nil {
						continue
					}
					err := retry.Do(ctx, p.policy, func() error {
						return job(ctx)
					})
					if err != nil && p.logger != nil {
						p.logger.Error("worker job failed", "worker_id", workerID, "error", err.Error())
					}
				}
			}
		}(i)
	}
}

// Submit queues a job for execution.
func (p *Pool) Submit(job Job) error {
	if p == nil {
		return errors.New("worker pool is nil")
	}
	select {
	case p.jobs <- job:
		return nil
	default:
		return errors.New("worker pool queue is full")
	}
}

// Close closes the queue and waits for workers to exit.
func (p *Pool) Close() {
	if p == nil {
		return
	}
	close(p.jobs)
	p.wg.Wait()
}
