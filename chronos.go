// Package chronos provides a fixed-timestep game loop for server-authoritative simulations.
package chronos

import (
	"context"
	"sync"
	"time"
)

// Loop drives an update callback at a fixed tick rate.
type Loop struct {
	hz       int
	interval time.Duration
	updateFn func(dt float64)

	startOnce sync.Once
	done      chan struct{}
}

// New creates a Loop that invokes updateFn hz times per second.
// Panics if hz is not positive or updateFn is nil, since both are consumer errors.
func New(hz int, updateFn func(dt float64)) *Loop {
	if hz <= 0 {
		panic("chronos: tick rate must be positive")
	}
	if updateFn == nil {
		panic("chronos: update callback must not be nil")
	}

	return &Loop{
		hz:       hz,
		interval: time.Second / time.Duration(hz),
		updateFn: updateFn,
		done:     make(chan struct{}),
	}
}

// Start runs the loop in a new goroutine and returns immediately.
// The loop stops when ctx is cancelled. Subsequent calls are no-ops.
func (l *Loop) Start(ctx context.Context) {
	l.startOnce.Do(func() {
		go l.run(ctx)
	})
}

// Done returns a channel that is closed once the loop has fully stopped.
func (l *Loop) Done() <-chan struct{} {
	return l.done
}

// Interval returns the fixed timestep duration.
func (l *Loop) Interval() time.Duration {
	return l.interval
}

func (l *Loop) run(ctx context.Context) {
	defer close(l.done)

	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	last := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			// Check again so a tick racing with cancellation doesn't run an extra update.
			if ctx.Err() != nil {
				return
			}

			dt := now.Sub(last).Seconds()
			last = now

			l.updateFn(dt)
		}
	}
}
