// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// The limiter's rules and constants are Java swath's ConcurrencyGauge
// (AIMD): a throttle answer multiplies the limit down, growth comes back
// one permit at a time after a throttle-free window, and growth (never the
// limit) freezes while successful attempts are slow or attempts time out.
const (
	decreaseFactor  = 0.7              // per throttle answer
	cleanWindow     = 10 * time.Second // throttle-free time before any growth
	growthPace      = time.Second      // at most one +1 step per pace
	timeoutWindow   = 10 * time.Second // trailing window of attempt timeouts
	timeoutFreeze   = 3                // timeouts in the window that freeze growth
	latencyFreeze   = 2.0              // growth freezes while the latency EWMA exceeds this multiple of the baseline
	latencyAlpha    = 0.2              // EWMA smoothing of successful attempts' latency
	baselineDecay   = time.Minute      // the rolling-minimum baseline re-floors to the last window's minimum
	valvePace       = 30 * time.Second // while latency-frozen, at most one +1 step per valve pace
	valveSuccessDiv = 32               // ... and only with more than max(1, limit/32) successes in that pace
)

// Limiter is the adaptive admission of a client's HTTP attempts: every
// attempt (pages, probes, retries, requeues) takes a permit before it is
// sent and returns it when it returns (Do). The limit starts at the
// maximum (the worker count) and is lowered only by throttle answers. Its
// inputs are the attempts' own latency and answers, never admission waits
// or anything downstream of the listing. Safe for concurrent use.
type Limiter struct {
	max int
	now func() time.Time

	mu       sync.Mutex
	limit    int
	inflight int
	waiters  []chan struct{} // FIFO; a closed channel holds a granted permit

	lastThrottle, lastGrowth, lastValve time.Time
	toStart                             time.Time // the timeout window's start
	toCount                             int
	base, winMin                        time.Duration // latency baseline and the decay window's minimum
	winStart                            time.Time
	ewma                                float64 // ns
	valveStart                          time.Time
	valveSuccesses                      int

	c limiterCounts
}

type limiterCounts struct {
	decreases, increases, cooldown, latencyFrozen, timeoutFrozen, valve, waits, waitNs, depth atomic.Int64
}

// NewLimiter returns a limiter of at most n permits (at least 1).
func NewLimiter(n int) *Limiter {
	return newLimiter(n, time.Now)
}

func newLimiter(n int, now func() time.Time) *Limiter {
	n = max(n, 1)
	return &Limiter{max: n, limit: n, now: now}
}

// Limit is the current limit.
func (l *Limiter) Limit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

// Acquire waits for a permit, or for ctx to end.
func (l *Limiter) Acquire(ctx context.Context) error {
	l.mu.Lock()
	if l.inflight < l.limit && len(l.waiters) == 0 {
		l.inflight++
		l.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	l.waiters = append(l.waiters, ch)
	l.mu.Unlock()
	t0 := l.now()
	l.c.waits.Add(1)
	select {
	case <-ch:
		l.c.waitNs.Add(int64(l.now().Sub(t0)))
		return nil
	case <-ctx.Done():
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, w := range l.waiters {
		if w == ch {
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			return ctx.Err()
		}
	}
	// granted while the context ended: give the permit back
	l.inflight--
	l.grant()
	return ctx.Err()
}

// grant hands free permits to waiters in arrival order (l.mu held).
func (l *Limiter) grant() {
	for l.inflight < l.limit && len(l.waiters) > 0 {
		l.inflight++
		close(l.waiters[0])
		l.waiters = l.waiters[1:]
	}
}

// Outcome is what an attempt told the limiter.
type Outcome int

const (
	Success   Outcome = iota // a usable answer: its latency is a sample
	Throttled                // the service asked the client to slow down
	TimedOut                 // the attempt ran past the request timeout
	Ignored                  // any other failure: no signal
)

// Release returns a permit with the attempt's outcome and latency (send to
// body read).
func (l *Limiter) Release(o Outcome, latency time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inflight--
	switch o {
	case Throttled:
		l.decrease(now)
	case TimedOut:
		if l.toStart.IsZero() || now.Sub(l.toStart) > timeoutWindow {
			l.toStart, l.toCount = now, 0
		}
		l.toCount++
	case Success:
		l.sample(now, latency)
		l.grow(now)
	}
	l.grant()
}

func (l *Limiter) decrease(now time.Time) {
	l.lastThrottle = now
	next := max(1, int(float64(l.limit)*decreaseFactor))
	if next >= l.limit {
		return // at the floor
	}
	l.limit = next
	l.c.decreases.Add(1)
	if d := int64(l.max - next); d > l.c.depth.Load() {
		l.c.depth.Store(d)
	}
}

// sample feeds a successful attempt's latency to the baseline (a rolling
// minimum that re-floors each decay window) and the EWMA.
func (l *Limiter) sample(now time.Time, d time.Duration) {
	if l.valveStart.IsZero() || now.Sub(l.valveStart) > valvePace {
		l.valveStart, l.valveSuccesses = now, 0
	}
	l.valveSuccesses++
	switch {
	case l.base == 0:
		l.base, l.winMin, l.winStart = d, d, now
	default:
		l.winMin = min(l.winMin, d)
		l.base = min(l.base, d)
		if now.Sub(l.winStart) > baselineDecay {
			l.base, l.winMin, l.winStart = l.winMin, d, now
		}
	}
	if l.ewma == 0 {
		l.ewma = float64(d)
	} else {
		l.ewma += latencyAlpha * (float64(d) - l.ewma)
	}
}

// grow takes one +1 step when it is due and nothing holds it back; each
// growth step that was due and held back is counted under its brake.
func (l *Limiter) grow(now time.Time) {
	if l.limit >= l.max {
		return
	}
	due := now.Sub(l.lastGrowth) >= growthPace
	switch {
	case !l.lastThrottle.IsZero() && now.Sub(l.lastThrottle) < cleanWindow:
		if due {
			l.lastGrowth = now
			l.c.cooldown.Add(1)
		}
		return
	case l.toCount >= timeoutFreeze && now.Sub(l.toStart) <= timeoutWindow:
		if due {
			l.lastGrowth = now
			l.c.timeoutFrozen.Add(1)
		}
		return
	case l.base > 0 && l.ewma > latencyFreeze*float64(l.base):
		if due {
			l.lastGrowth = now
			l.c.latencyFrozen.Add(1)
		}
		if l.valveSuccesses > max(1, l.limit/valveSuccessDiv) && now.Sub(l.lastValve) >= valvePace {
			l.lastValve = now
			l.limit++
			l.c.valve.Add(1)
		}
		return
	case !due:
		return
	}
	l.lastGrowth = now
	l.limit++
	l.c.increases.Add(1)
}

// LimiterCounts are a limiter's engagements, for the run summary.
type LimiterCounts struct {
	Decreases     int64 // multiplicative decreases on throttle answers
	Increases     int64 // paced +1 steps
	Cooldown      int64 // growth steps held back by the throttle-free window
	LatencyFrozen int64 // growth steps held back by latency inflation
	TimeoutFrozen int64 // growth steps held back by attempt timeouts
	Valve         int64 // +1 steps let through the latency freeze
	Waits         int64 // attempts that waited for a permit
	WaitNs        int64 // their summed wait
	Depth         int64 // most permits taken away below the maximum
}

// Counts returns the engagements so far (zero for a nil limiter).
func (l *Limiter) Counts() LimiterCounts {
	if l == nil {
		return LimiterCounts{}
	}
	c := &l.c
	return LimiterCounts{c.decreases.Load(), c.increases.Load(), c.cooldown.Load(), c.latencyFrozen.Load(),
		c.timeoutFrozen.Load(), c.valve.Load(), c.waits.Load(), c.waitNs.Load(), c.depth.Load()}
}
