// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// The limiter's rules start from Java swath's ConcurrencyGauge (AIMD): a
// throttle answer multiplies the limit down, growth comes back one permit
// at a time, and growth (never the limit) freezes while successful
// attempts are slow or attempts time out. Where Java cuts on every throttle
// answer and grows only after 10 s without one, the throttle signal here is
// read per round trip and as a fraction (docs/internals/providers/s3.md,
// "Admission"): a background of random throttle answers that the client's
// retries absorb is not congestion, and under Java's rules 1 % of answers
// throttled held the limit at its floor.
const (
	decreaseFactor = 0.7 // per cut
	// cutFraction: a throttle answer cuts only when at least this fraction
	// of the window's answers were throttles. A limit past a real rate cap
	// reads far more (77 % at 64 workers against 300 requests/s at 50 ms),
	// so the first cut comes within a round trip or two.
	cutFraction = 0.15
	// growFraction: growth needs the window's throttles under this fraction,
	// half the cut's, so a fraction between them (a limit just past a real
	// cap, or a background of 7.5 to 15 %) holds the limit where it is
	// instead of cutting and growing in turn, and a spurious cut over a
	// background under 7.5 % grows back.
	growFraction = 0.075
	// The window is the last windowPerWorker × workers answers to attempts
	// sent since the last cut, at least windowMin and at most windowMax:
	// four full rounds of the worker count, never fewer than 256 answers,
	// so a cut needs at least 39 throttles (15 % of 256, an unfilled
	// window's missing answers counted as successes). The statistical
	// argument: a 5 % background over 256 answers reads 12.8 with a
	// standard deviation of 3.5, and 39 is 7.5 deviations above it, which a
	// sliding window over a long run's thousands of windows does not reach
	// by chance; at 10 % (26, 3.7 deviations) a 120 s run at 5 % still cut
	// on some seeds, and over 64 answers (16 workers × 4) the rule cut on 7
	// throttles and took 16 workers to 7 (merge review). The price is
	// reaction time at few workers: past a real cap 77 % of answers are
	// throttles, so 39 arrive within about 51 answers, three round trips at
	// 16 workers, thirteen at 4. A cut empties the window, so a cut needs
	// fresh evidence, and answers to attempts sent before the cut (sent at a
	// limit already cut) are neither counted nor cut on: at most one cut per
	// round trip.
	windowPerWorker = 4
	windowMin       = 256
	windowMax       = 4096
	// cleanWindow: growth also resumes after this long without any throttle
	// answer (Java's rule; at low request rates the window turns over
	// slowly).
	cleanWindow      = 10 * time.Second
	growthPace       = time.Second      // at most one +1 step per pace
	timeoutWindow    = 10 * time.Second // trailing window of attempt timeouts
	timeoutFreeze    = 3                // timeouts in the window that freeze growth
	latencyFreeze    = 2.0              // growth freezes while the latency EWMA exceeds this multiple of the baseline
	latencyAlpha     = 0.2              // EWMA smoothing of successful attempts' latency
	baselineDecay    = time.Minute      // the rolling-minimum baseline re-floors to the last window's minimum
	valvePace        = 30 * time.Second // while latency-frozen, at most one +1 step per valve pace
	valveSuccessDiv  = 32               // ... and only with more than max(1, limit/32) successes in that pace
	slowStartInitial = 4                // a slow-start limiter's first limit (Java's SLOW_START_INITIAL_T)
)

// Limiter is the adaptive admission of a client's HTTP attempts: every
// attempt (pages, probes, retries, requeues) takes a permit before it is
// sent and returns it when it returns (Do). The limit starts at the
// maximum (the worker count) and is lowered only by throttle answers: a
// throttle answer to an attempt sent after the last cut, while at least
// cutFraction of the window's answers are throttles, multiplies it by 0.7
// and empties the window. Growth is +1 per second while the window, full
// since the last cut, holds under growFraction throttles, or after 10 s
// without a throttle answer. Its inputs are the attempts' own latency and
// answers, never admission waits or anything downstream of the listing.
// Safe for concurrent use.
//
// A slow-start limiter (NewSlowStartLimiter, Java's gauge ramp) starts at 4
// instead and doubles at each paced growth step (at most one per second)
// until the maximum or the first congestion signal (a cut or an attempt
// timeout), which ends the ramp for good: growth is then the +1 step.
// Java's reason: a run that starts, or a resume that restarts, at the full
// width into a throttling endpoint storms it at once. A tolerated throttle
// answer does not end it (Java's first throttle answer does): a background
// of random throttles would otherwise end every ramp at 4 or 8.
type Limiter struct {
	max int
	now func() time.Time

	ramp      bool      // slow start: growth doubles (l.mu)
	rampStart time.Time // the limiter's creation

	mu       sync.Mutex
	limit    int
	inflight int
	waiters  []chan struct{} // FIFO; a closed channel holds a granted permit

	lastThrottle, lastGrowth, lastValve time.Time // lastThrottle: the last throttle answer
	lastCut                             time.Time // the last cut: earlier attempts' answers are stale
	lastHeld                            time.Time // the held-back counts' own pace
	// The window: whether each of the last answers (to attempts sent since
	// the last cut) was a throttle, a ring of len(window); filled answers
	// in it, nThrottled of them throttles.
	window                  []bool
	pos, filled, nThrottled int
	toStart                 time.Time // the timeout window's start
	toCount                 int
	base, winMin            time.Duration // latency baseline and the decay window's minimum
	winStart                time.Time
	ewma                    float64 // ns
	valveStart              time.Time
	valveSuccesses          int

	c limiterCounts
}

type limiterCounts struct {
	decreases, floor, increases, cooldown, latencyFrozen, timeoutFrozen, valve, waits, waitNs, depth atomic.Int64
	stale, tolerated                                                                                 atomic.Int64
	doubles, rampNs, rampEnd                                                                         atomic.Int64
}

// NewLimiter returns a limiter of at most n permits (at least 1).
func NewLimiter(n int) *Limiter {
	return newLimiter(n, time.Now)
}

func newLimiter(n int, now func() time.Time) *Limiter {
	n = max(n, 1)
	return &Limiter{max: n, limit: n, now: now, window: make([]bool, min(max(windowPerWorker*n, windowMin), windowMax))}
}

// record adds an answer to the window (l.mu held).
func (l *Limiter) record(throttled bool) {
	if l.filled == len(l.window) {
		if l.window[l.pos] {
			l.nThrottled--
		}
	} else {
		l.filled++
	}
	l.window[l.pos] = throttled
	if throttled {
		l.nThrottled++
	}
	l.pos = (l.pos + 1) % len(l.window)
}

// over reports whether the window's throttles reach fraction f of its
// length (an unfilled window counts its missing answers as successes).
func (l *Limiter) over(f float64) bool {
	return float64(l.nThrottled) >= f*float64(len(l.window))
}

// NewSlowStartLimiter returns a limiter of at most n permits that starts
// at min(4, n) and ramps by doubling (Limiter).
func NewSlowStartLimiter(n int) *Limiter {
	return newSlowStartLimiter(n, time.Now)
}

func newSlowStartLimiter(n int, now func() time.Time) *Limiter {
	l := newLimiter(n, now)
	l.limit = min(slowStartInitial, l.max)
	l.rampStart = now()
	if l.limit < l.max {
		l.ramp = true
	} else {
		l.c.rampEnd.Store(int64(l.limit))
	}
	return l
}

// endRamp ends the slow start (l.mu held): the time since the limiter's
// creation and the limit it ended at are recorded once.
func (l *Limiter) endRamp(now time.Time) {
	if !l.ramp {
		return
	}
	l.ramp = false
	l.c.rampNs.Store(int64(now.Sub(l.rampStart)))
	l.c.rampEnd.Store(int64(l.limit))
}

// Limit is the current limit (0 for a nil limiter).
func (l *Limiter) Limit() int {
	if l == nil {
		return 0
	}
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
// body read). An answer to an attempt sent before the last cut is stale:
// it describes a limit already cut, so it is not counted in the window and
// a throttle among them does not cut again (one cut per round trip).
func (l *Limiter) Release(o Outcome, latency time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inflight--
	fresh := l.lastCut.IsZero() || !now.Add(-latency).Before(l.lastCut)
	switch o {
	case Throttled:
		if now.After(l.lastThrottle) {
			l.lastThrottle = now
		}
		switch {
		case !fresh:
			l.c.stale.Add(1)
		default:
			l.record(true)
			if !l.over(cutFraction) {
				l.c.tolerated.Add(1)
				break
			}
			l.endRamp(now)
			l.decrease(now)
		}
	case TimedOut:
		l.endRamp(now)
		if l.toStart.IsZero() || now.Sub(l.toStart) > timeoutWindow {
			l.toStart, l.toCount = now, 0
		}
		l.toCount++
	case Success:
		l.sample(now, latency)
		if fresh {
			l.record(false)
		}
		l.grow(now)
	}
	l.grant()
}

// decrease is a cut: the limit multiplied by 0.7, the window emptied and
// the cut's time recorded, so attempts sent before it are stale. A cut at
// the floor removes no concurrency and leaves the window and the stale
// mark as they are (as Java's gauge does not re-arm its window there); it
// is counted under FloorThrottles.
func (l *Limiter) decrease(now time.Time) {
	next := max(1, int(float64(l.limit)*decreaseFactor))
	if next >= l.limit {
		l.c.floor.Add(1)
		return
	}
	if now.After(l.lastCut) {
		l.lastCut = now
	}
	l.pos, l.filled, l.nThrottled = 0, 0, 0
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

// grow takes one +1 step (a doubling during slow start) when it is due
// and nothing holds it back; the valve's step is always +1. As in
// Java's gauge, only a step taken consumes the pace, so the first step
// after a cool-down or a freeze is immediate; a held-back step is counted
// under its brake at most once per pace, on a clock of its own.
func (l *Limiter) grow(now time.Time) {
	if l.limit >= l.max {
		return
	}
	held := func(c *atomic.Int64) {
		if now.Sub(l.lastHeld) >= growthPace {
			l.lastHeld = now
			c.Add(1)
		}
	}
	// Held by throttling: since a cut the window has not filled, or it
	// holds growFraction throttles or more; either way unless 10 s have
	// passed without a throttle answer.
	throttling := (!l.lastCut.IsZero() && l.filled < len(l.window)) || l.over(growFraction)
	switch {
	case throttling && !l.lastThrottle.IsZero() && now.Sub(l.lastThrottle) < cleanWindow:
		held(&l.c.cooldown)
		return
	case l.toCount >= timeoutFreeze && now.Sub(l.toStart) <= timeoutWindow:
		held(&l.c.timeoutFrozen)
		return
	case l.base > 0 && l.ewma > latencyFreeze*float64(l.base):
		held(&l.c.latencyFrozen)
		if l.valveSuccesses > max(1, l.limit/valveSuccessDiv) && now.Sub(l.lastValve) >= valvePace {
			l.lastValve = now
			l.limit++
			l.c.valve.Add(1)
		}
		return
	case now.Sub(l.lastGrowth) < growthPace:
		return
	}
	l.lastGrowth = now
	if l.ramp {
		l.limit = min(l.max, 2*l.limit)
		l.c.doubles.Add(1)
		if l.limit == l.max {
			l.endRamp(now)
		}
		return
	}
	l.limit++
	l.c.increases.Add(1)
}

// LimiterCounts are a limiter's engagements, for the run summary.
type LimiterCounts struct {
	Decreases      int64 // cuts: multiplicative decreases on throttle answers
	FloorThrottles int64 // cuts due at the floor: no decrease
	Stale          int64 // throttle answers to attempts sent before the last cut: no cut
	Tolerated      int64 // throttle answers while the window's throttles were under cutFraction: no cut
	Increases      int64 // paced +1 steps
	Cooldown       int64 // growth steps held back by throttling (the window since a cut unfilled, or at growFraction)
	// ThrottlePermille is the window's throttles per 1,000 of its length
	// now, and Limit the limit now: where the limit converged.
	ThrottlePermille int64
	Limit            int64
	LatencyFrozen    int64 // growth steps held back by latency inflation
	TimeoutFrozen    int64 // growth steps held back by attempt timeouts
	Valve            int64 // +1 steps let through the latency freeze
	Waits            int64 // attempts that waited for a permit
	WaitNs           int64 // their summed wait
	Depth            int64 // most permits taken away below the maximum
	// Slow start (zero without it): doublings, the time from the
	// limiter's creation to the ramp's end (the maximum reached or the
	// first cut or timeout) and the limit it ended at; RampEnd 0
	// while the ramp is still on.
	SlowStartDoubles int64
	SlowStartNs      int64
	SlowStartEnd     int64
}

// Counts returns the engagements so far (zero for a nil limiter).
func (l *Limiter) Counts() LimiterCounts {
	if l == nil {
		return LimiterCounts{}
	}
	c := &l.c
	l.mu.Lock()
	permille, limit := int64(l.nThrottled)*1000/int64(len(l.window)), int64(l.limit)
	l.mu.Unlock()
	return LimiterCounts{Decreases: c.decreases.Load(), FloorThrottles: c.floor.Load(), Increases: c.increases.Load(),
		Stale: c.stale.Load(), Tolerated: c.tolerated.Load(), ThrottlePermille: permille, Limit: limit,
		Cooldown: c.cooldown.Load(), LatencyFrozen: c.latencyFrozen.Load(), TimeoutFrozen: c.timeoutFrozen.Load(),
		Valve: c.valve.Load(), Waits: c.waits.Load(), WaitNs: c.waitNs.Load(), Depth: c.depth.Load(),
		SlowStartDoubles: c.doubles.Load(), SlowStartNs: c.rampNs.Load(), SlowStartEnd: c.rampEnd.Load()}
}
