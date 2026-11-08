// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }
func (l *Limiter) cycle(o Outcome, d time.Duration) {
	if err := l.Acquire(context.Background()); err != nil {
		panic(err)
	}
	l.Release(o, d)
}

// cut sends throttle answers at the clock's instant until the limiter
// cuts (or counts a cut due at the floor): a cut needs cutFraction of the
// window in throttles.
func (l *Limiter) cut() {
	before := l.Counts()
	for {
		l.cycle(Throttled, 0)
		if n := l.Counts(); n.Decreases != before.Decreases || n.FloorThrottles != before.FloorThrottles {
			return
		}
	}
}

// TestLimiterRules: a throttle answer cuts only once cutFraction of the
// window (4 x workers answers, at least 256) are throttles; each cut is
// x0.7 down to 1 and empties the window. Growth then waits for the window
// to fill again with under growFraction throttles, and is +1 per second up
// to the maximum; every held-back step is counted under its brake.
func TestLimiterRules(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newLimiter(64, c.now) // window 256: 39 throttles cut
	for range 38 {
		l.cycle(Throttled, 0)
	}
	if n := l.Counts(); l.Limit() != 64 || n.Tolerated != 38 || n.Decreases != 0 {
		t.Fatalf("under 15 %% of the window: limit %d %+v", l.Limit(), n)
	}
	l.cycle(Throttled, 0)
	if l.Limit() != 44 {
		t.Fatalf("the 39th throttle: limit %d, want 44", l.Limit())
	}
	for _, want := range []int{30, 21, 14, 9, 6, 4, 2, 1} {
		l.cut()
		if l.Limit() != want {
			t.Fatalf("after a cut: limit %d, want %d", l.Limit(), want)
		}
	}
	if n := l.Counts(); n.Decreases != 9 || n.Depth != 63 {
		t.Fatalf("counts %+v", n)
	}
	// Growth waits for a full window since the last cut, under 7.5 %.
	c.add(time.Second)
	for range 255 {
		l.cycle(Success, 100*time.Millisecond)
	}
	if l.Limit() != 1 || l.Counts().Cooldown != 1 {
		t.Fatalf("grew before the window filled: limit %d, %+v", l.Limit(), l.Counts())
	}
	l.cycle(Success, 100*time.Millisecond) // the 256th: the window is full and clean
	for range 100 {                        // many successes in one instant: one step per pace
		l.cycle(Success, 100*time.Millisecond)
	}
	if l.Limit() != 2 {
		t.Fatalf("limit %d after the first paced step, want 2", l.Limit())
	}
	for range 70 {
		c.add(time.Second)
		l.cycle(Success, 100*time.Millisecond)
	}
	if l.Limit() != 64 || l.Counts().Increases != 63 {
		t.Fatalf("recovery: limit %d, %+v", l.Limit(), l.Counts())
	}
}

// TestOneCutPerRoundTrip: a round trip whose every answer is a throttle
// cuts once; the answers to attempts sent before the cut are stale, neither
// counted in the window nor cut on.
func TestOneCutPerRoundTrip(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newLimiter(64, c.now)
	for range 64 {
		if err := l.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	c.add(50 * time.Millisecond)
	for range 64 {
		l.Release(Throttled, 50*time.Millisecond)
	}
	n := l.Counts()
	if l.Limit() != 44 || n.Decreases != 1 || n.Tolerated != 38 || n.Stale != 25 {
		t.Fatalf("one round of 64 throttles: limit %d, %+v (Java's rule: 1)", l.Limit(), n)
	}
}

// sim drives l through steps round trips of rtt on c: each round sends
// Limit() attempts at once and answers them together, answer(i, n) for
// the i-th of n; it returns the limit of each round.
func sim(l *Limiter, c *fakeClock, steps int, rtt time.Duration, answer func(i, n int) Outcome) []int {
	limits := make([]int, steps)
	for s := range steps {
		n := l.Limit()
		limits[s] = n
		for range n {
			if err := l.Acquire(context.Background()); err != nil {
				panic(err)
			}
		}
		c.add(rtt)
		for i := range n {
			l.Release(answer(i, n), rtt)
		}
	}
	return limits
}

func meanOf(xs []int) float64 {
	t := 0
	for _, x := range xs {
		t += x
	}
	return float64(t) / float64(len(xs))
}

// TestLimiterToleratesRandomThrottles: a load-independent background of
// throttle answers (1 % and 5 %, the red team's -throttle; under Java's
// rules the limit sat at its floor) leaves the limit at the worker count,
// at 16 workers as at 64 and over several random streams of 120 s, while
// a background past the cut fraction (25 %) is still cut on.
func TestLimiterToleratesRandomThrottles(t *testing.T) {
	for _, workers := range []int{16, 64} {
		for _, p := range []float64{0.01, 0.05, 0.25} {
			for seed := range uint64(4) {
				c := &fakeClock{t: time.Unix(1e9, 0)}
				l := newLimiter(workers, c.now)
				rng := rand.New(rand.NewPCG(seed, uint64(p*1000)))
				limits := sim(l, c, 2400, 50*time.Millisecond, func(int, int) Outcome { // 120 s
					if rng.Float64() < p {
						return Throttled
					}
					return Success
				})
				n := l.Counts()
				mean := meanOf(limits[len(limits)/2:])
				if seed == 0 {
					t.Logf("workers=%d p=%.2f: mean limit %.1f, cuts %d, tolerated %d, stale %d, increases %d, permille %d", workers, p, mean, n.Decreases, n.Tolerated, n.Stale, n.Increases, n.ThrottlePermille)
				}
				switch {
				case p < cutFraction && (n.Decreases != 0 || mean < float64(workers)):
					t.Errorf("workers=%d p=%.2f seed=%d: mean limit %.1f after %d cuts, want %d and none", workers, p, seed, mean, n.Decreases, workers)
				case p > cutFraction && (n.Decreases == 0 || mean > float64(workers)/4):
					t.Errorf("workers=%d p=%.2f seed=%d: mean limit %.1f, %d cuts: a background past the cut fraction must cut", workers, p, seed, mean, n.Decreases)
				}
			}
		}
	}
}

// TestLimiterPlateausUnderRateCap: a real cap (successes per 50 ms round
// trip, the rest throttled: 15 is the red team's -rps 300) is converged on
// within 30 s and held: in the last 60 s the limit stays between the cap
// and where 15 % of answers would be throttles, with no cut (no sawtooth)
// and the window's throttles under the cut fraction. A cap of 5, where one permit past it is already over 15 %,
// cuts and regrows instead, but stays within a factor of two of the cap.
func TestLimiterPlateausUnderRateCap(t *testing.T) {
	for _, capacity := range []int{5, 15, 40} {
		c := &fakeClock{t: time.Unix(1e9, 0)}
		l := newLimiter(64, c.now)
		answer := func(i, n int) Outcome {
			if i < capacity {
				return Success
			}
			return Throttled
		}
		sim(l, c, 600, 50*time.Millisecond, answer) // 30 s
		before := l.Counts()
		limits := sim(l, c, 1200, 50*time.Millisecond, answer) // 60 s
		n := l.Counts()
		lo, hi := slices.Min(limits), slices.Max(limits)
		t.Logf("cap %d: plateau %d..%d (mean %.1f), cuts %d then %d, permille %d", capacity, lo, hi, meanOf(limits), before.Decreases, n.Decreases-before.Decreases, n.ThrottlePermille)
		if capacity < 7 {
			if m := meanOf(limits); m < float64(capacity)/2 || m > 2*float64(capacity) {
				t.Errorf("cap %d: mean limit %.1f", capacity, m)
			}
			continue
		}
		// The plateau lies at or above the cap, below where 15 % of the
		// answers would be throttles.
		if lo < capacity-2 || float64(hi) > float64(capacity)/(1-cutFraction)+1 || n.Decreases != before.Decreases || n.ThrottlePermille >= cutFraction*1000 {
			t.Errorf("cap %d: plateau %d..%d, %d cuts in it, permille %d", capacity, lo, hi, n.Decreases-before.Decreases, n.ThrottlePermille)
		}
	}
}

func TestLimiterFreezes(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newLimiter(8, c.now)
	l.cut() // 5
	c.add(11 * time.Second)
	// three timeouts in 10 s freeze growth
	for range 3 {
		l.cycle(TimedOut, 0)
	}
	c.add(time.Second)
	l.cycle(Success, 50*time.Millisecond)
	if l.Limit() != 5 || l.Counts().TimeoutFrozen != 1 {
		t.Fatalf("timeout freeze: limit %d %+v", l.Limit(), l.Counts())
	}
	c.add(11 * time.Second) // the timeout window has passed
	l.cycle(Success, 50*time.Millisecond)
	if l.Limit() != 6 {
		t.Fatalf("after the timeout window: limit %d", l.Limit())
	}
	// latency EWMA above twice the 50 ms baseline freezes growth; the valve
	// lets one step through per 30 s once there is progress
	for range 20 {
		l.cycle(Success, 400*time.Millisecond)
	}
	before := l.Counts()
	for range 29 {
		c.add(time.Second)
		l.cycle(Success, 400*time.Millisecond)
	}
	n := l.Counts()
	if n.LatencyFrozen-before.LatencyFrozen != 29 || n.Valve-before.Valve > 1 || l.Limit() > 7 {
		t.Fatalf("latency freeze: limit %d %+v", l.Limit(), n)
	}
}

// TestLimiterAdmission: a lowered limit holds new attempts until enough
// permits return; waiters are served in order; a cancelled waiter takes
// nothing.
func TestLimiterAdmission(t *testing.T) {
	l := NewLimiter(2)
	ctx := context.Background()
	for range 38 { // the window (at least 256) holds 38 throttles: the 39th cuts
		l.cycle(Throttled, 0)
	}
	for range 2 {
		if err := l.Acquire(ctx); err != nil {
			t.Fatal(err)
		}
	}
	l.Release(Throttled, 0) // limit 1, one in flight
	got := make(chan int, 3)
	for i := range 2 {
		go func() {
			if l.Acquire(ctx) == nil {
				got <- i
			}
		}()
		for l.waiting() != i+1 {
			time.Sleep(time.Millisecond)
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error)
	go func() { done <- l.Acquire(cctx) }()
	for l.waiting() != 3 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter: %v", err)
	}
	select {
	case i := <-got:
		t.Fatalf("waiter %d admitted above the limit", i)
	case <-time.After(20 * time.Millisecond):
	}
	l.Release(Ignored, 0)
	if i := <-got; i != 0 {
		t.Fatalf("waiter %d first, want 0", i)
	}
	l.Release(Ignored, 0)
	if i := <-got; i != 1 {
		t.Fatalf("waiter %d second, want 1", i)
	}
	if n := l.Counts(); n.Waits != 3 {
		t.Fatalf("waits %d", n.Waits)
	}
}

func (l *Limiter) waiting() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.waiters)
}

func (l *Limiter) held() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inflight
}

// TestLimiterPanicReturnsPermit: an attempt that panics still returns its
// permit, so a caller that recovers keeps a working limiter (at one permit
// the leak would block every later attempt; durability review).
func TestLimiterPanicReturnsPermit(t *testing.T) {
	l := NewLimiter(1)
	var c Counters
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the attempt's panic was swallowed")
			}
		}()
		_, _ = Do(context.Background(), Retry{MaxAttempts: 1, Limiter: l}, &c, func() (int, error) { panic("attempt") })
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.Acquire(ctx); err != nil {
		t.Fatalf("the panicking attempt kept its permit: %v", err)
	}
	if n := l.held(); n != 1 {
		t.Fatalf("%d permits in flight, want 1", n)
	}
}

// TestLimiterGrantCancelRace: a waiter whose context ends at the moment a
// permit is granted to it, both ready before it looks: whichever it sees,
// the permit is either its own (it returns nil and releases it) or given
// back (it returns the context's error); never lost, never doubled. The
// clock hook runs inside Acquire after the waiter is queued and before it
// waits, so both events are in place every time.
func TestLimiterGrantCancelRace(t *testing.T) {
	var armed func()
	l := newLimiter(1, func() time.Time {
		if f := armed; f != nil {
			armed = nil
			f()
		}
		return time.Unix(0, 0)
	})
	granted, cancelled := 0, 0
	for range 500 {
		if err := l.Acquire(context.Background()); err != nil { // the one permit, held
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		armed = func() {
			cancel()
			l.Release(Ignored, 0) // grants the queued waiter
		}
		switch err := l.Acquire(ctx); {
		case err == nil:
			granted++
			l.Release(Ignored, 0)
		case errors.Is(err, context.Canceled):
			cancelled++
		default:
			t.Fatal(err)
		}
		if n, w := l.held(), l.waiting(); n != 0 || w != 0 {
			t.Fatalf("after the race: %d in flight, %d waiting", n, w)
		}
	}
	if granted == 0 || cancelled == 0 {
		t.Fatalf("one side of the race never ran: granted %d, cancelled %d", granted, cancelled)
	}
}

// TestFloorCuts: a cut due at a limit of 1 removes nothing, counts under
// floor_throttles and leaves the window as it is; once throttle answers
// stop, growth resumes after 10 s without one even before the window
// refills (at the floor's request rate it turns over slowly).
func TestFloorCuts(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newLimiter(64, c.now)
	for l.Limit() > 1 {
		l.cut()
	}
	l.cut() // due at the floor: the window keeps its throttles, so the next four are due too
	for range 4 {
		c.add(2 * time.Second)
		l.cycle(Throttled, 0)
	}
	if n := l.Counts(); n.FloorThrottles != 5 || n.Decreases != 9 || l.Limit() != 1 {
		t.Fatalf("limit %d, counts %+v", l.Limit(), n)
	}
	last := c.t
	c.t = last.Add(cleanWindow - time.Millisecond)
	l.cycle(Success, 100*time.Millisecond)
	if l.Limit() != 1 {
		t.Fatalf("grew within 10 s of a throttle with the window unfilled: limit %d", l.Limit())
	}
	c.t = last.Add(cleanWindow + time.Millisecond)
	l.cycle(Success, 100*time.Millisecond)
	if l.Limit() != 2 {
		t.Fatalf("limit %d 10 s after the last throttle, want 2", l.Limit())
	}
}

// TestFirstGrowthAfterCooldownIsImmediate: as in Java's gauge ("the first
// +1 after a cool-down fires immediately"), counting held-back steps does
// not consume the growth pace: a success just past the clean window (10 s
// without a throttle answer, the window since the cut unfilled) grows the
// limit even when a held-back step was counted under a second ago.
func TestFirstGrowthAfterCooldownIsImmediate(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newLimiter(8, c.now)
	l.cut() // 8 -> 5, the last throttle at t0
	t0 := c.t
	c.t = t0.Add(cleanWindow - time.Millisecond)
	l.cycle(Success, 100*time.Millisecond) // held back, counted
	c.t = t0.Add(cleanWindow + time.Millisecond)
	l.cycle(Success, 100*time.Millisecond)
	if l.Limit() != 6 {
		t.Fatalf("limit %d 1 ms after the clean window, want 6", l.Limit())
	}
	c.add(time.Second - 2*time.Millisecond)
	l.cycle(Success, 100*time.Millisecond)
	if l.Limit() != 6 {
		t.Fatalf("limit %d: two steps inside one pace", l.Limit())
	}
	if n := l.Counts(); n.Cooldown != 1 || n.Increases != 1 {
		t.Fatalf("counts %+v", n)
	}
}
