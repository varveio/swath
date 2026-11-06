// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"errors"
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

// TestLimiterRules pins Java's gauge rules: x0.7 per throttle answer down
// to 1, no growth for 10 s after the last throttle, then +1 per second up
// to the maximum; every held-back step counted under its brake.
func TestLimiterRules(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newLimiter(64, c.now)
	for _, want := range []int{44, 30, 21, 14, 9, 6, 4, 2, 1, 1} {
		l.cycle(Throttled, 0)
		if l.Limit() != want {
			t.Fatalf("after a throttle: limit %d, want %d", l.Limit(), want)
		}
	}
	if n := l.Counts(); n.Decreases != 9 || n.Depth != 63 {
		t.Fatalf("counts %+v", n)
	}
	for range 9 { // 9 s of successes inside the clean window
		c.add(time.Second)
		l.cycle(Success, 100*time.Millisecond)
	}
	if l.Limit() != 1 || l.Counts().Cooldown != 9 {
		t.Fatalf("grew inside the clean window: limit %d, %+v", l.Limit(), l.Counts())
	}
	c.add(time.Second + time.Millisecond)
	for i := range 100 { // many successes per second: still one step per second
		l.cycle(Success, 100*time.Millisecond)
		if i == 99 && l.Limit() != 2 {
			t.Fatalf("limit %d after the first paced step, want 2", l.Limit())
		}
	}
	for range 70 {
		c.add(time.Second)
		l.cycle(Success, 100*time.Millisecond)
	}
	if l.Limit() != 64 || l.Counts().Increases != 63 {
		t.Fatalf("recovery: limit %d, %+v", l.Limit(), l.Counts())
	}
}

func TestLimiterFreezes(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newLimiter(8, c.now)
	l.cycle(Throttled, 0) // 5
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
// the leak would block every later attempt; Sol, fifth review, Part 2).
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

// TestFloorThrottleDoesNotRearm is Java's ConcurrencyGaugeFloorRearmSuppressionTest
// on the fake clock: a throttle answer at a limit of 1 removes no
// concurrency and must not re-arm the 10 s clean window, else sustained
// throttling at the floor postpones growth forever; a real decrease (2 ->
// 1) still re-arms, and floor answers neither extend nor shorten its
// window.
func TestFloorThrottleDoesNotRearm(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newLimiter(64, c.now)
	for l.Limit() > 1 {
		l.cycle(Throttled, 0) // the last real decrease (2 -> 1) arms the window at t0
	}
	t0 := c.t
	for range 4 { // a floor answer every 2 s inside the window: no re-arm
		c.add(2 * time.Second)
		l.cycle(Throttled, 0)
	}
	if n := l.Counts(); n.FloorThrottles != 4 || n.Decreases != 9 {
		t.Fatalf("counts %+v", n)
	}
	c.t = t0.Add(cleanWindow - time.Millisecond)
	l.cycle(Success, 100*time.Millisecond)
	if l.Limit() != 1 {
		t.Fatalf("grew inside the window armed by the real decrease: limit %d", l.Limit())
	}
	c.t = t0.Add(cleanWindow + growthPace + time.Millisecond)
	l.cycle(Success, 100*time.Millisecond)
	if l.Limit() != 2 {
		t.Fatalf("floor throttles re-armed the clean window: limit %d 11 s after the last real decrease, want 2", l.Limit())
	}

	// Sustained throttling at the floor: one answer every 2 s, successes
	// between. The limit still leaves 1 once a window passes; each real
	// decrease back to 1 re-arms it.
	c2 := &fakeClock{t: time.Unix(2e9, 0)}
	l2 := newLimiter(64, c2.now)
	for l2.Limit() > 1 {
		l2.cycle(Throttled, 0)
	}
	grew := 0
	for range 60 { // 120 s
		c2.add(time.Second)
		l2.cycle(Success, 100*time.Millisecond)
		c2.add(time.Second)
		before := l2.Limit()
		l2.cycle(Throttled, 0)
		if before > 1 {
			grew++
		}
	}
	if grew == 0 {
		t.Fatal("sustained floor throttling pinned the limit at 1 for 120 s")
	}
	if n := l2.Counts(); n.Decreases != 9+int64(grew) || n.FloorThrottles != 60-int64(grew) {
		t.Fatalf("counts %+v, grew %d", n, grew)
	}
}

// TestFirstGrowthAfterCooldownIsImmediate: as in Java's gauge ("the first
// +1 after a cool-down fires immediately"), counting held-back steps does
// not consume the growth pace: a success just past the clean window grows
// the limit even when a held-back step was counted under a second ago.
func TestFirstGrowthAfterCooldownIsImmediate(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newLimiter(8, c.now)
	l.cycle(Throttled, 0) // 8 -> 5, window armed at t0
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
