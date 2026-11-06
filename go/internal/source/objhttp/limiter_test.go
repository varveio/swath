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
