// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"testing"
	"time"
)

// TestSlowStartRamp pins Java's slow start on a fake clock: the limit
// starts at 4, the first success doubles it at once, then at most one
// doubling per second, 4 -> 8 -> 16 -> 32 -> 64 in 3 s; the ramp's
// counters record the doublings, its length and where it ended.
func TestSlowStartRamp(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newSlowStartLimiter(64, c.now)
	if l.Limit() != 4 {
		t.Fatalf("initial limit %d, want 4", l.Limit())
	}
	for i := range 50 { // many successes in the first instant: one doubling
		l.cycle(Success, 100*time.Millisecond)
		if l.Limit() != 8 {
			t.Fatalf("success %d: limit %d, want 8", i, l.Limit())
		}
	}
	if n := l.Counts(); n.SlowStartDoubles != 1 || n.SlowStartEnd != 0 || n.Increases != 0 {
		t.Fatalf("after the first doubling: %+v", n)
	}
	for _, want := range []int{16, 32, 64, 64} {
		c.add(time.Second)
		l.cycle(Success, 100*time.Millisecond)
		if l.Limit() != want {
			t.Fatalf("limit %d, want %d", l.Limit(), want)
		}
	}
	n := l.Counts()
	if n.SlowStartDoubles != 4 || n.SlowStartNs != int64(3*time.Second) || n.SlowStartEnd != 64 || n.Increases != 0 || n.Decreases != 0 {
		t.Fatalf("ramp counts %+v", n)
	}
	// After the ramp the gauge's rules hold: x0.7, 10 s cool-down, +1.
	l.cycle(Throttled, 0)
	if l.Limit() != 44 {
		t.Fatalf("throttle after the ramp: limit %d", l.Limit())
	}
	c.add(11 * time.Second)
	l.cycle(Success, 100*time.Millisecond)
	if l.Limit() != 45 || l.Counts().SlowStartDoubles != 4 {
		t.Fatalf("growth after the ramp: limit %d %+v", l.Limit(), l.Counts())
	}
}

// TestSlowStartCongestionLatch: the first throttle answer or attempt
// timeout ends the ramp for good; growth is then +1 per second after the
// clean window, never a doubling.
func TestSlowStartCongestionLatch(t *testing.T) {
	for _, o := range []Outcome{Throttled, TimedOut} {
		c := &fakeClock{t: time.Unix(1e9, 0)}
		l := newSlowStartLimiter(64, c.now)
		l.cycle(Success, 100*time.Millisecond) // 8
		c.add(time.Second)
		l.cycle(Success, 100*time.Millisecond) // 16
		c.add(500 * time.Millisecond)
		l.cycle(o, 0)
		want := 16
		if o == Throttled {
			want = 11
		}
		n := l.Counts()
		if l.Limit() != want || n.SlowStartDoubles != 2 || n.SlowStartEnd != 16 || n.SlowStartNs != int64(1500*time.Millisecond) {
			t.Fatalf("outcome %d: limit %d, %+v", o, l.Limit(), n)
		}
		c.add(11 * time.Second)
		for range 3 {
			l.cycle(Success, 100*time.Millisecond)
			c.add(time.Second)
		}
		if l.Limit() != want+3 || l.Counts().SlowStartDoubles != 2 || l.Counts().Increases != 3 {
			t.Fatalf("outcome %d: after the latch limit %d, %+v", o, l.Limit(), l.Counts())
		}
	}
}

// TestSlowStartHeldGrowth: the ramp's doublings wait for the same brakes as
// the +1 (here the latency freeze), and the valve lets through +1, never a
// doubling. Ignored failures leave the ramp on.
func TestSlowStartHeldGrowth(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newSlowStartLimiter(64, c.now)
	l.cycle(Ignored, 0)
	l.cycle(Success, 10*time.Millisecond) // 8; baseline 10 ms
	for range 20 {                        // latency rises far past twice the baseline
		c.add(time.Second)
		l.cycle(Success, 100*time.Millisecond)
	}
	n := l.Counts()
	if n.LatencyFrozen == 0 || n.SlowStartDoubles != 1 || n.SlowStartEnd != 0 {
		t.Fatalf("latency freeze during the ramp: limit %d %+v", l.Limit(), n)
	}
	if l.Limit() != 8+int(n.Valve) {
		t.Fatalf("limit %d with %d valve steps", l.Limit(), n.Valve)
	}
}

// TestSlowStartSmallMax: at most 4 workers there is no ramp; one worker
// stays at one.
func TestSlowStartSmallMax(t *testing.T) {
	for _, n := range []int{0, 1, 3, 4} {
		l := NewSlowStartLimiter(n)
		want := max(n, 1)
		if l.Limit() != want || l.Counts().SlowStartEnd != int64(want) {
			t.Fatalf("n=%d: limit %d, %+v", n, l.Limit(), l.Counts())
		}
		l.cycle(Success, time.Millisecond)
		if l.Limit() != want || l.Counts().SlowStartDoubles != 0 {
			t.Fatalf("n=%d grew: %d", n, l.Limit())
		}
	}
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newSlowStartLimiter(6, c.now)
	l.cycle(Success, time.Millisecond)
	if l.Limit() != 6 || l.Counts().SlowStartEnd != 6 {
		t.Fatalf("a doubling is capped at the maximum: %d %+v", l.Limit(), l.Counts())
	}
}

// TestSlowStartAdmission: at the start only 4 attempts are in flight; the
// fifth waits until a doubling makes room.
func TestSlowStartAdmission(t *testing.T) {
	c := &fakeClock{t: time.Unix(1e9, 0)}
	l := newSlowStartLimiter(64, c.now)
	ctx := context.Background()
	for range 4 {
		if err := l.Acquire(ctx); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- l.Acquire(ctx) }()
	for l.waiting() != 1 {
		time.Sleep(time.Millisecond)
	}
	l.Release(Success, time.Millisecond) // 8: the waiter is admitted
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if l.held() != 4 || l.Limit() != 8 {
		t.Fatalf("held %d limit %d", l.held(), l.Limit())
	}
}
