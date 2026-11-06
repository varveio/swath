// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRateCapsAttempts: under --request-rate R, N attempts through Do take
// at least (N-1)/R, retries included (each takes a token), from many
// callers at once; with the limiter the cap composes: no more attempts in
// flight than the permits, and still no faster than R.
func TestRateCapsAttempts(t *testing.T) {
	const rate, callers, calls = 100.0, 8, 5 // 40 calls; every other call retried once: 60 attempts
	for _, lim := range []*Limiter{nil, NewLimiter(2)} {
		r := NewRate(rate)
		var c Counters
		var attempts, now, most atomic.Int64
		var wg sync.WaitGroup
		start := time.Now()
		for g := range callers {
			wg.Go(func() {
				for i := range calls {
					first := true
					_, err := Do(context.Background(), Retry{MaxAttempts: 3, Limiter: lim, Rate: r, BaseBackoff: time.Microsecond, MaxBackoff: time.Microsecond}, &c,
						func() (int, error) {
							attempts.Add(1)
							n := now.Add(1)
							defer now.Add(-1)
							for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
							}
							time.Sleep(30 * time.Millisecond) // three times the spacing: the cap alone lets 3 overlap
							if first && (g+i)%2 == 0 {
								first = false
								return 0, ErrTimeout // retried: a second attempt, a second token
							}
							return 0, nil
						})
					if err != nil {
						t.Error(err)
					}
				}
			})
		}
		wg.Wait()
		el, n := time.Since(start), attempts.Load()
		if n != 60 {
			t.Fatalf("%d attempts, want 60", n)
		}
		if min := time.Duration(float64(n-1) / rate * float64(time.Second)); el < min {
			t.Fatalf("limiter %v: %d attempts in %s, under the cap's %s", lim != nil, n, el, min)
		}
		if lim != nil && most.Load() > 2 || lim == nil && most.Load() < 3 {
			t.Fatalf("limiter %v: at most %d attempts in flight", lim != nil, most.Load())
		}
		if rc := r.Counts(); rc.Waits == 0 || rc.WaitNs <= 0 {
			t.Fatalf("rate counts %+v", rc)
		}
		t.Logf("limiter %v: %d attempts in %s (cap %.0f/s), most in flight %d", lim != nil, n, el, rate, most.Load())
	}
}

// TestRateWaitCancels: a wait for a token ends with the caller's context
// and gives its token back; no cap (nil) never waits.
func TestRateWaitCancels(t *testing.T) {
	r := NewRate(1) // one per second
	if err := r.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	if err := r.Wait(ctx); err == nil || time.Since(t0) > 500*time.Millisecond {
		t.Fatalf("wait: %v after %s", err, time.Since(t0))
	}
	r.mu.Lock()
	next := r.next
	r.mu.Unlock()
	if d := time.Until(next); d > time.Second || d < 900*time.Millisecond {
		t.Fatalf("the canceled token was not given back: next due in %s", d)
	}
	if NewRate(0) != nil || (*Rate)(nil).Wait(context.Background()) != nil {
		t.Fatal("no cap waited")
	}
}

// TestTinyRateStillCaps: a positive rate whose interval overflows a
// time.Duration is the minimum cap, never no cap (the conversion wrapped
// to a negative interval, clamped to 1 ns: every attempt went at once).
func TestTinyRateStillCaps(t *testing.T) {
	r := NewRate(1e-12)
	if err := r.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := r.Wait(ctx); err == nil {
		t.Fatal("the second attempt went at once")
	}
}
