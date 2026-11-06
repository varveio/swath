// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Rate caps a client's HTTP attempts per second (--request-rate, Java
// swath's ApiRateLimiter): a token bucket of one token, refilled every
// 1/rate, so attempts are spaced at least 1/rate apart and n attempts take
// at least (n-1)/rate. Every attempt waits for its token after the
// limiter's permit (Do), pages, probes, retries and requeues alike. Safe
// for concurrent use.
type Rate struct {
	every time.Duration

	mu   sync.Mutex
	next time.Time // the earliest time the next token is due

	waits, waitNs atomic.Int64
}

// NewRate returns a cap of perSecond attempts per second; nil (no cap) for
// perSecond <= 0.
func NewRate(perSecond float64) *Rate {
	if perSecond <= 0 {
		return nil
	}
	return &Rate{every: max(time.Duration(float64(time.Second)/perSecond), 1)}
}

// Wait takes a token, waiting for it or for ctx to end (then the token is
// given back if no later one was taken).
func (r *Rate) Wait(ctx context.Context) error {
	if r == nil {
		return nil
	}
	now := time.Now()
	r.mu.Lock()
	at := r.next
	if at.Before(now) {
		at = now
	}
	r.next = at.Add(r.every)
	r.mu.Unlock()
	d := at.Sub(now)
	if d <= 0 {
		return nil
	}
	r.waits.Add(1)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		r.waitNs.Add(int64(d))
		return nil
	case <-ctx.Done():
		r.mu.Lock()
		if r.next.Equal(at.Add(r.every)) {
			r.next = at
		}
		r.mu.Unlock()
		r.waitNs.Add(int64(time.Since(now)))
		return ctx.Err()
	}
}

// RateCounts are a cap's engagements.
type RateCounts struct {
	Waits  int64 // attempts that waited for a token
	WaitNs int64 // their summed wait
}

// Counts returns the engagements so far (zero for a nil cap).
func (r *Rate) Counts() RateCounts {
	if r == nil {
		return RateCounts{}
	}
	return RateCounts{r.waits.Load(), r.waitNs.Load()}
}
