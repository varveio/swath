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
// 1/rate. Tokens are reserved in advance at times 1/rate apart, so the
// k-th token of a burst is due (k-1)/rate after the first; the send
// follows the token, and a caller delayed after its token (scheduling)
// can send together with the next one: the cap holds over a window, the
// sends are not strictly spaced. Every attempt waits for its token after
// the limiter's permit (Do), pages, probes, retries and requeues alike.
// Safe for concurrent use.
type Rate struct {
	every time.Duration

	mu   sync.Mutex
	next time.Time // the earliest time the next token is due

	waits, waitNs atomic.Int64
}

// MinRate is the smallest cap (one attempt per 1,000 s): below it the
// interval would overflow a time.Duration, and the conversion would wrap
// to no cap at all.
const MinRate = 1e-3

// NewRate returns a cap of perSecond attempts per second; nil (no cap) for
// perSecond <= 0. A positive cap below MinRate is MinRate (the flag
// refuses it).
func NewRate(perSecond float64) *Rate {
	if perSecond <= 0 {
		return nil
	}
	return &Rate{every: max(time.Duration(float64(time.Second)/max(perSecond, MinRate)), 1)}
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
