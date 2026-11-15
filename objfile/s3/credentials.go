// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// MaxRefreshWindow is the most time before their expiry that credentials
// are refreshed: a request is never signed with credentials that may
// expire on its way or by the server's clock. Credentials that live less
// than four times as long are refreshed when a quarter of their lifetime
// is left, so a short lifetime is not refreshed on every request.
const MaxRefreshWindow = 5 * time.Minute

// Credentials are the credentials requests sign with, refreshed ahead of
// their expiry: when the smaller of MaxRefreshWindow and a quarter of the
// lifetime they had when fetched is left. One refresh runs at a time; while
// it runs, requests sign with the credentials still valid, and wait for it
// only when there are none. A refresh ahead that fails keeps the current
// credentials, tried again after refreshRetry, until they expire.
//
// Renew answers a service's refusal of credentials as expired: one renewal
// per generation of credentials, whose outcome every request refused for
// that generation shares. Each wait ends with the caller's context.
//
// It is an aws.CredentialsProvider. The provider it wraps is fetched again
// for each refresh: one that caches (aws.CredentialsCache) is invalidated
// first, and should have no expiry window of its own.
type Credentials struct {
	p   aws.CredentialsProvider
	now func() time.Time

	mu      sync.Mutex
	cur     aws.Credentials
	ok      bool      // cur may be signed with (fetched, and not refused after a failed renewal)
	due     time.Time // when cur is refreshed ahead (the zero time: never)
	gen     uint64    // the generation of cur: credentials installed so far
	flight  *fetch    // the fetch in progress, or nil
	refused *fetch    // the last renewal that failed, with the generation it renewed

	installed, forced atomic.Int64
}

// fetch is one fetch of the provider; done closes when it ends.
type fetch struct {
	done  chan struct{}
	gen   uint64 // the generation it replaces
	renew bool   // a renewal: a failure leaves no credentials to sign with
	creds aws.Credentials
	err   error
}

// refreshRetry is how long after a failed refresh ahead the next is tried,
// while the current credentials are still valid.
const refreshRetry = 10 * time.Second

// RefreshAhead returns p's credentials, refreshed ahead of their expiry.
func RefreshAhead(p aws.CredentialsProvider) *Credentials {
	return &Credentials{p: p, now: time.Now}
}

// Retrieve is aws.CredentialsProvider's Retrieve.
func (c *Credentials) Retrieve(ctx context.Context) (aws.Credentials, error) {
	creds, _, err := c.Get(ctx)
	return creds, err
}

// Get returns the credentials to sign with and their generation, for a
// later Renew.
func (c *Credentials) Get(ctx context.Context) (aws.Credentials, uint64, error) {
	c.mu.Lock()
	now := c.now()
	valid := c.ok && (!c.cur.CanExpire || now.Before(c.cur.Expires))
	if valid {
		if c.flight == nil && !c.due.IsZero() && !now.Before(c.due) {
			c.start(ctx, false)
		}
		creds, gen := c.cur, c.gen
		c.mu.Unlock()
		return creds, gen, nil
	}
	f := c.flight
	if f == nil {
		f = c.start(ctx, false)
	}
	c.mu.Unlock()
	if err := wait(ctx, f); err != nil {
		return aws.Credentials{}, 0, err
	}
	return f.creds, f.gen + 1, f.err
}

// Renew answers the refusal of generation gen's credentials as expired: it
// fetches new ones unless that was done for gen already, and returns that
// fetch's error. A renewal that fails leaves no credentials to sign with:
// the next Get fetches again.
func (c *Credentials) Renew(ctx context.Context, gen uint64) error {
	c.mu.Lock()
	var f *fetch
	switch {
	case c.gen != gen:
		c.mu.Unlock()
		return nil // renewed (or refreshed) since
	case c.flight != nil:
		f = c.flight // replacing gen
		f.renew = true
	case c.refused != nil && c.refused.gen == gen:
		f = c.refused
	default:
		f = c.start(ctx, true)
	}
	c.mu.Unlock()
	if err := wait(ctx, f); err != nil {
		return err
	}
	return f.err
}

// Refreshes is how many times credentials were installed after the first,
// and how many renewals were fetched.
func (c *Credentials) Refreshes() (refreshes, forced int64) {
	return max(c.installed.Load()-1, 0), c.forced.Load()
}

// start begins a fetch replacing generation c.gen; c.mu is held. It runs
// apart from ctx's cancellation: those waiting on it stop at their own.
func (c *Credentials) start(ctx context.Context, renew bool) *fetch {
	f := &fetch{done: make(chan struct{}), gen: c.gen, renew: renew}
	if renew {
		c.forced.Add(1)
	}
	c.flight = f
	go c.run(context.WithoutCancel(ctx), f)
	return f
}

func (c *Credentials) run(ctx context.Context, f *fetch) {
	if inv, ok := c.p.(interface{ Invalidate() }); ok {
		inv.Invalidate()
	}
	creds, err := c.p.Retrieve(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	f.creds, f.err = creds, err
	now := c.now()
	switch {
	case err == nil:
		c.cur, c.ok, c.gen, c.due = creds, true, c.gen+1, time.Time{}
		if creds.CanExpire {
			c.due = creds.Expires.Add(-min(MaxRefreshWindow, creds.Expires.Sub(now)/4))
		}
		c.installed.Add(1)
	case f.renew:
		c.ok, c.refused = false, f
	case c.ok:
		c.due = now.Add(refreshRetry)
	}
	c.flight = nil
	close(f.done)
}

// wait waits for f or for ctx's end.
func wait(ctx context.Context, f *fetch) error {
	select {
	case <-f.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("credentials: %w", context.Cause(ctx))
	}
}
